/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // topic 消息队列的路由信息 ，消息发送时根据路由表进行负载均衡
    private final HashMap<String/* topic */, Map<String /* brokerName */ , QueueData>> topicQueueTable;
    // Broker 基础信息 包含 brokerName、所属集群名称、主备 Broker 地址
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    // Broker 集群信息、存储集群中所有 Broker 名称
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    // Broker 状态信息，NameServer 每次收到心跳包时会替换该信息
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    // Broker 上的 FilterServer 列表，用于类模式消息过滤
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<>(1024);
        this.brokerAddrTable = new HashMap<>(128);
        this.clusterAddrTable = new HashMap<>(32);
        this.brokerLiveTable = new HashMap<>(256);
        this.filterServerTable = new HashMap<>(256);
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public void deleteTopic(final String topic, final String clusterName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (brokerNames != null
                    && !brokerNames.isEmpty()) {
                    Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                    if (queueDataMap != null) {
                        for (String brokerName : brokerNames) {
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic,
                                    removedQD);
                            }
                        }
                        if (queueDataMap.isEmpty()) {
                            log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                            this.topicQueueTable.remove(topic);
                        }
                    }

                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                // 1 加锁防止并发修改 RouteInfoManager 中的路由表
                this.lock.writeLock().lockInterruptibly();

                // 2 如果集群不存在则创建，假如该地址已经注册的话需要移除之前的配置
                Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
                brokerNames.add(brokerName);

                boolean registerFirst = false;

                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    registerFirst = true;
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        log.debug("remove entry {} from brokerData", item);
                        it.remove();
                    }
                }

                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                if (MixAll.MASTER_ID == brokerId) {
                    log.info("cluster [{}] brokerName [{}] master address change from {} to {}",
                            brokerData.getCluster(), brokerData.getBrokerName(), oldAddr, brokerAddr);
                }

                registerFirst = registerFirst || (null == oldAddr);

                // 3 如果 Broker 为主节点
                if (null != topicConfigWrapper
                        && MixAll.MASTER_ID == brokerId) {
                    // 并且 Broker 的 topic 配置信息发生过变化
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                            || registerFirst) {
                        ConcurrentMap<String, TopicConfig> tcTable =
                                topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                // 创建或更新 topic 路由元数据
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }

                //  4 更新 BrokerLiveInfo 存储状态正常的 Broker 信息表
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                        new BrokerLiveInfo(
                                System.currentTimeMillis(),
                                topicConfigWrapper.getDataVersion(),
                                channel,
                                haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                // 5 注册 Broker 的过滤器 Server 地址列表 一个 Broker 上会关联多个 FilterServer 消息过滤器
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr, long timeStamp) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(timeStamp);
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(queueData.getBrokerName(), queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            QueueData old = queueDataMap.put(queueData.getBrokerName(), queueData);
            if (old != null && !old.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), old,
                        queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
    }

    public int addWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
    }

    private int operateWritePermOfBrokerByLock(final String brokerName, final int requestCode) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, requestCode);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("operateWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }


    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;

        for (Map.Entry<String, Map<String, QueueData>> entry : topicQueueTable.entrySet()) {
            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            if (queueDataMap != null) {
                QueueData qd = queueDataMap.get(brokerName);
                if (qd != null) {
                    int perm = qd.getPerm();
                    switch (requestCode) {
                        case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                            perm &= ~PermName.PERM_WRITE;
                            break;
                        case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                            perm = PermName.PERM_READ | PermName.PERM_WRITE;
                            break;
                    }
                    qd.setPerm(perm);

                    topicCnt++;
                }
            }
        }

        return topicCnt;
    }

    public void unregisterBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                        brokerLiveInfo != null ? "OK" : "Failed",
                        brokerAddr
                );

                this.filterServerTable.remove(brokerAddr);

                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                            addr != null ? "OK" : "Failed",
                            brokerAddr
                    );

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                                brokerName
                        );

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                                removed ? "OK" : "Failed",
                                brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                    clusterName
                            );
                        }
                    }
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    private void removeTopicByBrokerName(final String brokerName) {
        Set<String> noBrokerRegisterTopic = new HashSet<>();

        this.topicQueueTable.forEach((topic, queueDataMap) -> {
            QueueData old = queueDataMap.remove(brokerName);
            if (old != null) {
                log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, old);
            }

            if (queueDataMap.size() == 0) {
                noBrokerRegisterTopic.add(topic);
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
            }
        });

        noBrokerRegisterTopic.forEach(topicQueueTable::remove);
    }

    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<>();
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                if (queueDataMap != null) {
                    topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                    foundQueueData = true;

                    brokerNameSet.addAll(queueDataMap.keySet());

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                    .getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            // skip if filter server table is empty
                            if (!filterServerTable.isEmpty()) {
                                for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                    List<String> filterServerList = this.filterServerTable.get(brokerAddr);

                                    // only add filter server list when not null
                                    if (filterServerList != null) {
                                        filterServerMap.put(brokerAddr, filterServerList);
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    /**
     * 剔除失效的 Broker
     * @return
     */
    public int scanNotActiveBroker() {
        int removeCount = 0;
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                RemotingUtil.closeChannel(next.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());

                removeCount++;
            }
        }

        return removeCount;
    }

    public void onChannelDestroy(String remoteAddr, Channel channel) {
        // 先看下是否有对应的 channel 存在，如果存在则取 channel 上的地址
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                            this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                 } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    this.lock.writeLock().lockInterruptibly();
                    this.brokerLiveTable.remove(brokerAddrFound);
                    this.filterServerTable.remove(brokerAddrFound);
                    String brokerNameFound = null;
                    boolean removeBrokerName = false;
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
                            this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        // 找到对应的 broker，将其从 brokerData 中 Broker 地址中移除
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                        brokerId, brokerAddr);
                                break;
                            }
                        }

                        // Broker 地址也是空的，将其移除 从brokerAddrTable 中移除
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                    brokerData.getBrokerName());
                        }
                    }

                    // 根据 BrokerName，从 clusterAddrTable 中找到 Broker 并将其从集群中移除
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                        brokerNameFound, clusterName);

                                // 如果集群中不包含任何 Broker，则将该集群从 clusterAddrTable 中移除
                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                            clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    // 根据 BrokerName，遍历所以主题队列，如果队列中包含当前 Broker 的队列则将其移除，如果topic 中包含待移除 Broker 的队列，
                    // 从路由表中删除该 topic
                    if (removeBrokerName) {
                        String finalBrokerNameFound = brokerNameFound;
                        Set<String> needRemoveTopic = new HashSet<>();

                        topicQueueTable.forEach((topic, queueDataMap) -> {
                            QueueData old = queueDataMap.remove(finalBrokerNameFound);
                            log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                    topic, old);

                            if (queueDataMap.size() == 0) {
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                        topic);
                                needRemoveTopic.add(topic);
                            }
                        });

                        needRemoveTopic.forEach(topicQueueTable::remove);
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, Map<String, QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Map<String, QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    this.topicQueueTable.forEach((topic, queueDataMap) -> {
                        if (queueDataMap.containsKey(brokerName)) {
                            topicList.getTopicList().add(topic);
                        }
                    });
                }

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getUnitTopics() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubTopicList() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        return topicQueueTableIter(qd -> !TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag())
                && TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    private TopicList topicQueueTableIter(Predicate<QueueData> pickCondition) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                topicQueueTable.forEach((topic, queueDataMap) -> {
                    for (QueueData qd : queueDataMap.values()) {
                        if (pickCondition.test(qd)) {
                            topicList.getTopicList().add(topic);
                        }

                        // we need only one queue data here
                        break;
                    }
                });

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }
}

class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private DataVersion dataVersion;
    private Channel channel;
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
                          String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
