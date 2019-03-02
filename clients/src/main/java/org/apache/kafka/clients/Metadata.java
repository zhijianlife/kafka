/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 封装集群元数据信息
 *
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    /** 元数据更新最小时间间隔，默认是 100 毫秒，防止更新太频繁 */
    private final long refreshBackoffMs;

    /** 元数据更新时间间隔，默认为 5 分钟 */
    private final long metadataExpireMs;

    /** 元数据版本号，每更新成功一次则版本号加 1 */
    private int version;

    /** 上一次更新元数据的时间戳，不管成功还是失败 */
    private long lastRefreshMs;

    /** 上一次成功更新元数据的时间戳 */
    private long lastSuccessfulRefreshMs;

    /** 集群信息 */
    private Cluster cluster;

    /** 标记是否需要更新集群元数据信息 */
    private boolean needUpdate;

    /** 记录集群中所有的 topic 信息，key 是 topic，value 是 topic 过期的时间戳 */
    private final Map<String, Long> topics;

    /** 元数据更新监听器 */
    private final List<Listener> listeners;

    private final ClusterResourceListeners clusterResourceListeners;

    /** 标记是否需要更新所有 topic 的元数据信息，一般只更新当前用到的 topic 的元数据信息 */
    private boolean needMetadataForAllTopics;

    /** 是否允许 topic 过期 */
    private final boolean topicExpiryEnabled;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this(refreshBackoffMs, metadataExpireMs, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     * polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata.
     * If topic expiry is enabled, expiry time will be reset on the next update.
     */
    public synchronized void add(String topic) {
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            // 当前 topic 是新加入的，标记需要更新集群元数据信息
            this.requestUpdateForNewTopics();
        }
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        // 将 needUpdate 字段修改为 true，sender 线程在运行时会基于该字段决定是否更新元数据信息
        this.needUpdate = true;
        // 返回当前集群元数据的版本
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        // 如果当前的版本小于等于 lastVersion 则说明元数据未更新完成
        while (this.version <= lastVersion) {
            if (remainingWaitMs != 0) {
                // 在未超时的情况下等待元数据完成更新
                this.wait(remainingWaitMs);
            }
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     *
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics)) {
            this.requestUpdateForNewTopics();
        }
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     *
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled,
     * expiry time is set for topics if required and expired topics are removed from the metadata.
     *
     * @param cluster the cluster containing metadata for topics with valid metadata
     * @param unavailableTopics topics which are non-existent or have one or more partitions whose
     * leader is not known
     * @param now current time in milliseconds
     */
    public synchronized void update(Cluster cluster, Set<String> unavailableTopics, long now) {
        Objects.requireNonNull(cluster, "cluster should not be null");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1; // 元数据版本加 1

        // 如果允许 topic 过期
        if (topicExpiryEnabled) {
            // 遍历集群中所有的 topic，移除已经过期的 topic
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE) {
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                } else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        // 遍历并应用所有注册的集群元数据更新监听器
        for (Listener listener : listeners)
            listener.onMetadataUpdate(cluster, unavailableTopics);

        String previousClusterId = cluster.clusterResource().clusterId();

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = this.getClusterForCurrentTopics(cluster);
        } else {
            this.cluster = cluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!cluster.isBootstrapConfigured()) {
            String clusterId = cluster.clusterResource().clusterId();
            if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId)) {
                log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
            }
            clusterResourceListeners.onUpdate(cluster.clusterResource());
        }

        this.notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     *
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            this.requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        /**
         * Callback invoked on metadata update.
         *
         * @param cluster the cluster containing metadata for topics with valid metadata
         * @param unavailableTopics topics which are non-existent or have one or more partitions whose
         * leader is not known
         */
        void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
    }

    /**
     *
     */
    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.requestUpdate();
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (partitionInfoList != null) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics);
    }
}
