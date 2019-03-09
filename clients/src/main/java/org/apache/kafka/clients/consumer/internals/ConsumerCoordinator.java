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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the coordination process with the consumer coordinator.
 *
 * KafkaConsumer 通过 ConsumerCoordinator 与服务端 GroupCoordinator 进行交互
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    /**
     * 消费者在发送 {@link org.apache.kafka.common.requests.JoinGroupRequest}
     * 请求时会传递自身的 {@link PartitionAssignor} 信息，服务端会从所有消费者都支持的分配策略中选择一种，
     * 并通知 leader 使用此分配策略进行分配
     */
    private final List<PartitionAssignor> assignors;
    /** 集群元数据信息 */
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    /** 追踪 TopicPartition 和 offset 的对应关系 */
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    /** 是否启用自动提交 */
    private final boolean autoCommitEnabled;
    /** 自动提交间隔 */
    private final int autoCommitIntervalMs;
    /** 注册的拦截器集合 */
    private final ConsumerInterceptors<?, ?> interceptors;
    /** 是否排除内部 topic */
    private final boolean excludeInternalTopics;
    private final AtomicInteger pendingAsyncCommits;

    /**
     * this collection must be thread-safe because it is modified from the response handler
     * of offset commit requests, which may be invoked from the heartbeat thread
     */
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    /** 元数据快照，用于检测 topic 分区数量是否发生变化 */
    private MetadataSnapshot metadataSnapshot;
    /** 元数据快照，用于检测分区分配过程中分区数量是否发生变化 */
    private MetadataSnapshot assignmentSnapshot;
    private long nextAutoCommitDeadline;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics,
                               final boolean leaveGroupOnClose) {
        super(client,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs,
                leaveGroupOnClose);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
        this.pendingAsyncCommits = new AtomicInteger();

        if (autoCommitEnabled) {
            this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;
        }

        this.metadata.requestUpdate();
        addMetadataListener();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        this.joinedSubscription = subscriptions.subscription();
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(joinedSubscription);
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = new HashSet<>();

        for (String topic : cluster.topics())
            if (subscriptions.subscribedPattern().matcher(topic).matches() &&
                    !(excludeInternalTopics && cluster.internalTopics().contains(topic))) {
                topicsToSubscribe.add(topic);
            }

        subscriptions.subscribeFromPattern(topicsToSubscribe);

        // note we still need to update the topics contained in the metadata. Although we have
        // specified that all topics should be fetched, only those set explicitly will be retained
        metadata.setTopics(subscriptions.groupSubscription());
    }

    /**
     * 添加元数据更新监听器
     */
    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                // 如果存在未授权的 topic，则抛出异常
                if (!cluster.unauthorizedTopics().isEmpty()) {
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                }

                // 如果使用 AUTO_PATTERN 订阅主题
                if (subscriptions.hasPatternSubscription()) {
                    // 依据正则检索匹配的 topic
                    updatePatternSubscription(cluster);
                }

                // check if there are any changes to the metadata which should trigger a rebalance
                // 如果当前的订阅方式是 AUTO_TOPICS 或 AUTO_PATTERN
                if (subscriptions.partitionsAutoAssigned()) {
                    // 创建元数据快照
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    // 比对快照版本，如果发生变更则更新本地缓存的快照信息
                    if (!snapshot.equals(metadataSnapshot)) {
                        metadataSnapshot = snapshot;
                    }
                }

                // 如果当前缓存的 topic 存在不可用的 topic，则标记更新集群元数据
                if (!Collections.disjoint(metadata.topics(), unavailableTopics)) {
                    metadata.requestUpdate();
                }
            }
        });
    }

    /**
     * 遍历寻找 name 对应的 PartitionAssignor
     *
     * @param name
     * @return
     */
    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : assignors) {
            if (assignor.name().equals(name)) {
                return assignor;
            }
        }
        return null;
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String assignmentStrategy, ByteBuffer assignmentBuffer) {
        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader) {
            assignmentSnapshot = null;
        }

        // 从消费者支持的分配策略集合中选择 assignmentStrategy 的对应的策略
        PartitionAssignor assignor = this.lookupAssignor(assignmentStrategy);
        if (assignor == null) {
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        }

        // 反序列化获取分区分配信息
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // 标记需要从 GroupCoordinator 获取最近提交的 offset，
        subscriptions.needRefreshCommits();

        // 初始化每个分区对应的分区状态
        subscriptions.assignFromSubscribed(assignment.partitions());

        /*
         * check if the assignment contains some topics that were not in the original subscription,
         * if yes we will obey what leader has decided and add these topics into the subscriptions
         * as long as they still match the subscribed pattern
         *
         * TODO this part of the logic should be removed once we allow regex on leader assign
         */

        // 遍历获取新分配的 topic
        Set<String> addedTopics = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (!joinedSubscription.contains(tp.topic())) {
                addedTopics.add(tp.topic());
            }
        }

        if (!addedTopics.isEmpty()) {
            Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
            Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
            newSubscription.addAll(addedTopics);
            newJoinedSubscription.addAll(addedTopics);

            // 使用 AUTO_PATTERN 模式进行订阅
            subscriptions.subscribeFromPattern(newSubscription);
            joinedSubscription = newJoinedSubscription;
        }

        // 更新集群元数据信息
        metadata.setTopics(subscriptions.groupSubscription());
        client.ensureFreshMetadata();

        // give the assignor a chance to update internal state based on the received assignment
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        // 应用监听分区 rebalance 操作的监听器
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment", listener.getClass().getName(), groupId, e);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator
     * is known and that the consumer has joined the group (if it is using group management).
     * This also handles periodic offset commits if they are enabled.
     *
     * @param now current time in milliseconds
     */
    public void poll(long now) {
        invokeCompletedOffsetCommitCallbacks();

        // 当前是 AUTO_TOPICS 或 AUTO_PATTERN（USER_ASSIGNED 不需要 rebalance），且 coordinator 不可达，则需要触发 rebalance
        if (subscriptions.partitionsAutoAssigned() && coordinatorUnknown()) {
            this.ensureCoordinatorReady();
            now = time.milliseconds();
        }

        // 需要执行 rejoin
        if (this.needRejoin()) {
            /*
             * due to a race condition between the initial metadata fetch and the initial rebalance,
             * we need to ensure that the metadata is fresh before joining initially.
             * This ensures that we have matched the pattern against the cluster's topics at least once before joining.
             *
             * 如果是 AUTO_PATTERN 订阅模式，则需要检查是否需要更新集群元数据
             */
            if (subscriptions.hasPatternSubscription()) {
                client.ensureFreshMetadata();
            }

            /*
             * 1. 检查目标 coordinator 节点是否准备好接收请求
             * 2. 启动心跳线程
             * 3. 执行 join group 操作
             */
            this.ensureActiveGroup();
            now = time.milliseconds();
        }

        // 发送心跳
        this.pollHeartbeat(now);
        // 异步提交 offset
        this.maybeAutoCommitOffsetsAsync(now);
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(long)}.
     *
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled) {
            return timeToNextHeartbeat(now);
        }

        if (now > nextAutoCommitDeadline) {
            return 0;
        }

        return Math.min(nextAutoCommitDeadline - now, timeToNextHeartbeat(now));
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
        // 从消费者支持的分配策略集合中选择 assignmentStrategy 的对应的策略
        PartitionAssignor assignor = this.lookupAssignor(assignmentStrategy);
        if (assignor == null) {
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        }

        // 解析封装订阅信息
        Set<String> allSubscribedTopics = new HashSet<>(); // group 中消费者订阅的所有 topic
        Map<String, Subscription> subscriptions = new HashMap<>(); // Map<String, ByteBuffer> -> Map<String, Subscription>
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            // ByteBuffer -> Subscription
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }

        /*
         * 对于 leader 消费者来说，需要关注 group 中所有消费者订阅的 topic，
         * 以保证当相应 topic 对应的元数据发生变化，能够感知
         */
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        metadata.setTopics(this.subscriptions.groupSubscription());

        /*
         * update metadata (if needed) and keep track of the metadata used for assignment,
         * so that we can check after rebalance completion whether anything has changed
         */
        client.ensureFreshMetadata(); // 更新元数据信息

        isLeader = true;

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}", groupId, assignor.name(), subscriptions);

        /*
         * 执行分区分配，依据具体的分区分配策略（range/round-robin）进行分区
         * 返回结果：key 是消费 ID，value 是对应的分区分配结果
         */
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        /*
         * user-customized assignor may have created some topics
         * that are not in the subscription list and assign their partitions to the members;
         * in this case we would like to update the leader's own metadata with the newly added topics
         * so that it will not trigger a subsequent rebalance when these topics gets updated from metadata refresh.
         *
         * TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol we may need to modify the PartitionAssignor API to better support this case.
         */

        // 记录所有分配的 topic
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignment.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }
        // 如果 group 中存在某些订阅 topic 为分配，则日志记录
        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members in the group {} : {} ", groupId, notAssignedTopics);
        }

        // 如果分配的 topic 包含一些未订阅的 topic
        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            // 日志记录这些未订阅的 topic
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned to group {}, and their metadata will be fetched from the brokers : {}", groupId, newlyAddedTopics);

            // 将这些已分配但是未订阅的 topic 添加到 group 集合中
            allSubscribedTopics.addAll(assignedTopics);
            this.subscriptions.groupSubscribe(allSubscribedTopics);
            metadata.setTopics(this.subscriptions.groupSubscription());
            client.ensureFreshMetadata(); // 更新元数据信息
        }

        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group {}: {}", groupId, assignment);

        // 对结果进行序列化，并返回
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // 如果设置 offset 自动提交，则同步提交 offset
        this.maybeAutoCommitOffsetsSync(rebalanceTimeoutMs);

        // 调用注册的 ConsumerRebalanceListener 监听器的 onPartitionsRevoked 方法
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation", listener.getClass().getName(), groupId, e);
        }

        isLeader = false;
        // 收缩 groupSubscription
        subscriptions.resetGroupSubscription();
    }

    @Override
    public boolean needRejoin() {
        // USER_ASSIGNED 不需要 rebalance
        if (!subscriptions.partitionsAutoAssigned()) {
            return false;
        }

        // we need to rejoin if we performed the assignment and metadata has changed
        // 分区分配过程中分区数量是否发生变化
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot)) {
            return true;
        }

        // we need to join if our subscription has changed since the last join
        // 消费者订阅信息发生变化
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            return true;
        }

        return super.needRejoin();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp)) {
                    this.subscriptions.committed(tp, entry.getValue());
                }
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded()) {
                return future.value();
            }

            if (!future.isRetriable()) {
                throw future.exception();
            }

            time.sleep(retryBackoffMs);
        }
    }

    @Override
    public void close(long timeoutMs) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();

        long now = time.milliseconds();
        long endTimeMs = now + timeoutMs;
        try {
            maybeAutoCommitOffsetsSync(timeoutMs);
            now = time.milliseconds();
            if (pendingAsyncCommits.get() > 0 && endTimeMs > now) {
                ensureCoordinatorReady(now, endTimeMs - now);
                now = time.milliseconds();
            }
        } finally {
            super.close(Math.max(0, endTimeMs - now));
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            // 激活 OffsetCommitCompletion 中的 callback
            completion.invoke();
        }
    }

    /**
     * 异步提交 commit
     *
     * @param offsets
     * @param callback
     */
    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 遍历激活注册的 OffsetCommitCompletion 中的 callback
        this.invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            this.doCommitOffsetsAsync(offsets, callback);
        }
        // 目标节点不可达
        else {
            /*
             * we don't know the current coordinator, so try to find it and then send the commit
             * or fail (we don't want recursive retries which can cause offset commits to arrive out of order).
             * Note that there may be multiple offset commits chained to the same coordinator lookup request.
             * This is fine because the listeners will be invoked in the same order that they were added.
             * Note also that AbstractCoordinator prevents multiple concurrent coordinator lookup requests.
             */
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets, new RetriableCommitFailedException(e)));
                }
            });
        }

        /*
         * ensure the commit has a chance to be transmitted (without blocking on its completion).
         * Note that commits are treated as heartbeats by the coordinator,
         * so there is no need to explicitly allow heartbeats through delayed task execution.
         */
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 标记需要从 GroupCoordinator 获取最近提交的 offset
        subscriptions.needRefreshCommits();
        // 创建并发送 OffsetCommitRequest 请求
        RequestFuture<Void> future = this.sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null) {
                    interceptors.onCommit(offsets);
                }

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     *
     * @param offsets The offsets to be committed
     * @return If the offset commit was successfully sent and a successful response was received from
     * the coordinator
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *                                                               or to any of the specified partitions
     * @throws CommitFailedException                                 if an unrecoverable error occurs before the commit can be completed
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, long timeoutMs) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty()) {
            return true;
        }

        long now = time.milliseconds();
        long startMs = now;
        long remainingMs = timeoutMs;
        do {
            if (coordinatorUnknown()) {
                if (!ensureCoordinatorReady(now, remainingMs)) {
                    return false;
                }

                remainingMs = timeoutMs - (time.milliseconds() - startMs);
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, remainingMs);

            if (future.succeeded()) {
                if (interceptors != null) {
                    interceptors.onCommit(offsets);
                }
                return true;
            }

            if (!future.isRetriable()) {
                throw future.exception();
            }

            time.sleep(retryBackoffMs);

            now = time.milliseconds();
            remainingMs = timeoutMs - (now - startMs);
        } while (remainingMs > 0);

        return false;
    }

    private void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            if (coordinatorUnknown()) {
                this.nextAutoCommitDeadline = now + retryBackoffMs;
            } else if (now >= nextAutoCommitDeadline) {
                this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
                this.doAutoCommitOffsetsAsync();
            }
        }
    }

    public void maybeAutoCommitOffsetsNow() {
        if (autoCommitEnabled && !coordinatorUnknown()) {
            doAutoCommitOffsetsAsync();
        }
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {} for group {}", allConsumedOffsets, groupId);

        this.commitOffsetsAsync(allConsumedOffsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    log.warn("Auto-commit of offsets {} failed for group {}: {}", offsets, groupId, exception.getMessage());
                    if (exception instanceof RetriableException) {
                        nextAutoCommitDeadline = Math.min(time.milliseconds() + retryBackoffMs, nextAutoCommitDeadline);
                    }
                } else {
                    log.debug("Completed auto-commit of offsets {} for group {}", offsets, groupId);
                }
            }
        });
    }

    private void maybeAutoCommitOffsetsSync(long timeoutMs) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {} for group {}", allConsumedOffsets, groupId);
                if (!commitOffsetsSync(allConsumedOffsets, timeoutMs)) {
                    log.debug("Auto-commit of offsets {} for group {} timed out before completion",
                            allConsumedOffsets, groupId);
                }
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} for group {} was interrupted before completion",
                        allConsumedOffsets, groupId);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto-commit of offsets {} failed for group {}: {}", allConsumedOffsets, groupId,
                        e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null) {
                log.error("Offset commit with offsets {} failed for group {}", offsets, groupId, exception);
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) {
            // 如果没有请求的数据，则直接返回
            return RequestFuture.voidSuccess();
        }

        // 获取 coordinator 节点，并检查其可达性
        Node coordinator = this.coordinator();
        if (coordinator == null) {
            return RequestFuture.coordinatorNotAvailable();
        }

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }
            // key 是分区，value 是分区对应的请求数据
            offsetData.put(entry.getKey(),
                    new OffsetCommitRequest.PartitionData(offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        // 获取当前 group 的年代信息
        final Generation generation;
        if (subscriptions.partitionsAutoAssigned()) {
            generation = this.generation();
        } else {
            generation = Generation.NO_GENERATION;
        }
        if (generation == null) {
            /*
             * if the generation is null, we are not part of an active group (and we expect to be).
             * the only thing we can do is fail the commit and let the user rejoin the group in poll()
             */
            return RequestFuture.failure(new CommitFailedException());
        }

        // 创建 OffsetCommitRequest 请求
        OffsetCommitRequest.Builder builder =
                new OffsetCommitRequest.Builder(this.groupId, offsetData).
                        setGenerationId(generation.generationId).
                        setMemberId(generation.memberId).
                        setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);

        log.trace("Sending OffsetCommit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);

        // 发送 OffsetCommitRequest 请求，并注册注解处理器
        return client.send(coordinator, builder).compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            // 遍历对所有分区的响应
            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                // 获取当前分区对应的响应错误码
                Errors error = Errors.forCode(entry.getValue());
                // 正常响应
                if (error == Errors.NONE) {
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp)) {
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                    }
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    resetGeneration();
                    future.raise(new CommitFailedException());
                    return;
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(new KafkaException("Partition " + tp + " may not exist or user may not have Describe access to topic"));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = coordinator();
        if (coordinator == null) {
            return RequestFuture.coordinatorNotAvailable();
        }

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder =
                new OffsetFetchRequest.Builder(this.groupId, new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch for group {} failed: {}", groupId, error.message());

                if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    // re-discover the coordinator and retry
                    coordinatorDead();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Partition " + tp + " may not exist or the user may not have " +
                                "Describe access to the topic"));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                    this.metricGrpName,
                    "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                    this.metricGrpName,
                    "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                    this.metricGrpName,
                    "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                    new Measurable() {
                        @Override
                        public double measure(MetricConfig config, long now) {
                            return subscriptions.assignedPartitions().size();
                        }
                    };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                    this.metricGrpName,
                    "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {

        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null) {
                callback.onComplete(offsets, exception);
            }
        }
    }

}
