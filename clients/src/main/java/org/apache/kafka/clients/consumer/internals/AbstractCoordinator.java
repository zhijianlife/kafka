/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 * <li>Group Registration: Group members register with the coordinator providing their own metadata
 * (such as the set of topics they are interested in).</li>
 * <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 * as the leader.</li>
 * <li>State Assignment: The leader collects the metadata from all the members of the group and
 * assigns state.</li>
 * <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 * processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    /**
     * 消费者状态
     */
    private enum MemberState {
        UNJOINED,    // the client is not part of a group
        REBALANCING, // the client has begun rebalancing
        STABLE,      // the client has joined and is sending heartbeats
    }

    /** 分区再平衡操作超时时间 */
    protected final int rebalanceTimeoutMs;
    /** 消费者与服务端会话超时时间，超过该时间则认为与服务端断开连接 */
    private final int sessionTimeoutMs;
    /** 指定消费者被关闭时是否离开所属 group，如果为 true 的话会触发分区再平衡操作 */
    private final boolean leaveGroupOnClose;
    private final GroupCoordinatorMetrics sensors;

    /** 心跳机制 */
    private final Heartbeat heartbeat;
    /** 执行心跳机制的线程 */
    private HeartbeatThread heartbeatThread = null;

    /** 当前消费者所属的 group */
    protected final String groupId;
    /** 网络通信客户端 */
    protected final ConsumerNetworkClient client;
    /** 时间戳工具 */
    protected final Time time;
    /** 重试时间间隔 */
    protected final long retryBackoffMs;

    /** 标记是否需要重新发送 {@link JoinGroupRequest} 的请求条件之一 */
    private boolean rejoinNeeded = true;
    /** 标记是否需要执行发送 {@link JoinGroupRequest} 请求前的准备工作 */
    private boolean needsJoinPrepare = true;
    /** 记录当前消费者的运行状态 */
    private MemberState state = MemberState.UNJOINED;
    /** 分区再平衡操作请求对应的 future 对象，避免多个请求同时执行 */
    private RequestFuture<ByteBuffer> joinFuture = null;
    /** 服务端 GroupCoordinator 所在节点 */
    private Node coordinator = null;
    /** 服务端 GroupCoordinator 返回的年代信息，用于区分两次分区再平衡操作 */
    private Generation generation = Generation.NO_GENERATION;
    /** 获取可用 GroupCoordinator 节点请求对应的 future，避免多个请求同时执行 */
    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean leaveGroupOnClose) {
        this.client = client;
        this.time = time;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
        this.heartbeat = new Heartbeat(sessionTimeoutMs, heartbeatIntervalMs, rebalanceTimeoutMs, retryBackoffMs);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     *
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     *
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     *
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     *
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     */
    public synchronized void ensureCoordinatorReady() {
        // Using zero as current time since timeout is effectively infinite
        this.ensureCoordinatorReady(0, Long.MAX_VALUE);
    }

    /**
     * Ensure that the coordinator is ready to receive requests.
     *
     * 检查目标 coordinator 节点是否准备好接收请求
     *
     * @param startTimeMs Current time in milliseconds
     * @param timeoutMs Maximum time to wait to discover the coordinator
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    protected synchronized boolean ensureCoordinatorReady(long startTimeMs, long timeoutMs) {
        long remainingMs = timeoutMs;

        // 如果对应的 GroupCoordinator 节点不可达，则重新查找可用的 GroupCoordinator 节点
        while (this.coordinatorUnknown()) {
            // 查找负载最小且可用的 GroupCoordinator 节点，并发送 GroupCoordinatorRequest 请求
            RequestFuture<Void> future = this.lookupCoordinator();
            client.poll(future, remainingMs);

            // 查找 GroupCoordinator 节点失败，如果运行重试，则等待集群元数据更新后重试
            if (future.failed()) {
                if (future.isRetriable()) {
                    remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
                    if (remainingMs <= 0) {
                        break;
                    }
                    log.debug("Coordinator discovery failed for group {}, refreshing metadata", groupId);
                    client.awaitMetadataUpdate(remainingMs);
                } else {
                    throw future.exception();
                }
            }
            // 查找到可用节点，但是建立连接失败，稍后重试
            else if (coordinator != null && client.connectionFailed(coordinator)) {
                /*
                 * we found the coordinator, but the connection has failed,
                 * so mark it dead and backoff before retrying discovery
                 */
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }

            remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
            if (remainingMs <= 0) {
                // 超时
                break;
            }
        }

        // 返回是否有可用的 coordinator 节点
        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // 获取负载最小的节点
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                // TODO: If there are no brokers left, perhaps we should use the bootstrap set
                // from configuration?
                log.debug("No broker available to send GroupCoordinator request for group {}", groupId);
                // 如果没有可用的节点，抛出 NoAvailableBrokersException 异常
                return RequestFuture.noBrokersAvailable();
            } else {
                // 像该节点发送 GroupCoordinatorRequest 请求，并返回结果
                findCoordinatorFuture = this.sendGroupCoordinatorRequest(node);
            }
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     *
     * @return true if it should, false otherwise
     */
    protected synchronized boolean needRejoin() {
        return rejoinNeeded;
    }

    private synchronized boolean rejoinIncomplete() {
        return joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }

            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group, we don't need to send heartbeats
        if (state == MemberState.UNJOINED) {
            return Long.MAX_VALUE;
        }
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     *
     * always ensure that the coordinator is ready because we may have been disconnected
     * when sending heartbeats and does not necessarily require us to rejoin the group.
     */
    public void ensureActiveGroup() {
        // 检查目标 GroupCoordinator 节点是否准备好接收请求
        this.ensureCoordinatorReady();
        // 启动心跳线程
        this.startHeartbeatThreadIfNeeded();
        // 执行 join group 操作
        this.joinGroupIfNeeded();
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private synchronized void disableHeartbeatThread() {
        if (heartbeatThread != null) {
            heartbeatThread.disable();
        }
    }

    private void closeHeartbeatThread() {
        if (heartbeatThread != null) {
            heartbeatThread.close();

            try {
                heartbeatThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for consumer heartbeat thread to close");
                throw new InterruptException(e);
            }
        }
    }

    /**
     * visible for testing. Joins the group without starting the heartbeat thread.
     */
    void joinGroupIfNeeded() {
        // 如果需要执行分区再平衡，且目前正在进行中
        while (this.needRejoin() || this.rejoinIncomplete()) {
            // 再次检查目标 GroupCoordinator 节点是否准备好接收请求
            this.ensureCoordinatorReady();

            // 执行前期准备工作
            if (needsJoinPrepare) {
                /*
                 * 1. 如果开启了 offset 自动提交，则同步提交 offset
                 * 2. 调用注册的 ConsumerRebalanceListener 监听器的 onPartitionsRevoked 方法
                 * 3. 取消当前消费者的 leader 身份（如果是的话），恢复成为一个普通的消费者
                 */
                this.onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }

            // 创建并发送 JoinGroupRequest 请求，申请加入目标 group
            RequestFuture<ByteBuffer> future = this.initiateJoinGroup();
            client.poll(future);
            // 申请加入 group 完成，将 joinFuture 置为 null，表示允许发送下一次 JoinGroupRequest 请求
            this.resetJoinGroupFuture();

            // 执行分区分配成功
            if (future.succeeded()) {
                needsJoinPrepare = true;
                this.onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());
            }
            // 执行分区分配失败，依据失败类型考虑是否重试
            else {
                RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException) {
                    continue;
                } else if (!future.isRetriable()) {
                    throw exception;
                }
                time.sleep(retryBackoffMs);
            }
        }
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    /**
     * 发起 join group
     *
     * @return
     */
    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // 如果当前正在执行分区再平衡操作，则 joinFuture 不为 null，防止重复执行
        if (joinFuture == null) {
            // 临时关闭心跳机制，防止干扰分区再平衡的过程
            this.disableHeartbeatThread();

            // 设置消费者状态为 REBALANCING，表示正在执行分区再平衡操作
            state = MemberState.REBALANCING;
            // 创建并缓存 JoinGroupRequest 请求，等到 poll 操作时发送
            joinFuture = this.sendJoinGroupRequest();
            // 添加监听器
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // 申请加入目标 group 成功
                    synchronized (AbstractCoordinator.this) {
                        log.info("Successfully joined group {} with generation {}", groupId, generation.generationId);
                        state = MemberState.STABLE;
                        if (heartbeatThread != null) {
                            // 重新启动心跳机制，防止被误判为离线
                            heartbeatThread.enable();
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // 申请加入目标 group 失败
                    synchronized (AbstractCoordinator.this) {
                        state = MemberState.UNJOINED;
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (this.coordinatorUnknown()) {
            // 如果目标 GroupCoordinator 节点不可达，则返回异常
            return RequestFuture.coordinatorNotAvailable();
        }

        log.info("(Re-)joining group {}", groupId);
        // 构建 JoinGroupRequest 请求
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                groupId,
                sessionTimeoutMs,
                generation.memberId,
                protocolType(),
                metadata()).setRebalanceTimeout(rebalanceTimeoutMs);

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);
        // 发送 JoinGroupRequest 请求，并注册结果处理器 JoinGroupResponseHandler
        return client.send(coordinator, requestBuilder).compose(new JoinGroupResponseHandler());
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {

        /**
         * 处理 JoinGroupResponse 响应
         *
         * @param joinResponse join group 响应
         * @param future 对应上面方法 sendJoinGroupRequest 返回的 RequestFuture 对象
         */
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            // 获取响应错误码
            Errors error = Errors.forCode(joinResponse.errorCode());
            // 正常响应
            if (error == Errors.NONE) {
                log.debug("Received successful JoinGroup response for group {}: {}", groupId, joinResponse);
                sensors.joinLatency.record(response.requestLatencyMs());

                synchronized (AbstractCoordinator.this) {
                    if (state != MemberState.REBALANCING) {
                        // 在接收到响应之前，消费者的状态发生变更（可能已经从所属 group 离开），抛出异常
                        future.raise(new UnjoinedGroupException());
                    } else {
                        // 基于响应，更新 group 的年代信息
                        generation = new Generation(
                                joinResponse.generationId(), joinResponse.memberId(), joinResponse.groupProtocol());
                        rejoinNeeded = false;
                        // 如果当前消费者是 group 中的 leader 角色
                        if (joinResponse.isLeader()) {
                            /*
                             * 基于分区分配策略执行分区分配，leader 需要关注当前 group 中所有消费者订阅的 topic，
                             * 并发送 SyncGroupRequest 请求反馈分区分配结果给 GroupCoordinator 节点
                             */
                            onJoinLeader(joinResponse)
                                    // 这里调用 chain 方法，是希望当 SyncGroupResponse 处理完成之后，能够将结果传递给 future
                                    .chain(future);
                        } else {
                            // 如果是 follower 消费者，则只关注自己订阅的 topic，这一步仅发送 SyncGroupRequest 请求
                            onJoinFollower().chain(future);
                        }
                    }
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId, coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                resetGeneration();
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, requestBuilder);
        return this.sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // 基于分区分配策略分配分区
            Map<String, ByteBuffer> groupAssignment = this.performAssignment(
                    joinResponse.leaderId(), joinResponse.groupProtocol(), joinResponse.members());

            // 创建 SyncGroupRequest 请求，反馈分区分配结果给 GroupCoordinator 节点
            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, requestBuilder);
            // 发送 SyncGroupRequest 请求
            return this.sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown()) {
            return RequestFuture.coordinatorNotAvailable();
        }
        return client.send(coordinator, requestBuilder).compose(new SyncGroupResponseHandler());
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        @Override
        public void handle(SyncGroupResponse syncResponse, RequestFuture<ByteBuffer> future) {
            // 解析响应错误码
            Errors error = Errors.forCode(syncResponse.errorCode());
            // 正常响应
            if (error == Errors.NONE) {
                sensors.syncLatency.record(response.requestLatencyMs());
                // 将分区分配结果传递给调用方
                future.complete(syncResponse.memberAssignment());
            }
            // 异常响应
            else {
                // 标记需要重新发送 JoinGroupRequest 请求
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    resetGeneration();
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    coordinatorDead();
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group.
     * Sends a GroupMetadata request to one of the brokers.
     * The returned future should be polled to get the result of the request.
     *
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending GroupCoordinator request for group {} to broker {}", groupId, node);
        GroupCoordinatorRequest.Builder requestBuilder = new GroupCoordinatorRequest.Builder(this.groupId);
        // 向目标 coordinator 发送 GroupCoordinatorRequest 请求，并注册响应处理器
        return client.send(node, requestBuilder).compose(new GroupCoordinatorResponseHandler());
    }

    /**
     * GroupCoordinatorResponse 处理器
     */
    private class GroupCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received GroupCoordinator response {} for group {}", resp, groupId);

            GroupCoordinatorResponse groupCoordinatorResponse = (GroupCoordinatorResponse) resp.responseBody();
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            // 解析响应错误码
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            clearFindCoordinatorFuture();
            // 正常响应
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // 更新本地缓存的 coordinator 节点信息
                    coordinator = new Node(
                            Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                            groupCoordinatorResponse.node().host(),
                            groupCoordinatorResponse.node().port());
                    log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
                    // 尝试建立连接
                    client.tryConnect(coordinator);
                    heartbeat.resetTimeouts(time.milliseconds());
                }
                future.complete(null);
            }
            // 权限认证失败
            else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            }
            // 其他错误则直接返回
            else {
                log.debug("Group coordinator lookup for group {} failed: {}", groupId, error.message());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     *
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        // 如果为 null 表示对应的 coordinator 不可达
        return coordinator() == null;
    }

    /**
     * Get the current coordinator
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node coordinator() {
        // 如果 coordinator 节点无法建立连接，则返回 null
        if (coordinator != null && client.connectionFailed(coordinator)) {
            // 清空所有发往该 coordinator 节点的 unsent 请求
            coordinatorDead();
            return null;
        }
        return coordinator;
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected synchronized void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            this.coordinator = null;
        }
    }

    /**
     * Get the current generation state if the group is stable.
     *
     * @return the current generation or null if the group is unjoined/rebalancing
     */
    protected synchronized Generation generation() {
        if (this.state != MemberState.STABLE) {
            return null;
        }
        return generation;
    }

    /**
     * Reset the generation and memberId because we have fallen out of the group.
     */
    protected synchronized void resetGeneration() {
        this.generation = Generation.NO_GENERATION;
        this.rejoinNeeded = true;
        this.state = MemberState.UNJOINED;
    }

    protected synchronized void requestRejoin() {
        this.rejoinNeeded = true;
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(0);
    }

    protected void close(long timeoutMs) {
        try {
            closeHeartbeatThread();
        } finally {

            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (leaveGroupOnClose) {
                    // 如果设置关闭消费者则视为离开 group，则需要触发再平衡操作
                    maybeLeaveGroup();
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = coordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timeoutMs)) {
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections for group {}.",
                            client.pendingRequestCount(coordinator), groupId);
                }
            }
        }
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public synchronized void maybeLeaveGroup() {
        if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {
            // this is a minimal effort attempt to leave the group.
            // we do not attempt any resending if the request fails or times out.
            log.debug("Sending LeaveGroup request to coordinator {} for group {}", coordinator, groupId);
            // 构建并发送 LeaveGroupRequest 请求
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(groupId, generation.memberId);
            client.send(coordinator, request).compose(new LeaveGroupResponseHandler());
            client.pollNoWakeup();
        }
        // 重置年代信息
        this.resetGeneration();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            Errors error = Errors.forCode(leaveResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("LeaveGroup request for group {} returned successfully", groupId);
                future.complete(null);
            } else {
                log.debug("LeaveGroup request for group {} failed with error: {}", groupId, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request for group {} to coordinator {}", groupId, coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(this.groupId, this.generation.generationId, this.generation.memberId);
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler());
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            // 获取响应错误码
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            // 心跳正常
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response for group {}", groupId);
                future.complete(null);
            }
            // GroupCoordinator 不可用或找不到
            else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                log.debug("Attempt to heartbeat failed for group {} since coordinator {} is either not started or not valid.", groupId, coordinator());
                // 清空 unsent 中对应的请求
                coordinatorDead();
                future.raise(error);
            }
            // 正在指定 rebalance
            else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.debug("Attempt to heartbeat failed for group {} since it is rebalancing.", groupId);
                // 标记需要重新发送 JoinGroupRequest 请求
                requestRejoin();
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            }
            // 非法的 generation id
            else if (error == Errors.ILLEGAL_GENERATION) {
                log.debug("Attempt to heartbeat failed for group {} since generation id is not legal.", groupId);
                // 重置 generation 和 member 状态，并标记需要重新发送 JoinGroupRequest 请求
                resetGeneration();
                future.raise(Errors.ILLEGAL_GENERATION);
            }
            // 非法的 member id
            else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.debug("Attempt to heartbeat failed for group {} since member id is not valid.", groupId);
                // 重置 generation 和 member 状态，并标记需要重新发送 JoinGroupRequest 请求
                resetGeneration();
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            }
            // 权限认证失败
            else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {

        /** 待处理的响应 */
        protected ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                coordinatorDead();
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                // 对响应进行处理
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone()) {
                    future.raise(e);
                }
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                    this.metricGrpName,
                    "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                    this.metricGrpName,
                    "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                    new Measurable() {
                        @Override
                        public double measure(MetricConfig config, long now) {
                            return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                        }
                    };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                    this.metricGrpName,
                    "The number of seconds since the last controller heartbeat was sent"),
                    lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread {
        private boolean enabled = false;
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super("kafka-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.trace("Enabling heartbeat thread for group {}", groupId);
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.trace("Disabling heartbeat thread for group {}", groupId);
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread for group {} started", groupId);
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed) {
                            return;
                        }

                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture == null) {
                                lookupCoordinator();
                            } else {
                                AbstractCoordinator.this.wait(retryBackoffMs);
                            }
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            coordinatorDead();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            maybeLeaveGroup();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);

                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();

                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
                this.failed.set(new RuntimeException(e));
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error", groupId, e);
                this.failed.set(e);
            } finally {
                log.debug("Heartbeat thread for group {} has closed", groupId);
            }
        }

    }

    protected static class Generation {

        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocol;

        public Generation(int generationId, String memberId, String protocol) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocol = protocol;
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

        private static final long serialVersionUID = -2291999808391175603L;
    }

}
