/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{JoinGroupRequest, OffsetFetchResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of groups.
 * Groups are assigned to coordinators based on their group names.
 *
 * 每一个 broker 上都会实例化一个 GroupCoordinator 对象，每个 GroupCoordinator 只负责管理消费者 group 的一个子集。
 *
 * GroupCoordinator 的主要功能有：
 *
 * 1. 负责处理 JoinGroupRequest 和 SyncGroupRequest 请求完成分区的分配工作
 * 2. 通过 GroupMetadataManager 和内部 topic 维护消费的 offset 信息，即使出现消费者宕机也可以找回之前提交的 offset
 * 3. 记录消费者 group 相关信息，即使 broker 宕机导致 group 由新的 GroupCoordinator 进行管理，新的 GroupCoordinator 也知道 group 中每个消费者负责处理哪个分区等信息
 * 4. 通过心跳机制检测消费者的运行状态
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig, // 记录 group 中 session 过期的最小时长和最大时长，即超时时长的合法区间
                       val offsetConfig: OffsetConfig, // 记录 OffsetMetadata 相关的配置项
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat], // 管理 DelayedHeartbeat
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin], // 管理 DelayedJoin
                       time: Time) extends Logging {

    type JoinCallback = JoinGroupResult => Unit
    type SyncCallback = (Array[Byte], Short) => Unit

    this.logIdent = "[GroupCoordinator " + brokerId + "]: "

    private val isActive = new AtomicBoolean(false)

    def offsetsTopicConfigs: Properties = {
        val props = new Properties
        props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
        props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
        props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
        props
    }

    /**
     * NOTE: If a group lock and metadataLock are simultaneously needed,
     * be sure to acquire the group lock before metadataLock to prevent deadlock
     */

    /**
     * Startup logic executed at the same time when the server starts up.
     */
    def startup(enableMetadataExpiration: Boolean = true) {
        info("Starting up.")
        if (enableMetadataExpiration)
            groupManager.enableMetadataExpiration()
        isActive.set(true)
        info("Startup complete.")
    }

    /**
     * Shutdown logic executed at the same time when server shuts down.
     * Ordering of actions should be reversed from the startup process.
     */
    def shutdown() {
        info("Shutting down.")
        isActive.set(false)
        groupManager.shutdown()
        heartbeatPurgatory.shutdown()
        joinPurgatory.shutdown()
        info("Shutdown complete.")
    }

    /**
     * 处理 JoinGroupRequest 请求
     *
     * @param groupId
     * @param memberId
     * @param clientId
     * @param clientHost
     * @param rebalanceTimeoutMs
     * @param sessionTimeoutMs
     * @param protocolType
     * @param protocols
     * @param responseCallback
     */
    def handleJoinGroup(groupId: String,
                        memberId: String,
                        clientId: String,
                        clientHost: String,
                        rebalanceTimeoutMs: Int,
                        sessionTimeoutMs: Int,
                        protocolType: String,
                        protocols: List[(String, Array[Byte])],
                        responseCallback: JoinCallback) {
        // 检测 GroupCoordinator 是否启动
        if (!isActive.get) {
            responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
        } else if (!validGroupId(groupId)) { // 检测 groudId 是否合法
            responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
        } else if (!isCoordinatorForGroup(groupId)) { // 检测 GroupCoordinator 是否管理当前 group
            responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
        } else if (isCoordinatorLoadingInProgress(groupId)) { // 检测 GroupCoordinator 是否已经加载了该 group 对应的 offset topic 分区
            responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
        } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
                sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) { // session 时长检测，保证消费者是活跃的
            responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
        } else {
            // only try to create the group if the group is not unknown AND
            // the member id is UNKNOWN, if member is specified but group does not
            // exist we should reject the request
            groupManager.getGroup(groupId) match {
                case None =>
                    if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
                    } else {
                        // 创建 GroupMetadata 对象
                        val group = groupManager.addGroup(new GroupMetadata(groupId))
                        doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
                    }

                case Some(group) =>
                    doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }
        }
    }

    /**
     * P438
     *
     * @param group
     * @param memberId
     * @param clientId
     * @param clientHost
     * @param rebalanceTimeoutMs
     * @param sessionTimeoutMs
     * @param protocolType
     * @param protocols
     * @param responseCallback
     */
    private def doJoinGroup(group: GroupMetadata,
                            memberId: String,
                            clientId: String,
                            clientHost: String,
                            rebalanceTimeoutMs: Int,
                            sessionTimeoutMs: Int,
                            protocolType: String,
                            protocols: List[(String, Array[Byte])],
                            responseCallback: JoinCallback) {

        group synchronized {
            if (!group.is(Empty) && (group.protocolType != Some(protocolType) || // 检测 member 支持的 PartitionAssignor
                    !group.supportsProtocols(protocols.map(_._1).toSet))) {
                // if the new member does not support the group protocol, reject it
                responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
            } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) { // 检测 memberId 是否能够被识别
                // if the member trying to register with a un-recognized id, send the response to let
                // it reset its member id and retry
                responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
            } else {
                // 依据 group 的状态分别进行处理
                group.currentState match {
                    // 直接返回错误码
                    case Dead =>
                        // if the group is marked as dead, it means some other thread has just removed the group
                        // from the coordinator metadata; this is likely that the group has migrated to some other
                        // coordinator OR the group is in a transient unstable phase. Let the member retry
                        // joining without the specified member id,
                        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))

                    case PreparingRebalance =>
                        // 当前为未知的 member，申请加入
                        if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
                        } else {
                            // 已知的 member，重新申请加入
                            val member = group.get(memberId)
                            updateMemberAndRebalance(group, member, protocols, responseCallback)
                        }

                    case AwaitingSync =>
                        if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                            // 未知的 member，申请加入会发生状态切换
                            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
                        } else {
                            // 已知的 member 重新申请加入
                            val member = group.get(memberId)
                            if (member.matches(protocols)) {
                                // member is joining with the same metadata (which could be because it failed to
                                // receive the initial JoinGroup response), so just return current group information
                                // for the current generation.
                                // 支持的 PartitionAssignor 未发生变化，返回 GroupMetadata 的信息
                                responseCallback(JoinGroupResult(
                                    members = if (memberId == group.leaderId) {
                                        group.currentMemberMetadata
                                    } else {
                                        Map.empty
                                    },
                                    memberId = memberId,
                                    generationId = group.generationId,
                                    subProtocol = group.protocol,
                                    leaderId = group.leaderId,
                                    errorCode = Errors.NONE.code))
                            } else {
                                // member has changed metadata, so force a rebalance
                                // 支持的 PartitionAssignor 发生变化，需要更新 member 信息并执行状态切换
                                updateMemberAndRebalance(group, member, protocols, responseCallback)
                            }
                        }

                    case Empty | Stable =>
                        if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                            // if the member id is unknown, register the member to the group
                            // 未知的 member 申请加入，执行状态切换
                            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
                        } else {
                            // 已知的 member 重新申请加入
                            val member = group.get(memberId)
                            if (memberId == group.leaderId || !member.matches(protocols)) {
                                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                                // The latter allows the leader to trigger rebalances for changes affecting assignment
                                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                                // 当前 member 是 group leader 或支持的 PartitionAssignor 发生变化，则更新 member 信息，并执行状态切换
                                updateMemberAndRebalance(group, member, protocols, responseCallback)
                            } else {
                                // for followers with no actual change to their metadata, just return group information
                                // for the current generation which will allow them to issue SyncGroup
                                // 支持的 PartitionAssignor 未发生变化，返回 GroupMetadata 信息
                                responseCallback(JoinGroupResult(
                                    members = Map.empty,
                                    memberId = memberId,
                                    generationId = group.generationId,
                                    subProtocol = group.protocol,
                                    leaderId = group.leaderId,
                                    errorCode = Errors.NONE.code))
                            }
                        }
                }

                // 尝试执行 DelayedJoin
                if (group.is(PreparingRebalance))
                    joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
        }
    }

    /**
     * 处理 SyncGroupRequest 请求
     *
     * @param groupId
     * @param generation
     * @param memberId
     * @param groupAssignment
     * @param responseCallback
     */
    def handleSyncGroup(groupId: String,
                        generation: Int,
                        memberId: String,
                        groupAssignment: Map[String, Array[Byte]],
                        responseCallback: SyncCallback) {
        if (!isActive.get) {
            responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
        } else if (!isCoordinatorForGroup(groupId)) {
            responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
        } else {
            groupManager.getGroup(groupId) match {
                case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
                case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
            }
        }
    }

    /**
     * P452
     *
     * @param group
     * @param generationId
     * @param memberId
     * @param groupAssignment
     * @param responseCallback
     */
    private def doSyncGroup(group: GroupMetadata,
                            generationId: Int,
                            memberId: String,
                            groupAssignment: Map[String, Array[Byte]],
                            responseCallback: SyncCallback) {
        var delayedGroupStore: Option[DelayedStore] = None

        group synchronized {
            if (!group.has(memberId)) {
                responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
            } else if (generationId != group.generationId) {
                responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
            } else {
                group.currentState match {
                    case Empty | Dead =>
                        // 直接返回错误码
                        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
                    case PreparingRebalance =>
                        // 直接返回错误码
                        responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)

                    case AwaitingSync =>
                        // 设置 awaitingSyncCallback 回调函数
                        group.get(memberId).awaitingSyncCallback = responseCallback

                        // if this is the leader, then we can attempt to persist state and transition to stable
                        // 处理 group leader 发来的 SyncGroupRequest 请求
                        if (memberId == group.leaderId) {
                            info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

                            // fill any missing members with an empty assignment
                            // 将未分配到分区的 member 对应的分区结果填充为空的字节数组
                            val missing = group.allMembers -- groupAssignment.keySet
                            val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

                            // 调用 GroupMetadataManager.prepareStoreGroup 方法将 GroupMetadata 相关信息构建消息，写入到对应的 offset topic 分区中
                            delayedGroupStore = groupManager.prepareStoreGroup(group, assignment, (error: Errors) => {
                                group synchronized {
                                    // another member may have joined the group while we were awaiting this callback,
                                    // so we must ensure we are still in the AwaitingSync state and the same generation
                                    // when it gets invoked. if we have transitioned to another state, then do nothing
                                    // 检查 group 的状态和年代信息
                                    if (group.is(AwaitingSync) && generationId == group.generationId) {
                                        if (error != Errors.NONE) {
                                            // 清空分区的分配结果，发送异常响应
                                            resetAndPropagateAssignmentError(group, error)
                                            // 切换 group 状态为 PreparingRebalance
                                            maybePrepareRebalance(group)
                                        } else {
                                            // 设置分区的分配结果，发送正常的 SyncGroupResponse 响应
                                            setAndPropagateAssignment(group, assignment)
                                            group.transitionTo(Stable)
                                        }
                                    }
                                }
                            })
                        }

                    case Stable =>
                        // if the group is stable, we just return the current assignment
                        // 将分配给当前 member 处理的分区信息返回
                        val memberMetadata = group.get(memberId)
                        responseCallback(memberMetadata.assignment, Errors.NONE.code)
                        // 心跳相关操作
                        completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
                }
            }
        }

        // store the group metadata without holding the group lock to avoid the potential
        // for deadlock if the callback is invoked holding other locks (e.g. the replica
        // state change lock)
        delayedGroupStore.foreach(groupManager.store)
    }

    /**
     * 处理 LeaveGroupRequest 请求
     *
     * @param groupId
     * @param memberId
     * @param responseCallback
     */
    def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Short => Unit) {
        if (!isActive.get) {
            responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
        } else if (!isCoordinatorForGroup(groupId)) {
            responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
        } else if (isCoordinatorLoadingInProgress(groupId)) {
            responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
        } else {
            groupManager.getGroup(groupId) match {
                case None =>
                    // if the group is marked as dead, it means some other thread has just removed the group
                    // from the coordinator metadata; this is likely that the group has migrated to some other
                    // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
                    // joining without specified consumer id,
                    responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

                case Some(group) =>
                    group synchronized {
                        if (group.is(Dead) || !group.has(memberId)) {
                            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                        } else {
                            val member = group.get(memberId)
                            // 设置 MemberMetadata.isLeaving 为 true，尝试完成对应的 DelayedHeartbeat
                            removeHeartbeatForLeavingMember(group, member)
                            // 移除对应的 MemberMetadata 对象，并切换状态
                            onMemberFailure(group, member)
                            // 调用回调函数
                            responseCallback(Errors.NONE.code)
                        }
                    }
            }
        }
    }

    /**
     * 处理 HeartbeatRequest 请求
     *
     * @param groupId
     * @param memberId
     * @param generationId
     * @param responseCallback
     */
    def handleHeartbeat(groupId: String,
                        memberId: String,
                        generationId: Int,
                        responseCallback: Short => Unit) {
        if (!isActive.get) {
            responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
        } else if (!isCoordinatorForGroup(groupId)) {
            responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
        } else if (isCoordinatorLoadingInProgress(groupId)) {
            // the group is still loading, so respond just blindly
            responseCallback(Errors.NONE.code)
        } else {
            groupManager.getGroup(groupId) match {
                // 对应的 group 不存在
                case None =>
                    responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

                case Some(group) =>
                    group synchronized {
                        group.currentState match {
                            case Dead =>
                                // if the group is marked as dead, it means some other thread has just removed the group
                                // from the coordinator metadata; this is likely that the group has migrated to some other
                                // coordinator OR the group is in a transient unstable phase. Let the member retry
                                // joining without the specified member id,
                                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

                            case Empty =>
                                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

                            case AwaitingSync =>
                                if (!group.has(memberId))
                                    responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                                else
                                    responseCallback(Errors.REBALANCE_IN_PROGRESS.code)

                            case PreparingRebalance =>
                                if (!group.has(memberId)) {
                                    responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                                } else if (generationId != group.generationId) {
                                    responseCallback(Errors.ILLEGAL_GENERATION.code)
                                } else {
                                    val member = group.get(memberId)
                                    completeAndScheduleNextHeartbeatExpiration(group, member)
                                    responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
                                }

                            case Stable =>
                                if (!group.has(memberId)) {
                                    responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                                } else if (generationId != group.generationId) {
                                    responseCallback(Errors.ILLEGAL_GENERATION.code)
                                } else {
                                    val member = group.get(memberId)
                                    completeAndScheduleNextHeartbeatExpiration(group, member)
                                    responseCallback(Errors.NONE.code)
                                }
                        }
                    }
            }
        }
    }

    /**
     * 处理 OffsetCommitRequest 请求
     *
     * @param groupId
     * @param memberId
     * @param generationId
     * @param offsetMetadata
     * @param responseCallback
     */
    def handleCommitOffsets(groupId: String,
                            memberId: String,
                            generationId: Int,
                            offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                            responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
        if (!isActive.get) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
        } else if (!isCoordinatorForGroup(groupId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
        } else if (isCoordinatorLoadingInProgress(groupId)) {
            responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
        } else {
            groupManager.getGroup(groupId) match {
                // group 对应的 GroupMetadata 不存在
                case None =>
                    // GroupCoordinator 不维护 group 的分区分配结果，只记录提交的 offset 信息
                    if (generationId < 0) {
                        // the group is not relying on Kafka for group management, so allow the commit
                        val group = groupManager.addGroup(new GroupMetadata(groupId))
                        doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
                    } else {
                        // or this is a request coming from an older generation. either way, reject the commit
                        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
                    }

                case Some(group) =>
                    doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
            }
        }
    }

    private def doCommitOffsets(group: GroupMetadata,
                                memberId: String,
                                generationId: Int,
                                offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                                responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
        var delayedOffsetStore: Option[DelayedStore] = None

        group synchronized {
            if (group.is(Dead)) {
                responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
            } else if (generationId < 0 && group.is(Empty)) {
                // the group is only using Kafka to store offsets
                delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
            } else if (group.is(AwaitingSync)) {
                responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
            } else if (!group.has(memberId)) {
                responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
            } else if (generationId != group.generationId) {
                responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
            } else {
                // 将记录 offset 的消息追加到对应的 offset topic 分区中
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
            }
        }

        // store the offsets without holding the group lock
        delayedOffsetStore.foreach(groupManager.store)
    }

    /**
     * 查找分区对应的 offset 信息
     *
     * @param groupId
     * @param partitions
     * @return
     */
    def handleFetchOffsets(groupId: String,
                           partitions: Option[Seq[TopicPartition]] = None): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
        if (!isActive.get)
            (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Map())
        // 检测当前 GroupCoordinator 是否是对应 group 的管理者
        else if (!isCoordinatorForGroup(groupId)) {
            debug("Could not fetch offsets for group %s (not group coordinator).".format(groupId))
            (Errors.NOT_COORDINATOR_FOR_GROUP, Map())

        } else if (isCoordinatorLoadingInProgress(groupId)) // 检测 GroupMetadata 是否已经加载完成
                   (Errors.GROUP_LOAD_IN_PROGRESS, Map())
        else {
            // return offsets blindly regardless the current group state since the group may be using
            // Kafka commit storage without automatic group management
            (Errors.NONE, groupManager.getOffsets(groupId, partitions))
        }
    }

    def handleListGroups(): (Errors, List[GroupOverview]) = {
        if (!isActive.get) {
            (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
        } else {
            val errorCode = if (groupManager.isLoading) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
            (errorCode, groupManager.currentGroups.map(_.overview).toList)
        }
    }

    def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
        if (!isActive.get) {
            (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
        } else if (!isCoordinatorForGroup(groupId)) {
            (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
        } else if (isCoordinatorLoadingInProgress(groupId)) {
            (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
        } else {
            groupManager.getGroup(groupId) match {
                case None => (Errors.NONE, GroupCoordinator.DeadGroup)
                case Some(group) =>
                    group synchronized {
                        (Errors.NONE, group.summary)
                    }
            }
        }
    }

    def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
        groupManager.cleanupGroupMetadata(Some(topicPartitions))
    }

    /**
     * 在 GroupMetadata 被删除前，将 group 状态切换成 Dead，并根据之前的 group 状态进行相应的清理工作
     *
     * @param group
     */
    private def onGroupUnloaded(group: GroupMetadata) {
        group synchronized {
            info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
            val previousState = group.currentState
            // 切换成 Dead 状态
            group.transitionTo(Dead)

            previousState match {
                case Empty | Dead =>
                case PreparingRebalance =>
                    // 遍历调用所有 member 的 awaitingJoinCallback 函数，返回对应的错误码
                    for (member <- group.allMemberMetadata) {
                        if (member.awaitingJoinCallback != null) {
                            member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
                            member.awaitingJoinCallback = null
                        }
                    }
                    // 尝试执行 DelayedJoin 操作
                    joinPurgatory.checkAndComplete(GroupKey(group.groupId))

                case Stable | AwaitingSync =>
                    // 遍历调用所有 member 的 awaitingJoinCallback 函数，返回对应的错误码
                    for (member <- group.allMemberMetadata) {
                        if (member.awaitingSyncCallback != null) {
                            member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
                            member.awaitingSyncCallback = null
                        }
                        // 尝试执行 DelayHeartbeat
                        heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
                    }
            }
        }
    }

    /**
     * 当出现 GroupMetadata 重复加载时，更新心跳
     *
     * @param group
     */
    private def onGroupLoaded(group: GroupMetadata) {
        group synchronized {
            info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
            assert(group.is(Stable) || group.is(Empty))
            // 遍历更新素有 member 的心跳
            group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
        }
    }

    def handleGroupImmigration(offsetTopicPartitionId: Int) {
        groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
    }

    /**
     * 当 broker 成为 offset topic 分区的 follower 副本时会回调该方法执行清理工作
     *
     * @param offsetTopicPartitionId
     */
    def handleGroupEmigration(offsetTopicPartitionId: Int) {
        groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
    }

    /**
     * @param group
     * @param assignment
     */
    private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
        assert(group.is(AwaitingSync))
        // 更新 GroupMetadata 中每个相关的 MemberMetadata 的 assignment 字段
        group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
        propagateAssignment(group, Errors.NONE)
    }

    private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
        assert(group.is(AwaitingSync))
        // 清空所有 MemberMetadata 的 assignment 字段
        group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
        propagateAssignment(group, error)
    }

    private def propagateAssignment(group: GroupMetadata, error: Errors) {
        for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
                // 调用 MemberMetadata 的 awaitingSyncCallback 回调函数
                member.awaitingSyncCallback(member.assignment, error.code)
                member.awaitingSyncCallback = null

                // reset the session timeout for members after propagating the member's assignment.
                // This is because if any member's session expired while we were still awaiting either
                // the leader sync group or the storage callback, its expiration will be ignored and no
                // future heartbeat expectations will not be scheduled.
                // 开启等待下次心跳的延迟任务
                completeAndScheduleNextHeartbeatExpiration(group, member)
            }
        }
    }

    private def validGroupId(groupId: String): Boolean = {
        groupId != null && !groupId.isEmpty
    }

    private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
        JoinGroupResult(
            members = Map.empty,
            memberId = memberId,
            generationId = 0,
            subProtocol = GroupCoordinator.NoProtocol,
            leaderId = GroupCoordinator.NoLeader,
            errorCode = errorCode)
    }

    /**
     * Complete existing DelayedHeartbeats for the given member and schedule the next one
     *
     * 更新对应的 member 的心跳时间戳，尝试执行对应的 DelayedHeartbeat，
     * 并创建新的 DelayedHeartbeat 对象放入 heartbeatPurgatory 中等待下次心跳到来或 DelayedHeartbeat 超时
     */
    private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
        // complete current heartbeat expectation
        // 更新心跳时间
        member.latestHeartbeat = time.milliseconds()
        // 获取 DelayedHeartbeat 对应的 key
        val memberKey = MemberKey(member.groupId, member.memberId)
        // 尝试完成之前添加的 DelayedHeartbeat
        heartbeatPurgatory.checkAndComplete(memberKey)

        // reschedule the next heartbeat expiration deadline
        // 计算下一次的心跳超时时间
        val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
        // 创建新的 DelayedHeartbeat 对象，并添加到 heartbeatPurgatory 中进行管理
        val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
        heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
    }

    private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
        member.isLeaving = true
        val memberKey = MemberKey(member.groupId, member.memberId)
        heartbeatPurgatory.checkAndComplete(memberKey)
    }

    /**
     * 添加 member 信息，执行 GroupMetadata 的状态切换
     *
     * @param rebalanceTimeoutMs
     * @param sessionTimeoutMs
     * @param clientId
     * @param clientHost
     * @param protocolType
     * @param protocols
     * @param group
     * @param callback
     * @return
     */
    private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                      sessionTimeoutMs: Int,
                                      clientId: String,
                                      clientHost: String,
                                      protocolType: String,
                                      protocols: List[(String, Array[Byte])],
                                      group: GroupMetadata,
                                      callback: JoinCallback): MemberMetadata = {
        // use the client-id with a random id suffix as the member-id
        // clientId-UUID
        val memberId = clientId + "-" + group.generateMemberIdSuffix
        // 创建新的 MemberMetadata 对象
        val member = new MemberMetadata(memberId, group.groupId, clientId,
            clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols)
        // 设置回调函数
        member.awaitingJoinCallback = callback
        // 添加到 GroupMetadata 中
        group.add(member)
        // 尝试执行状态切换
        maybePrepareRebalance(group)
        member
    }

    /**
     * 更新 member 信息，执行 GroupMetadata 的状态切换
     *
     * @param group
     * @param member
     * @param protocols
     * @param callback
     */
    private def updateMemberAndRebalance(group: GroupMetadata,
                                         member: MemberMetadata,
                                         protocols: List[(String, Array[Byte])],
                                         callback: JoinCallback) {
        // 更新 MemberMetadata 支持的协议
        member.supportedProtocols = protocols
        // 更新 MemberMetadata 的 awaitingJoinCallback 回调函数
        member.awaitingJoinCallback = callback
        // 尝试执行状态切换
        maybePrepareRebalance(group)
    }

    private def maybePrepareRebalance(group: GroupMetadata) {
        group synchronized {
            if (group.canRebalance)
                prepareRebalance(group)
        }
    }

    /**
     * 切换状态为 PreparingRebalance，并创建相应的 DelayedJoin
     *
     * @param group
     */
    private def prepareRebalance(group: GroupMetadata) {
        // if any members are awaiting sync, cancel their request and have them rejoin
        // 如果处于 AwaitingSync 状态，则先要重置 MemberMetadata.assignment 字段，
        // 并回调 awaitingSyncCallback 函数向消费者返回对应错误码
        if (group.is(AwaitingSync))
            resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

        // 将 group 状态切换成 PreparingRebalance 状态，表示准备执行再平衡操作
        group.transitionTo(PreparingRebalance)
        info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

        // 超时时长是所有消费者设置的超时时长的最大值
        val rebalanceTimeout = group.rebalanceTimeoutMs
        // 创建 DelayedJoin 对象
        val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)
        val groupKey = GroupKey(group.groupId)
        // 尝试立即完成 DelayedJoin，否则将 DelayedFetch 添加到 joinPurgatory 中
        joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
    }

    private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
        trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
        // 将对应的 member 从 GroupMetadata 中删除
        group.remove(member.memberId)
        group.currentState match {
            case Dead | Empty =>
            // 之前的分区分配可能已经失效，切换 GroupMetadata 状态为 PreparingRebalance
            case Stable | AwaitingSync => maybePrepareRebalance(group)
            // GroupMetadata 中的 member 减少，可能满足 DelayedJoin 的执行条件，尝试执行
            case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
        }
    }

    def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean): Boolean = {
        group synchronized {
            // 判断已知 member 是否已经申请加入
            if (group.notYetRejoinedMembers.isEmpty)
                forceComplete()
            else false
        }
    }

    def onExpireJoin() {
        // TODO: add metrics for restabilize timeouts
    }

    def onCompleteJoin(group: GroupMetadata) {
        var delayedStore: Option[DelayedStore] = None
        group synchronized {
            // remove any members who haven't joined the group yet
            // 获取并移除未重新加入的已知的 member 集合
            group.notYetRejoinedMembers.foreach { failedMember =>
                group.remove(failedMember.memberId)
                // TODO: cut the socket connection to the client
            }

            if (!group.is(Dead)) {
                // 递增 group 的年代信息，并选择 group 最终使用的 PartitionAssignor
                group.initNextGeneration()
                // 如果 GroupMetadata 中已经没有 member，则将其切换成 Dead 状态
                if (group.is(Empty)) {
                    info(s"Group ${group.groupId} with generation ${group.generationId} is now empty")

                    delayedStore = groupManager.prepareStoreGroup(group, Map.empty, error => {
                        if (error != Errors.NONE) {
                            // we failed to write the empty group metadata. If the broker fails before another rebalance,
                            // the previous generation written to the log will become active again (and most likely timeout).
                            // This should be safe since there are no active members in an empty generation, so we just warn.
                            warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
                        }
                    })
                } else {
                    info(s"Stabilized group ${group.groupId} generation ${group.generationId}")

                    // trigger the awaiting join group response callback for all the members after rebalancing
                    // 向 GroupMetadata 中所有的 member 发送 JoinGroupResponse 响应
                    for (member <- group.allMemberMetadata) {
                        assert(member.awaitingJoinCallback != null)
                        val joinResult = JoinGroupResult(
                            members = if (member.memberId == group.leaderId) {
                                group.currentMemberMetadata
                            } else {
                                Map.empty
                            },
                            memberId = member.memberId,
                            generationId = group.generationId,
                            subProtocol = group.protocol,
                            leaderId = group.leaderId,
                            errorCode = Errors.NONE.code)

                        // 该回调函数在 KafkaApis.handleJoinGroupRequest 中定义（对应 sendResponseCallback 方法），用于将响应对象放入 channel 中等待发送
                        member.awaitingJoinCallback(joinResult)
                        member.awaitingJoinCallback = null
                        // 心跳机制
                        completeAndScheduleNextHeartbeatExpiration(group, member)
                    }
                }
            }
        }

        // call without holding the group lock
        delayedStore.foreach(groupManager.store)
    }

    /**
     * 符合以下 4 个条件之一则认为可以执行 DelayedHeartbeat：
     * 1. 最后一次收到心跳的时间与 heartbeatDeadline 的差距大于 sessionTimeout
     * 2. awaitingJoinCallback 不为 null，即消费者正在等待 JoinGroupResponse
     * 3. awaitingSyncCallback 不为 null，即消费者正在等待 SyncGroupResponse
     * 4. 消费者已经离开了当前 group
     *
     * @param group
     * @param member
     * @param heartbeatDeadline
     * @param forceComplete
     * @return
     */
    def tryCompleteHeartbeat(group: GroupMetadata,
                             member: MemberMetadata,
                             heartbeatDeadline: Long,
                             forceComplete: () => Boolean): Boolean = {
        group synchronized {
            if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
                forceComplete()
            else false
        }
    }

    /**
     * 将对应的 member 从 GroupMetadata 中删除，并按照 GroupMetadata 的状态分而治之
     *
     * @param group
     * @param member
     * @param heartbeatDeadline
     */
    def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
        group synchronized {
            // 再次检测 member 是否下线
            if (!shouldKeepMemberAlive(member, heartbeatDeadline))
            // 确实已经下线
                onMemberFailure(group, member)
        }
    }

    def onCompleteHeartbeat() {
        // TODO: add metrics for complete heartbeats
    }

    def partitionFor(group: String): Int = groupManager.partitionFor(group)

    private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long): Boolean =
        member.awaitingJoinCallback != null ||
                member.awaitingSyncCallback != null ||
                member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

    private def isCoordinatorForGroup(groupId: String): Boolean = groupManager.isGroupLocal(groupId)

    private def isCoordinatorLoadingInProgress(groupId: String): Boolean = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

    val NoState = ""
    val NoProtocolType = ""
    val NoProtocol = ""
    val NoLeader = ""
    val NoMembers: List[MemberSummary] = List[MemberSummary]()
    val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
    val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

    def apply(config: KafkaConfig,
              zkUtils: ZkUtils,
              replicaManager: ReplicaManager,
              time: Time): GroupCoordinator = {
        val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
        val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
        apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
    }

    private[coordinator] def offsetConfig(config: KafkaConfig) = OffsetConfig(
        maxMetadataSize = config.offsetMetadataMaxSize,
        loadBufferSize = config.offsetsLoadBufferSize,
        offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
        offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
        offsetsTopicNumPartitions = config.offsetsTopicPartitions,
        offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
        offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
        offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
        offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
        offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
    )

    def apply(config: KafkaConfig,
              zkUtils: ZkUtils,
              replicaManager: ReplicaManager,
              heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
              joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
              time: Time): GroupCoordinator = {
        val offsetConfig = this.offsetConfig(config)
        val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
            groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

        val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
            offsetConfig, replicaManager, zkUtils, time)
        new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
    }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)
