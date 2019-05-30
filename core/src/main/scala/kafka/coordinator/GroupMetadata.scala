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

import java.util.UUID

import kafka.common.OffsetAndMetadata
import kafka.utils.nonthreadsafe
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, immutable, mutable}

/**
 * 表示消费者 group 的状态，用于服务端 GroupCoordinator 管理 group
 */
private[coordinator] sealed trait GroupState {
    def state: Byte
}

/**
 * Group is preparing to rebalance
 *
 * action:
 * 1. respond to heartbeats with REBALANCE_IN_PROGRESS
 * 2. respond to sync group with REBALANCE_IN_PROGRESS
 * 3. remove member on leave group request
 * 4. park join group requests from new or existing members until all expected members have joined
 * 5. allow offset commits from previous generation
 * 6. allow offset fetch requests
 *
 * transition:
 * 1. some members have joined by the timeout => AwaitingSync
 * 2. all members have left the group => Empty
 * 3. group is removed by partition emigration => Dead
 *
 */
private[coordinator] case object PreparingRebalance extends GroupState {
    val state: Byte = 1
}

/**
 * Group is awaiting state assignment from the leader
 *
 * action:
 * 1. respond to heartbeats with REBALANCE_IN_PROGRESS
 * 2. respond to offset commits with REBALANCE_IN_PROGRESS
 * 3. park sync group requests from followers until transition to Stable
 * 4. allow offset fetch requests
 *
 * transition:
 * 1. sync group with state assignment received from leader => Stable
 * 2. join group from new member or existing member with updated metadata => PreparingRebalance
 * 3. leave group from existing member => PreparingRebalance
 * 4. member failure detected => PreparingRebalance
 * 5. group is removed by partition emigration => Dead
 *
 * 正在等待 group leader 将分区的分配结果发送给 GroupCoordinator
 */
private[coordinator] case object AwaitingSync extends GroupState {
    val state: Byte = 5
}

/**
 * Group is stable
 *
 * action:
 * 1. respond to member heartbeats normally
 * 2. respond to sync group from any member with current assignment
 * 3. respond to join group from followers with matching metadata with current group metadata
 * 4. allow offset commits from member of current generation
 * 5. allow offset fetch requests
 *
 * transition:
 * 1. member failure detected via heartbeat => PreparingRebalance
 * 2. leave group from existing member => PreparingRebalance
 * 3. leader join-group received => PreparingRebalance
 * 4. follower join-group with new metadata => PreparingRebalance
 * 5. group is removed by partition emigration => Dead
 *
 * 表示 group 处于正常状态，初始状态
 */
private[coordinator] case object Stable extends GroupState {
    val state: Byte = 3
}

/**
 * Group has no more members and its metadata is being removed
 *
 * action:
 * 1. respond to join group with UNKNOWN_MEMBER_ID
 * 2. respond to sync group with UNKNOWN_MEMBER_ID
 * 3. respond to heartbeat with UNKNOWN_MEMBER_ID
 * 4. respond to leave group with UNKNOWN_MEMBER_ID
 * 5. respond to offset commit with UNKNOWN_MEMBER_ID
 * 6. allow offset fetch requests
 *
 * transition:
 * 1. Dead is a final state before group metadata is cleaned up, so there are no transitions
 *
 * 处于该状态的 group 已经没有 member，并且对应的 metadata 已经被删除
 */
private[coordinator] case object Dead extends GroupState {
    val state: Byte = 4
}

/**
 * Group has no more members, but lingers until all offsets have expired. This state
 * also represents groups which use Kafka only for offset commits and have no members.
 *
 * action:
 * 1. respond normally to join group from new members
 * 2. respond to sync group with UNKNOWN_MEMBER_ID
 * 3. respond to heartbeat with UNKNOWN_MEMBER_ID
 * 4. respond to leave group with UNKNOWN_MEMBER_ID
 * 5. respond to offset commit with UNKNOWN_MEMBER_ID
 * 6. allow offset fetch requests
 *
 * transition:
 * 1. last offsets removed in periodic expiration task => Dead
 * 2. join group from a new member => PreparingRebalance
 * 3. group is removed by partition emigration => Dead
 * 4. group is removed by expiration => Dead
 *
 * 处于该状态的 group 已经没有 member，等待所有的分区 offset 过期
 */
private[coordinator] case object Empty extends GroupState {
    val state: Byte = 5
}

private object GroupMetadata {
    private val validPreviousStates: Map[GroupState, Set[GroupState]] =
        Map(Dead -> Set(Stable, PreparingRebalance, AwaitingSync, Empty, Dead),
            AwaitingSync -> Set(PreparingRebalance),
            Stable -> Set(AwaitingSync),
            PreparingRebalance -> Set(Stable, AwaitingSync, Empty),
            Empty -> Set(PreparingRebalance))
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
 * Group contains the following metadata:
 *
 * Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 * State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 *
 * 记录 group 的元数据信息
 */
@nonthreadsafe
private[coordinator] class GroupMetadata(val groupId: String, // group 的 ID
                                         initialState: GroupState = Empty // group 初始状态
                                        ) {

    /** 当前 group 的状态 */
    private var state: GroupState = initialState
    /** 记录消费者的元数据信息，key 是消费者 ID */
    private val members = new mutable.HashMap[String, MemberMetadata]
    private val offsets = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
    private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]

    var protocolType: Option[String] = None
    /** 当前 group 的年代信息，避免受过期请求的影响 */
    var generationId = 0
    /** group 中 leader 消费者的 ID */
    var leaderId: String = _
    /** 记录当前 group 选择的分区分配策略 PartitionAssignor */
    var protocol: String = _

    def is(groupState: GroupState): Boolean = state == groupState

    def not(groupState: GroupState): Boolean = state != groupState

    def has(memberId: String): Boolean = members.contains(memberId)

    def get(memberId: String): MemberMetadata = members(memberId)

    def add(member: MemberMetadata) {
        if (members.isEmpty)
            this.protocolType = Some(member.protocolType)

        assert(groupId == member.groupId)
        assert(this.protocolType.orNull == member.protocolType)
        assert(supportsProtocols(member.protocols))

        // 第一个加入 group 的消费者成为 leader
        if (leaderId == null)
            leaderId = member.memberId
        members.put(member.memberId, member)
    }

    def remove(memberId: String) {
        members.remove(memberId)
        // group leader 消费者被删除，则重新选择 leader
        if (memberId == leaderId) {
            leaderId = if (members.isEmpty) {
                null
            } else {
                members.keys.head
            }
        }
    }

    def currentState: GroupState = state

    def notYetRejoinedMembers: List[MemberMetadata] = members.values.filter(_.awaitingJoinCallback == null).toList

    def allMembers: collection.Set[String] = members.keySet

    def allMemberMetadata: List[MemberMetadata] = members.values.toList

    def rebalanceTimeoutMs: Int = members.values.foldLeft(0) { (timeout, member) =>
        timeout.max(member.rebalanceTimeoutMs)
    }

    // TODO: decide if ids should be predictable or random
    def generateMemberIdSuffix: String = UUID.randomUUID().toString

    /**
     * 前置状态为 Stable, AwaitingSync, Empty 中的一个
     *
     * @return
     */
    def canRebalance: Boolean = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state)

    /**
     * 切换 group 状态
     *
     * @param groupState
     */
    def transitionTo(groupState: GroupState) {
        assertValidTransition(groupState)
        state = groupState
    }

    /**
     * 为消费者 group 选择合适的分区分配策略
     *
     * @return
     */
    def selectProtocol: String = {
        if (members.isEmpty)
            throw new IllegalStateException("Cannot select protocol for empty group")

        // 计算所有消费者都支持的分区分配策略
        val candidates = candidateProtocols

        // 选择所有消费者都支持的协议作为候选协议集合，
        // 每个消费者都会通过 vote 方法进行投票（为支持的协议中的第一个协议投一票），
        // 最终选择投票最多的分区分配策略
        val votes: List[(String, Int)] = allMemberMetadata
                .map(_.vote(candidates))
                .groupBy(identity)
                .mapValues(_.size)
                .toList

        votes.maxBy(_._2)._1
    }

    private def candidateProtocols: Set[String] = {
        // get the set of protocols that are commonly supported by all members
        allMemberMetadata
                .map(_.protocols)
                .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols)
    }

    def supportsProtocols(memberProtocols: Set[String]): Boolean = {
        members.isEmpty || (memberProtocols & candidateProtocols).nonEmpty
    }

    def initNextGeneration(): Unit = {
        assert(notYetRejoinedMembers == List.empty[MemberMetadata])
        if (members.nonEmpty) {
            generationId += 1
            // 基于投票的方式选择一个所有消费者都支持的分区分配策略
            protocol = selectProtocol
            transitionTo(AwaitingSync)
        } else {
            generationId += 1
            protocol = null
            transitionTo(Empty)
        }
    }

    def currentMemberMetadata: Map[String, Array[Byte]] = {
        if (is(Dead) || is(PreparingRebalance))
            throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
        members.map { case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol)) }.toMap
    }

    def summary: GroupSummary = {
        if (is(Stable)) {
            val members = this.members.values.map { member => member.summary(protocol) }.toList
            GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members)
        } else {
            val members = this.members.values.map { member => member.summaryNoMetadata() }.toList
            GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members)
        }
    }

    def overview: GroupOverview = {
        GroupOverview(groupId, protocolType.getOrElse(""))
    }

    def initializeOffsets(offsets: collection.Map[TopicPartition, OffsetAndMetadata]) {
        this.offsets ++= offsets
    }

    def completePendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata) {
        if (pendingOffsetCommits.contains(topicPartition))
            offsets.put(topicPartition, offset)

        pendingOffsetCommits.get(topicPartition) match {
            case Some(stagedOffset) if offset == stagedOffset => pendingOffsetCommits.remove(topicPartition)
            case _ =>
        }
    }

    def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
        pendingOffsetCommits.get(topicPartition) match {
            case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
            case _ =>
        }
    }

    def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]) {
        pendingOffsetCommits ++= offsets
    }

    def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
        topicPartitions.flatMap { topicPartition =>
            pendingOffsetCommits.remove(topicPartition)
            val removedOffset = offsets.remove(topicPartition)
            removedOffset.map(topicPartition -> _)
        }.toMap
    }

    def removeExpiredOffsets(startMs: Long): Map[TopicPartition, OffsetAndMetadata] = {
        val expiredOffsets = offsets.filter {
            case (topicPartition, offset) => offset.expireTimestamp < startMs && !pendingOffsetCommits.contains(topicPartition)
        }
        offsets --= expiredOffsets.keySet
        expiredOffsets.toMap
    }

    def allOffsets: Map[TopicPartition, OffsetAndMetadata] = offsets.toMap

    def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition)

    def numOffsets: Int = offsets.size

    def hasOffsets: Boolean = offsets.nonEmpty || pendingOffsetCommits.nonEmpty

    private def assertValidTransition(targetState: GroupState) {
        if (!GroupMetadata.validPreviousStates(targetState).contains(state))
            throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
                    .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
    }

    override def toString: String = {
        "[%s,%s,%s,%s]".format(groupId, protocolType, currentState.toString, members)
    }
}

