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

package kafka.controller

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, ReplicationUtils, ZkUtils}

import scala.collection._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 *
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 * replica can only get become follower state change request.  Valid previous state is NonExistentReplica
 *
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 * Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 *
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 * is down. Valid previous state are NewReplica, OnlineReplica
 *
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 *
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 * moved to this state. Valid previous state is ReplicaDeletionStarted
 *
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 *
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 * ReplicaDeletionSuccessful
 *
 * 用于管理集群所有副本状态的状态机：
 *
 * - NonExistentReplica -> NewReplica:
 *
 * controller 向此副本所在的 broker 发送 LeaderAndIsrRequest 请求，并向集群中所有可用的 broker 发送 UpdateMetadataRequest 请求
 *
 * - NewReplica -> OnlineReplica:
 *
 * controller 将 NewReplica 加入到 AR 集合
 *
 * - OnlineReplica/OfflineReplica -> OnlineReplica:
 *
 * controller 向副本所在的 broker 发送 LeaderAndIsrRequest 请求，并向集群中所有可用的 broker 发送 UpdateMetadataRequest 请求
 *
 * - NewReplica/OnlineReplica/OfflineReplica/ReplicaDeletionIneligible -> OfflineReplica:
 *
 * controller 向副本所在的 broker 发送 StopReplicaRequest 请求，之后会从 ISR 集合中清除副本，
 * 最后向其他可用副本所在的 broker 发送 LeaderAndIsrRequest 请求，并向集群中所有可用的 broker 发送 UpdateMetadataRequest 请求
 *
 * - OfflineReplica -> ReplicaDeletionStarted:
 *
 * controller 向副本所在的 broker 发送 StopReplicaRequest 请求
 *
 * - ReplicaDeletionStarted -> ReplicaDeletionSuccessful:
 *
 * 只做状态切换，不做其他操作
 *
 * - ReplicaDeletionStarted -> ReplicaDeletionIneligible:
 *
 * 只做状态切换，不做其他操作
 *
 * - ReplicaDeletionSuccessful -> NonExistentReplica
 *
 * controller 从 AR 集合中删除副本
 *
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {

    /** Controller 上下文信息 */
    private val controllerContext = controller.controllerContext

    /** Controller 的 ID */
    private val controllerId = controller.config.brokerId

    /** ZK 工具类 */
    private val zkUtils = controllerContext.zkUtils

    /** 记录每个副本对应的状态 */
    private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty

    /** ZK 监听器，用于监听 broker 节点的上下线 */
    private val brokerChangeListener = new BrokerChangeListener(controller)

    /** 用于向 broker 节点批量发送请求 */
    private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)

    /** 标识当前副本状态机是否成功启动 */
    private val hasStarted = new AtomicBoolean(false)

    private val stateChangeLogger = KafkaController.stateChangeLogger

    this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "

    /**
     * Invoked on successful controller election. First registers a broker change listener since that triggers all
     * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
     * Then triggers the OnlineReplica state change for all replicas.
     */
    def startup() {
        // 初始化每个 topic 分区 AR 集合中的副本状态
        this.initializeReplicaState()
        // 标识当前副本状态机启动成功
        hasStarted.set(true)
        // 尝试将所有可用副本切换成 OnlineReplica 状态
        this.handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

        info("Started replica state machine with initial state -> " + replicaState.toString())
    }

    // register ZK listeners of the replica state machine
    def registerListeners() {
        // register broker change listener
        this.registerBrokerChangeListener()
    }

    // de-register ZK listeners of the replica state machine
    def deregisterListeners() {
        // de-register broker change listener
        deregisterBrokerChangeListener()
    }

    /**
     * Invoked on controller shutdown.
     */
    def shutdown() {
        // reset started flag
        hasStarted.set(false)
        // reset replica state
        replicaState.clear()
        // de-register all ZK listeners
        deregisterListeners()

        info("Stopped replica state machine")
    }

    /**
     * This API is invoked by the broker change controller callbacks and the startup API of the state machine
     *
     * @param replicas    The list of replicas (brokers) that need to be transitioned to the target state
     * @param targetState The state that the replicas should be moved to
     *                    The controller's allLeaders cache should have been updated before this
     */
    def handleStateChanges(replicas: Set[PartitionAndReplica],
                           targetState: ReplicaState,
                           callbacks: Callbacks = (new CallbackBuilder).build) {
        if (replicas.nonEmpty) {
            info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
            try {
                // 校验待发送的请求集合，确保历史的请求已经全部发送完毕
                brokerRequestBatch.newBatch()
                // 遍历切换每个副本的状态
                replicas.foreach(r => this.handleStateChange(r, targetState, callbacks))
                // 发送请求
                brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
            } catch {
                case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
            }
        }
    }

    /**
     * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state. Valid state transitions are:
     * NonExistentReplica --> NewReplica
     * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
     * partition to every live broker
     *
     * NewReplica -> OnlineReplica
     * --add the new replica to the assigned replica list if needed
     *
     * OnlineReplica,OfflineReplica -> OnlineReplica
     * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
     * partition to every live broker
     *
     * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
     * --send StopReplicaRequest to the replica (w/o deletion)
     * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
     * UpdateMetadata request for the partition to every live broker.
     *
     * OfflineReplica -> ReplicaDeletionStarted
     * --send StopReplicaRequest to the replica (with deletion)
     *
     * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
     * -- mark the state of the replica in the state machine
     *
     * ReplicaDeletionStarted -> ReplicaDeletionIneligible
     * -- mark the state of the replica in the state machine
     *
     * ReplicaDeletionSuccessful -> NonExistentReplica
     * -- remove the replica from the in memory partition replica assignment cache
     *
     * @param partitionAndReplica The replica for which the state transition is invoked
     * @param targetState         The end state that the replica should be moved to
     */
    def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState, callbacks: Callbacks) {
        val topic = partitionAndReplica.topic
        val partition = partitionAndReplica.partition
        val replicaId = partitionAndReplica.replica
        val topicAndPartition = TopicAndPartition(topic, partition)
        // 检测副本状态机是否已经启动
        if (!hasStarted.get)
            throw new StateChangeFailedException(
                "Controller %d epoch %d initiated state change of replica %d for partition %s to %s failed because replica state machine has not started"
                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))

        // 获取指定副本当前的状态，如果不存在则初始化为 NonExistentReplica 状态
        val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
        try {
            // 获取分区的 AR 集合
            val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
            // 在转换开始之前，会依据于目标状态检查副本的前置状态是否合法
            targetState match {
                case NewReplica =>
                    // 如果目标状态为 NewReplica，则前置状态必须是 NonExistentReplica
                    this.assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
                    // 从 ZK 获取分区的 leader 副本和 ISR 集合等信息
                    val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
                    leaderIsrAndControllerEpochOpt match {
                        case Some(leaderIsrAndControllerEpoch) =>
                            // NewReplica 状态的副本不能是 leader
                            if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                                        .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
                            // 向该副本所在 broker 节点发送 LeaderAndIsrRequest 请求，并发送 UpdateMetadataRequest 给所有可用的 broker 节点
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(
                                List(replicaId), topic, partition, leaderIsrAndControllerEpoch, replicaAssignment)
                        case None => // new leader request will be sent to this replica when one gets elected
                    }
                    // 更新当前副本状态为 NewReplica
                    replicaState.put(partitionAndReplica, NewReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case ReplicaDeletionStarted =>
                    // 如果目标状态为 ReplicaDeletionStarted，则前置状态必须是 OfflineReplica
                    this.assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
                    // 更新当前副本状态为 ReplicaDeletionStarted
                    replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
                    // 向该副本所在 broker 节点发送 StopReplicaRequest 请求
                    brokerRequestBatch.addStopReplicaRequestForBrokers(
                        List(replicaId), topic, partition, deletePartition = true, callbacks.stopReplicaResponseCallback)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case ReplicaDeletionIneligible =>
                    // 如果目标状态为 ReplicaDeletionIneligible，则前置状态必须是 ReplicaDeletionStarted
                    this.assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
                    // 更新当前副本状态为 ReplicaDeletionIneligible
                    replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case ReplicaDeletionSuccessful =>
                    // 如果目标状态为 ReplicaDeletionSuccessful，则前置状态必须是 ReplicaDeletionStarted
                    this.assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
                    // 更新当前副本状态为 ReplicaDeletionSuccessful
                    replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case NonExistentReplica =>
                    // 如果目标状态为 NonExistentReplica，则前置状态必须是 ReplicaDeletionSuccessful
                    this.assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
                    // 从对应 topic 分区的 AR 集合中移除当前副本
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
                    // 移除副本缓存在本地的状态
                    replicaState.remove(partitionAndReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case OnlineReplica =>
                    // 如果目标状态为 OnlineReplica，则前置状态必须是 NewReplica、OnlineReplica、OfflineReplica 和 ReplicaDeletionIneligible 中的一个
                    this.assertValidPreviousStates(partitionAndReplica,
                        List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
                    replicaState(partitionAndReplica) match {
                        case NewReplica =>
                            // 如果前置状态是 NewReplica，则尝试将当前副本添加到对应分区的 AR 集合中
                            val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                            if (!currentAssignedReplicas.contains(replicaId))
                                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
                            stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                        case _ =>
                            // 检测是否存在 leader 副本
                            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                                // 如果存在 leader 副本，则向该副本所在 broker 节点发送 LeaderAndIsrRequest 请求，并发送 UpdateMetadataRequest 给所有可用的 broker 节点
                                case Some(leaderIsrAndControllerEpoch) =>
                                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(
                                        List(replicaId), topic, partition, leaderIsrAndControllerEpoch, replicaAssignment)
                                    replicaState.put(partitionAndReplica, OnlineReplica)
                                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                                // 不存在 leader 副本
                                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                                // started a log for that partition and does not have a high watermark value for this partition
                            }
                    }
                    // 更新当前副本状态为 OnlineReplica
                    replicaState.put(partitionAndReplica, OnlineReplica)
                case OfflineReplica =>
                    // 如果目标状态为 OfflineReplica，则前置状态必须是 NewReplica、OnlineReplica、OfflineReplica 和 ReplicaDeletionIneligible 中的一个
                    this.assertValidPreviousStates(partitionAndReplica,
                        List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
                    // 向副本所在 broker 节点发送 StopReplicaRequest 请求
                    brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
                    // As an optimization, the controller removes dead replicas from the ISR
                    val leaderAndIsrIsEmpty: Boolean =
                        controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                            case Some(_) =>
                                // 将当前副本从所在分区的 ISR 集合中移除
                                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                                    case Some(updatedLeaderIsrAndControllerEpoch) =>
                                        // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                                        val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                                        if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                                            // 向除当前副本以外可用的 AR 集合副本发送 LeaderAndIsrRequest 请求，并向集群中所有可用的 broker 发送 UpdateMetadataRequest 请求
                                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                                                topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                                        }
                                        // 更新当前副本状态为 OfflineReplica
                                        replicaState.put(partitionAndReplica, OfflineReplica)
                                        stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s".format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                                        false
                                    case None =>
                                        true
                                }
                            case None =>
                                true
                        }
                    if (leaderAndIsrIsEmpty && !controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition))
                        throw new StateChangeFailedException(
                            "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty".format(replicaId, topicAndPartition))
            }
        }
        catch {
            case t: Throwable =>
                stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                        .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
        }
    }

    /**
     * 检测指定 topic 的所有副本是否都已经被成功删除
     *
     * @param topic
     * @return
     */
    def areAllReplicasForTopicDeleted(topic: String): Boolean = {
        // 获取指定 topic 所有的副本对象
        val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
        // 获取副本对应的状态
        val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
        debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
        // 是否所有的副本都已经被成功删除
        replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
    }

    /**
     * 至少存在一个副本准备好被删除
     *
     * @param topic
     * @return
     */
    def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
        val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
        val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
        replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
    }

    def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
        replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
    }

    def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
        replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
    }

    def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
        val deletionStates = Set[ReplicaState](ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
        replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
    }

    private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                          targetState: ReplicaState) {
        assert(fromStates.contains(replicaState(partitionAndReplica)),
            "Replica %s should be in the %s states before moving to %s state"
                    .format(partitionAndReplica, fromStates.mkString(","), targetState) +
                    ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
    }

    private def registerBrokerChangeListener(): util.List[String] = {
        zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
    }

    private def deregisterBrokerChangeListener(): Unit = {
        zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
    }

    /**
     * Invoked on startup of the replica's state machine to
     * set the initial state for replicas of all existing partitions in zookeeper
     */
    private def initializeReplicaState() {
        for ((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
            val topic = topicPartition.topic
            val partition = topicPartition.partition
            // 遍历每个分区的 AR 集合，初始化对应分区的状态
            assignedReplicas.foreach { replicaId =>
                val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
                // 将可用的副本初始化为 OnlineReplica 状态
                if (controllerContext.liveBrokerIds.contains(replicaId)) replicaState.put(partitionAndReplica, OnlineReplica)
                // 将不可用的副本初始化为 ReplicaDeletionIneligible 状态
                else replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
            }
        }
    }

    def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int): Seq[TopicAndPartition] = {
        controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a replica
     *
     * 监听 ZK 的 /broker/ids 路径，主要负责处理 broker 节点的上线和下线，
     * broker 上线时会在 ZK 路径下创建对应的临时节点，下线时会删除对应节点
     *
     */
    class BrokerChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

        protected def logName = "BrokerChangeListener"

        def doHandleChildChange(parentPath: String, currentBrokerList: Seq[String]) {
            info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
            inLock(controllerContext.controllerLock) {
                if (hasStarted.get) {
                    ControllerStats.leaderElectionTimer.time {
                        try {
                            // 从 ZK 获取 broker 节点列表
                            val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
                            val curBrokerIds = curBrokers.map(_.id)
                            val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
                            // 筛选新增的 broker 节点列表
                            val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
                            // 筛选故障的 broker 节点列表
                            val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
                            val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
                            // 更新 Controller 上下文信息
                            controllerContext.liveBrokers = curBrokers
                            val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
                            val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
                            val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
                            info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                                    .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
                            // 创建 controller 到新增的 broker 节点之间的网络连接，并启动请求发送线程
                            newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
                            // 关闭 controller 到故障的 broker 节点之间的网络连接
                            deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
                            // 如果存在新增的 broker 节点，通知集群中的其它 broker 节点，并上线新增的分区副本等
                            if (newBrokerIds.nonEmpty) controller.onBrokerStartup(newBrokerIdsSorted)
                            // 如果存在故障的 broker 节点，则下线故障 broker 节点上的分区和副本，并通知到集群中的其它 broker 节点
                            if (deadBrokerIds.nonEmpty) controller.onBrokerFailure(deadBrokerIdsSorted)
                        } catch {
                            case e: Throwable => error("Error while handling broker changes", e)
                        }
                    }
                }
            }
        }
    }

}

/**
 * 副本状态
 */
sealed trait ReplicaState {
    def state: Byte
}

/**
 * 创建新的 topic 或进行副本重新分配时，新创建的副本对应的状态，处于此状态的副本只能成为 follower 副本
 */
case object NewReplica extends ReplicaState {
    val state: Byte = 1
}

/**
 * 副本开始正常工作时的状态，处于此状态的副本可以成为 leader 副本，也可以成为 follower 副本
 */
case object OnlineReplica extends ReplicaState {
    val state: Byte = 2
}

/**
 * 副本所有的 broker 下线后，对应该状态
 */
case object OfflineReplica extends ReplicaState {
    val state: Byte = 3
}

/**
 * 刚开始删除副本时，会先将副本转换为此状态，然后开始删除操作
 */
case object ReplicaDeletionStarted extends ReplicaState {
    val state: Byte = 4
}

/**
 * 副本删除成功之后，处于此状态
 */
case object ReplicaDeletionSuccessful extends ReplicaState {
    val state: Byte = 5
}

/**
 * 如果副本删除操作失败，处于此状态
 */
case object ReplicaDeletionIneligible extends ReplicaState {
    val state: Byte = 6
}

/**
 * 副本被删除成功之后，最终转换为此状态
 */
case object NonExistentReplica extends ReplicaState {
    val state: Byte = 7
}
