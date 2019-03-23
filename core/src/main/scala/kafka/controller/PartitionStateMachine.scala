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

import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._
import kafka.utils.{Logging, ReplicationUtils}
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import scala.collection._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 * replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 * Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 * moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 *
 * 用于管理集群中所有分区状态的状态机：
 *
 * - NonExistentPartition -> NewPartition
 *
 * 从 ZK 上加载分区的 AR 集合到 ControllerContext 的 partitionReplicaAssignment 集合
 *
 * - NewPartition -> OnlinePartition
 *
 * 首先将 leader 副本和 ISR 集合的信息写入 ZK，将分区 AR 集合中的第一个可用的副本选举成为 leader 副本，
 * 并将分区的所有可用副本作为 ISR 集合。之后，向所有可用的副本发送 LeaderAndIsrRequest 请求，
 * 指导这些副本执行 leader/follower 角色的切换，并向所有可用的 broker 发送 UpdateMetadataRequest 请求，
 * 以更新对应的 MetadataCache
 *
 * - OnlinePartition/OfflinePartition -> OnlinePartition
 *
 * 为分区选择新的 leader 副本和 ISR 集合，将结果写入 ZK。之后，向需要进行角色切换的副本发送 leaderAndIsrRequest 请求，
 * 指导这些副本进行 leader/follower 的角色切换，并向所有可用的 broker 发送 UpdateMetadataRequest 请求，以更新对应的 MeatadataCache
 *
 * - NewPartition/OnlinePartition -> OfflinePartition
 *
 * 只进行状态转换，不执行其他操作
 *
 * - OfflinePartition -> NonExistentPartition
 *
 * 只进行状态转换，不执行其他操作
 */
class PartitionStateMachine(controller: KafkaController) extends Logging {

    /** 用于维护 KafkaController 的上下文信息 */
    private val controllerContext = controller.controllerContext
    private val controllerId = controller.config.brokerId
    private val zkUtils = controllerContext.zkUtils

    /** 记录每个分区对应的分区状态信息 */
    private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty

    /** 用于向指定的 broker 批量发送请求 */
    private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
    private val hasStarted = new AtomicBoolean(false)

    /** 默认 leader 副本选举器 */
    private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)

    /** 用于监听 topic 变化的 ZK 监听器 */
    private val topicChangeListener = new TopicChangeListener(controller)

    /** 用于监听 topic 删除的 ZK 监听器 */
    private val deleteTopicsListener = new DeleteTopicsListener(controller)

    /** 用于监听分区修改 */
    private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
    private val stateChangeLogger = KafkaController.stateChangeLogger

    this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

    /**
     * Invoked on successful controller election. First registers a topic change listener since that triggers all
     * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
     * the OnlinePartition state change for all new or offline partitions.
     */
    def startup() {
        // 初始化分区状态
        initializePartitionState()
        // 标识 PartitionStateMachine 已经启动
        hasStarted.set(true)
        // 尝试将分区切换到 OnlinePartition 状态
        triggerOnlinePartitionStateChange()

        info("Started partition state machine with initial state -> " + partitionState.toString())
    }

    // register topic and partition change listeners
    def registerListeners() {
        registerTopicChangeListener()
        registerDeleteTopicListener()
    }

    // de-register topic and partition change listeners
    def deregisterListeners() {
        deregisterTopicChangeListener()
        partitionModificationsListeners.foreach {
            case (topic, listener) =>
                zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
        }
        partitionModificationsListeners.clear()
        deregisterDeleteTopicListener()
    }

    /**
     * Invoked on controller shutdown.
     */
    def shutdown() {
        // reset started flag
        hasStarted.set(false)
        // clear partition state
        partitionState.clear()
        // de-register all ZK listeners
        deregisterListeners()

        info("Stopped partition state machine")
    }

    /**
     * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
     * state. This is called on a successful controller election and on broker changes
     */
    def triggerOnlinePartitionStateChange() {
        try {
            brokerRequestBatch.newBatch()
            // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
            // that belong to topics to be deleted
            for ((topicAndPartition, partitionState) <- partitionState
                 if !controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic)) {
                if (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
                    handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                        (new CallbackBuilder).build)
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
        } catch {
            case e: Throwable => error("Error while moving some partitions to the online state", e)
            // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
        }
    }

    def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
        partitionState.filter(p => p._2 == state).keySet
    }

    /**
     * This API is invoked by the partition change zookeeper listener
     *
     * @param partitions  The list of partitions that need to be transitioned to the target state
     * @param targetState The state that the partitions should be moved to
     */
    def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                           leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                           callbacks: Callbacks = (new CallbackBuilder).build) {
        info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
        try {
            brokerRequestBatch.newBatch()
            partitions.foreach { topicAndPartition =>
                handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
        } catch {
            case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
            // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
        }
    }

    /**
     * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state. Valid state transitions are:
     * NonExistentPartition -> NewPartition:
     * --load assigned replicas from ZK to controller cache
     *
     * NewPartition -> OnlinePartition
     * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
     * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
     *
     * OnlinePartition,OfflinePartition -> OnlinePartition
     * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
     * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
     *
     * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
     * --nothing other than marking partition state as Offline
     *
     * OfflinePartition -> NonExistentPartition
     * --nothing other than marking the partition state as NonExistentPartition
     *
     * 管理分区状态的核心方法，控制 PartitionState 的转换
     *
     * @param topic       The topic of the partition for which the state transition is invoked
     * @param partition   The partition for which the state transition is invoked
     * @param targetState The end state that the partition should be moved to
     */
    private def handleStateChange(topic: String, partition: Int,
                                  targetState: PartitionState,
                                  leaderSelector: PartitionLeaderSelector, // 执行 leader 选举的选择器
                                  callbacks: Callbacks) {
        val topicAndPartition = TopicAndPartition(topic, partition)

        // 检测当前状态机是否已经启动，只有 controller leader 的分区状态机才需要启动，如果没有启动则抛出异常
        if (!hasStarted.get)
            throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                    "the partition state machine has not started")
                    .format(controllerId, controller.epoch, topicAndPartition, targetState))

        // 获取分区状态，没有则初始化为 NonExistentPartition 状态
        val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
        try {
            // 在转换开始之前，会依据于 targetState 检查分区的前置状态是否合法
            targetState match {
                case NewPartition =>
                    // pre: partition did not exist before this
                    assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
                    partitionState.put(topicAndPartition, NewPartition)
                    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                            .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                assignedReplicas))
                // post: partition has been assigned replicas
                case OnlinePartition =>
                    assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
                    partitionState(topicAndPartition) match {
                        case NewPartition =>
                            // 为分区初始化 leader 副本和 ISR 集合
                            initializeLeaderAndIsrForPartition(topicAndPartition)
                        case OfflinePartition =>
                            // 为分区选举新的 leader 副本
                            electLeaderForPartition(topic, partition, leaderSelector)
                        case OnlinePartition => // invoked when the leader needs to be re-elected
                            // 为分区重新选举新的 leader 副本
                            electLeaderForPartition(topic, partition, leaderSelector)
                        case _ => // should never come here since illegal previous states are checked above
                    }
                    // 设置分区状态为 OnlinePartition
                    partitionState.put(topicAndPartition, OnlinePartition)
                    val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                            .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
                // post: partition has a leader
                case OfflinePartition =>
                    // pre: partition should be in New or Online state
                    assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
                    // should be called when the leader for a partition is no longer alive
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                            .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
                    // 设置分区状态为 OfflinePartition
                    partitionState.put(topicAndPartition, OfflinePartition)
                // post: partition has no alive leader
                case NonExistentPartition =>
                    // pre: partition should be in Offline state
                    assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
                    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                            .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
                    // 设置分区状态为 NonExistentPartition
                    partitionState.put(topicAndPartition, NonExistentPartition)
                // post: partition state is deleted from all brokers and zookeeper
            }
        } catch {
            case t: Throwable =>
                stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
                        .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
        }
    }

    /**
     * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
     * zookeeper
     */
    private def initializePartitionState() {
        // 遍历集群的所有分区
        for (topicPartition <- controllerContext.partitionReplicaAssignment.keys) {
            // check if leader and isr path exists for partition. If not, then it is in NEW state
            controllerContext.partitionLeadershipInfo.get(topicPartition) match {
                // 存在 leader 副本和 ISR 集合
                case Some(currentLeaderIsrAndEpoch) =>
                    // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
                    // leader 副本所在的 broker 可用，初始化为 OnlinePartition 状态
                    if (controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader))
                        partitionState.put(topicPartition, OnlinePartition)
                    // leader 副本所在的 broker 不可用，初始化为 OfflinePartition 状态
                    else
                        partitionState.put(topicPartition, OfflinePartition)
                case None =>
                    partitionState.put(topicPartition, NewPartition)
            }
        }
    }

    private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                          targetState: PartitionState) {
        if (!fromStates.contains(partitionState(topicAndPartition)))
            throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
                    .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
                    .format(partitionState(topicAndPartition)))
    }

    /**
     * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
     * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
     * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
     * OfflinePartition state.
     *
     * @param topicAndPartition The topic/partition whose leader and isr path is to be initialized
     */
    private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
        val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
        val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
        liveAssignedReplicas.size match {
            case 0 =>
                val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                        "live brokers are [%s]. No assigned replica is alive.")
                        .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
                stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                throw new StateChangeFailedException(failMsg)
            case _ =>
                debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
                // make the first replica in the list of assigned replicas, the leader
                val leader = liveAssignedReplicas.head
                val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
                    controller.epoch)
                debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
                try {
                    zkUtils.createPersistentPath(
                        getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                        zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
                    // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
                    // took over and initialized this partition. This can happen if the current controller went into a long
                    // GC pause
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
                        topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
                } catch {
                    case _: ZkNodeExistsException =>
                        // read the controller epoch
                        val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
                            topicAndPartition.partition).get
                        val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                                "exists with value %s and controller epoch %d")
                                .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
                        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                        throw new StateChangeFailedException(failMsg)
                }
        }
    }

    /**
     * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
     * It invokes the leader election API to elect a leader for the input offline partition
     *
     * @param topic          The topic of the offline partition
     * @param partition      The offline partition
     * @param leaderSelector Specific leader selector (e.g., offline/reassigned/etc.)
     */
    def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
        val topicAndPartition = TopicAndPartition(topic, partition)
        // handle leader election for the partitions whose leader is no longer alive
        stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                .format(controllerId, controller.epoch, topicAndPartition))
        try {
            var zookeeperPathUpdateSucceeded: Boolean = false
            var newLeaderAndIsr: LeaderAndIsr = null
            var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
            while (!zookeeperPathUpdateSucceeded) {
                val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
                val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
                val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
                if (controllerEpoch > controller.epoch) {
                    val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                            "already written by another controller. This probably means that the current controller %d went through " +
                            "a soft failure and another controller was elected with epoch %d.")
                            .format(topic, partition, controllerId, controllerEpoch)
                    stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                    throw new StateChangeFailedException(failMsg)
                }
                // elect new leader or throw exception
                val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
                val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
                    leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
                newLeaderAndIsr = leaderAndIsr
                newLeaderAndIsr.zkVersion = newVersion
                zookeeperPathUpdateSucceeded = updateSucceeded
                replicasForThisPartition = replicas
            }
            val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
            // update the leader cache
            controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
            stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                    .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
            val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
            // store new leader and isr info in cache
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
                newLeaderIsrAndControllerEpoch, replicas)
        } catch {
            case _: LeaderElectionNotNeededException => // swallow
            case nroe: NoReplicaOnlineException => throw nroe
            case sce: Throwable =>
                val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
                stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
                throw new StateChangeFailedException(failMsg, sce)
        }
        debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
    }

    private def registerTopicChangeListener() = {
        zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
    }

    private def deregisterTopicChangeListener() = {
        zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
    }

    def registerPartitionChangeListener(topic: String) = {
        partitionModificationsListeners.put(topic, new PartitionModificationsListener(controller, topic))
        zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    }

    def deregisterPartitionChangeListener(topic: String) = {
        zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
        partitionModificationsListeners.remove(topic)
    }

    private def registerDeleteTopicListener() = {
        zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
    }

    private def deregisterDeleteTopicListener() = {
        zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
    }

    private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
        val topicAndPartition = TopicAndPartition(topic, partition)
        ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
            case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
            case None =>
                val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
                throw new StateChangeFailedException(failMsg)
        }
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a partition
     */
    class TopicChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

        protected def logName = "TopicChangeListener"

        def doHandleChildChange(parentPath: String, children: Seq[String]) {
            inLock(controllerContext.controllerLock) {
                if (hasStarted.get) {
                    try {
                        val currentChildren = {
                            debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
                            children.toSet
                        }
                        val newTopics = currentChildren -- controllerContext.allTopics
                        val deletedTopics = controllerContext.allTopics -- currentChildren
                        controllerContext.allTopics = currentChildren

                        val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
                        controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
                            !deletedTopics.contains(p._1.topic))
                        controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
                        info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
                            deletedTopics, addedPartitionReplicaAssignment))
                        if (newTopics.nonEmpty)
                            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)
                    } catch {
                        case e: Throwable => error("Error while handling new topic", e)
                    }
                }
            }
        }
    }

    /**
     * Delete topics includes the following operations -
     * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
     * 2. If there are topics to be deleted, it signals the delete topic thread
     */
    class DeleteTopicsListener(protected val controller: KafkaController) extends ControllerZkChildListener {
        private val zkUtils = controllerContext.zkUtils

        protected def logName = "DeleteTopicsListener"

        /**
         * Invoked when a topic is being deleted
         *
         * @throws Exception On any error.
         */
        @throws[Exception]
        def doHandleChildChange(parentPath: String, children: Seq[String]) {
            inLock(controllerContext.controllerLock) {
                var topicsToBeDeleted = children.toSet
                debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
                val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
                if (nonExistentTopics.nonEmpty) {
                    warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
                    nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
                }
                topicsToBeDeleted --= nonExistentTopics
                if (controller.config.deleteTopicEnable) {
                    if (topicsToBeDeleted.nonEmpty) {
                        info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
                        // mark topic ineligible for deletion if other state changes are in progress
                        topicsToBeDeleted.foreach { topic =>
                            val preferredReplicaElectionInProgress =
                                controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
                            val partitionReassignmentInProgress =
                                controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
                            if (preferredReplicaElectionInProgress || partitionReassignmentInProgress)
                                controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
                        }
                        // add topic to deletion list
                        controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
                    }
                } else {
                    // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
                    for (topic <- topicsToBeDeleted) {
                        info("Removing " + getDeleteTopicPath(topic) + " since delete topic is disabled")
                        zkUtils.zkClient.delete(getDeleteTopicPath(topic))
                    }
                }
            }
        }

        def doHandleDataDeleted(dataPath: String) {}
    }

    class PartitionModificationsListener(protected val controller: KafkaController, topic: String) extends ControllerZkDataListener {

        protected def logName = "AddPartitionsListener"

        def doHandleDataChange(dataPath: String, data: AnyRef) {
            inLock(controllerContext.controllerLock) {
                try {
                    info(s"Partition modification triggered $data for path $dataPath")
                    val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
                    val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
                        !controllerContext.partitionReplicaAssignment.contains(p._1))
                    if (controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
                        error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                                .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
                    else {
                        if (partitionsToBeAdded.nonEmpty) {
                            info("New partitions to be added %s".format(partitionsToBeAdded))
                            controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
                            controller.onNewPartitionCreation(partitionsToBeAdded.keySet)
                        }
                    }
                } catch {
                    case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e)
                }
            }
        }

        // this is not implemented for partition change
        def doHandleDataDeleted(parentPath: String): Unit = {}
    }

}

/**
 * 分区状态
 */
sealed trait PartitionState {
    def state: Byte
}

/**
 * 新创建的分区，可能已经被分配了 AR 集合，但是还未指定 leader 副本和 ISR 集合
 */
case object NewPartition extends PartitionState {
    val state: Byte = 0
}

/**
 * 分区成功选举出 leader 之后，分区切换成 OnlinePartition 状态
 */
case object OnlinePartition extends PartitionState {
    val state: Byte = 1
}

/**
 * 已经成功选举出分区的 leader，但是 leader 副本发生宕机，则分区转换成 OfflinePartition 状态，
 * 或者新创建的分区直接转换成 OfflinePartition 状态
 */
case object OfflinePartition extends PartitionState {
    val state: Byte = 2
}

/**
 * 分区不存在，对应的分区未被创建过，或者创建后被删除了
 */
case object NonExistentPartition extends PartitionState {
    val state: Byte = 3
}
