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

import java.lang
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, ReentrantLock}

import kafka.common.TopicAndPartition
import kafka.server.ConfigType
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._
import kafka.utils.{Logging, ShutdownableThread, ZkUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractResponse, StopReplicaResponse}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 * is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 * be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 * for deletion in the following scenarios -
 *    3.1 broker hosting one of the replicas for that topic goes down
 *    3.2 partition reassignment for partitions of that topic is in progress
 *    3.3 preferred replica election for partitions of that topic is in progress
 * (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 preferred replica election for partitions of that topic completes
 *    4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 * This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 * change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 * StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 * is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 * TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 * TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 * In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 * respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 * broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 * it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 * will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 * state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 * as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 * if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 * it marks the topic for deletion retry.
 *
 * 管理待删除的 topic 和不可删除的 topic 集合，并启动一个线程 DeleteTopicsThread 执行对 topic 的删除操作，
 *
 * 当 topic 满足以下条件之一时不能删除：
 *
 * 1. topic 中的任一分区正在重新分配副本
 * 2. topic 中的任一分区正在执行优先副本选举
 * 3. topic 中的任一分区的任一副本所在的 broker 宕机
 *
 * @param controller
 * @param initialTopicsToBeDeleted           The topics that are queued up for deletion in zookeeper at the time of controller failover
 * @param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 *
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,
                           initialTopicsIneligibleForDeletion: Set[String] = Set.empty) extends Logging {

    this.logIdent = "[Topic Deletion Manager " + controller.config.brokerId + "], "

    /** Controller 上下文 */
    val controllerContext: ControllerContext = controller.controllerContext

    /** 管理分区状态的状态机 */
    val partitionStateMachine: PartitionStateMachine = controller.partitionStateMachine

    /** 管理副本状态的状态机 */
    val replicaStateMachine: ReplicaStateMachine = controller.replicaStateMachine

    val deleteLock = new ReentrantLock()

    /** 条件对象，用于同步其他线程与 deleteTopicsThread 线程 */
    val deleteTopicsCond: Condition = deleteLock.newCondition()

    /** 标识 topic 删除操作是否开始 */
    val deleteTopicStateChanged: AtomicBoolean = new AtomicBoolean(false)

    /** 执行 topic 删除操作的线程 */
    var deleteTopicsThread: DeleteTopicsThread = _

    /** 是否允许删除 topic，对应 delete.topic.enable  */
    val isDeleteTopicEnabled: lang.Boolean = controller.config.deleteTopicEnable

    /** 记录待删除的 topic 集合 */
    val topicsToBeDeleted: mutable.Set[String] = if (isDeleteTopicEnabled) {
        mutable.Set.empty[String] ++ initialTopicsToBeDeleted
    } else {
        // 如果配置不允许删除，则将对应的 topic 从 ZK 对应节点（/admin/delete_topics）下移除
        val zkUtils = controllerContext.zkUtils
        for (topic <- initialTopicsToBeDeleted) {
            val deleteTopicPath = getDeleteTopicPath(topic)
            info("Removing " + deleteTopicPath + " since delete topic is disabled")
            zkUtils.zkClient.delete(deleteTopicPath)
        }
        mutable.Set.empty[String]
    }

    /** 记录不可删除的 topic 集合 */
    val topicsIneligibleForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++ (initialTopicsIneligibleForDeletion & topicsToBeDeleted)

    /** 记录待删除的分区集合 */
    val partitionsToBeDeleted: mutable.Set[TopicAndPartition] = topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)

    /**
     * Invoked at the end of new controller initiation
     */
    def start() {
        if (isDeleteTopicEnabled) {
            deleteTopicsThread = new DeleteTopicsThread()
            // 表示 topic 删除操作开始运行
            if (topicsToBeDeleted.nonEmpty) deleteTopicStateChanged.set(true)
            // 启动后台删除线程
            deleteTopicsThread.start()
        }
    }

    /**
     * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
     */
    def shutdown() {
        // Only allow one shutdown to go through
        if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
            // Resume the topic deletion so it doesn't block on the condition
            resumeTopicDeletionThread()
            // Await delete topic thread to exit
            deleteTopicsThread.awaitShutdown()
            topicsToBeDeleted.clear()
            partitionsToBeDeleted.clear()
            topicsIneligibleForDeletion.clear()
        }
    }

    /**
     * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
     * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
     * i.e. all replicas of all partitions of that topic are deleted successfully.
     *
     * @param topics Topics that should be deleted
     */
    def enqueueTopicsForDeletion(topics: Set[String]) {
        if (isDeleteTopicEnabled) {
            // 添加待删除的 topic
            topicsToBeDeleted ++= topics
            // 添加待删除的 topic 分区
            partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic)
            // 唤醒删除线程执行删除操作
            resumeTopicDeletionThread()
        }
    }

    /**
     * Invoked when any event that can possibly resume（恢复） topic deletion occurs. These events include:
     * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
     * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
     * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished preferred replica election
     *
     * @param topics Topics for which deletion can be resumed
     */
    def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
        if (isDeleteTopicEnabled) {
            val topicsToResumeDeletion = topics & topicsToBeDeleted
            if (topicsToResumeDeletion.nonEmpty) {
                // 将不满足删除条件的 topic 集合从 topicsIneligibleForDeletion 中移除
                topicsIneligibleForDeletion --= topicsToResumeDeletion
                // 唤醒 DeleteTopicsThread 线程
                resumeTopicDeletionThread()
            }
        }
    }

    /**
     * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
     * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
     * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
     * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
     * if it has received a response for all replicas of a topic to be deleted
     *
     * @param replicas Replicas for which deletion has failed
     */
    def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
        if (isDeleteTopicEnabled) {
            val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
            if (replicasThatFailedToDelete.nonEmpty) {
                val topics = replicasThatFailedToDelete.map(_.topic)
                debug("Deletion failed for replicas %s. Halting deletion for topics %s".format(replicasThatFailedToDelete.mkString(","), topics))
                // 将删除失败的副本状态切换成 ReplicaDeletionIneligible
                controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible)
                // 标记当前 topic 标记为不可删除
                this.markTopicIneligibleForDeletion(topics)
                // 唤醒 DeleteTopicsThread 线程，尝试再次删除
                resumeTopicDeletionThread()
            }
        }
    }

    /**
     * Halt delete topic if -
     * 1. replicas being down
     * 2. partition reassignment in progress for some partitions of the topic
     * 3. preferred replica election in progress for some partitions of the topic
     *
     * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
     */
    def markTopicIneligibleForDeletion(topics: Set[String]) {
        if (isDeleteTopicEnabled) {
            val newTopicsToHaltDeletion = topicsToBeDeleted & topics
            topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
            if (newTopicsToHaltDeletion.nonEmpty)
                info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
        }
    }

    def isTopicIneligibleForDeletion(topic: String): Boolean = {
        if (isDeleteTopicEnabled) {
            topicsIneligibleForDeletion.contains(topic)
        } else {
            true
        }
    }

    def isTopicDeletionInProgress(topic: String): Boolean = {
        if (isDeleteTopicEnabled) {
            controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
        } else
            false
    }

    def isPartitionToBeDeleted(topicAndPartition: TopicAndPartition): Boolean = {
        if (isDeleteTopicEnabled) {
            partitionsToBeDeleted.contains(topicAndPartition)
        } else
            false
    }

    def isTopicQueuedUpForDeletion(topic: String): Boolean = {
        if (isDeleteTopicEnabled) {
            topicsToBeDeleted.contains(topic)
        } else
            false
    }

    /**
     * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
     * controllerLock should be acquired before invoking this API
     */
    private def awaitTopicDeletionNotification() {
        inLock(deleteLock) {
            while (deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {
                debug("Waiting for signal to start or continue topic deletion")
                deleteTopicsCond.await()
            }
        }
    }

    /**
     * Signals the delete-topic-thread to process topic deletion
     */
    private def resumeTopicDeletionThread() {
        deleteTopicStateChanged.set(true)
        inLock(deleteLock) {
            deleteTopicsCond.signal()
        }
    }

    /**
     * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
     * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
     * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
     *
     * @param replicas Replicas that were successfully deleted by the broker
     */
    private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
        val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
        debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
        controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
        resumeTopicDeletionThread()
    }

    /**
     * Topic deletion can be retried if -
     * 1. Topic deletion is not already complete
     * 2. Topic deletion is currently not in progress for that topic
     * 3. Topic is currently marked ineligible for deletion
     *
     * @param topic Topic
     * @return Whether or not deletion can be retried for the topic
     */
    private def isTopicEligibleForDeletion(topic: String): Boolean = {
        topicsToBeDeleted.contains(topic) && // 当前 topic 需要被删除，且未完成删除操作
                (!isTopicDeletionInProgress(topic) && // topic 还未开始执行删除操作
                        !isTopicIneligibleForDeletion(topic)) // topic 未标记为不可删除
    }

    /**
     * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
     * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
     *
     * 处理不可删除的 topic
     *
     * @param topic Topic for which deletion should be retried
     */
    private def markTopicForDeletionRetry(topic: String) {
        // reset replica states from ReplicaDeletionIneligible to OfflineReplica
        // 找到状态为 ReplicaDeletionIneligible 的副本，并将状态设置为 OfflineReplica
        val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
        info("Retrying delete topic for topic %s since replicas %s were not successfully deleted".format(topic, failedReplicas.mkString(",")))
        controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
    }

    /**
     * 处理成功删除的 topic
     *
     * @param topic
     */
    private def completeDeleteTopic(topic: String) {
        // 1. 注销 PartitionModificationsListener 监听器
        partitionStateMachine.deregisterPartitionChangeListener(topic)

        // 2. 将指定 topic 下被成功删除的副本状态切换成 NonExistentReplica
        val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
        replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)

        // 3. 将指定 topic 下所有的 topic 分区状态切换成 NonExistentPartition
        val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
        partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
        partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)

        // 4. 将 topic 及其分区从待删除集合中移除
        topicsToBeDeleted -= topic
        partitionsToBeDeleted.retain(_.topic != topic)

        // 5. 清除 ZK 上与该 topic 相关的数据
        val zkUtils = controllerContext.zkUtils
        zkUtils.zkClient.deleteRecursive(getTopicPath(topic))
        zkUtils.zkClient.deleteRecursive(getEntityConfigPath(ConfigType.Topic, topic))
        zkUtils.zkClient.delete(getDeleteTopicPath(topic))

        // 6. 清空 Controller 上下文中与该 topic 相关的数据
        controllerContext.removeTopic(topic)
    }

    /**
     * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
     * It invokes the delete partition callback for all partitions of a topic.
     * The updateMetadataRequest is also going to set the leader for the topics being deleted to
     * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
     * removed from their caches.
     */
    private def onTopicDeletion(topics: Set[String]) {
        info("Topic deletion callback for %s".format(topics.mkString(",")))
        val partitions = topics.flatMap(controllerContext.partitionsForTopic)
        // 向所有可用的 broker 节点发送 UpdateMetadataRequest 请求，通知当前 topic 需要被删除
        controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
        // 按照 topic 进行分组
        val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
        // 开始执行分区的删除操作
        topics.foreach { topic =>
            this.onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).keySet)
        }
    }

    /**
     * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
     * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
     * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
     * is never retried. A topic is removed from the in progress list when
     * 1. Either the topic is successfully deleted OR
     * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
     * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
     * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
     * the replicas a StopReplicaRequest (delete=true)
     * This callback does the following things -
     * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
     * for deletion if some replicas are dead since it won't complete successfully anyway
     * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
     *
     * @param replicasForTopicsToBeDeleted
     */
    private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
        replicasForTopicsToBeDeleted.groupBy(_.topic).keys.foreach { topic =>
            // 获取 topic 名下所有可用的副本集合
            val aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic == topic)
            // 获取 topic 名下中所有的不可用副本集合
            val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
            // 获取 topic 名下所有已完成删除的副本集合
            val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
            // 获取 topic 名下未完成删除的 topic 集合
            val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
            // 将不可用副本状态变更为 ReplicaDeletionIneligible
            replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
            // 将待删除的副本状态变更为 OfflineReplica，用于关闭 follower 对于 leader 的 fetch 请求
            replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
            debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
            // 将待删除的副本状态变更为 ReplicaDeletionStarted，标记当前副本准备好开始删除
            controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
                new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
            // 如果 topic 存在不可用的副本，标记该 topic 不可删除
            if (deadReplicasForTopic.nonEmpty) {
                debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
                markTopicIneligibleForDeletion(Set(topic))
            }
        }
    }

    /**
     * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
     * It does the following -
     * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
     *    deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
     * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
     * and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
     * it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
     * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
     * will delete all persistent data from all replicas of the respective partitions
     */
    private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
        info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
        // 获取待删除分区的 AR 集合
        val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
        this.startReplicaDeletion(replicasPerPartition)
    }

    private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: AbstractResponse, replicaId: Int) {
        val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
        debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
        val responseMap = stopReplicaResponse.responses.asScala
        // 获取删除失败的分区集合
        val partitionsInError =
            if (stopReplicaResponse.errorCode != Errors.NONE.code) responseMap.keySet
            else responseMap.filter { case (_, error) => error != Errors.NONE.code }.keySet
        // 获取删除失败的副本集合
        val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
        inLock(controllerContext.controllerLock) {
            // 将删除失败的副本状态切换成 ReplicaDeletionIneligible，并唤醒删除线程再次尝试删除
            this.failReplicaDeletion(replicasInError)
            // 存在某些副本被成功删除，将这些副本状态切换成 ReplicaDeletionSuccessful
            if (replicasInError.size != responseMap.size) {
                val deletedReplicas = responseMap.keySet -- partitionsInError // 已经成功删除的副本
                this.completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
            }
        }
    }

    /**
     * 执行删除 topic 任务的线程
     */
    class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {

        val zkUtils: ZkUtils = controllerContext.zkUtils

        override def doWork() {
            // 等待线程被唤醒
            awaitTopicDeletionNotification()

            if (!isRunning.get) return

            inLock(controllerContext.controllerLock) {
                // 获取待删除的 topic 集合
                val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted
                if (topicsQueuedForDeletion.nonEmpty)
                    info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))

                topicsQueuedForDeletion.foreach { topic =>
                    // 如果当前 topic 的所有副本都已经被成功删除
                    if (controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
                        // 变更 topic 及其分区和副本的状态，并从 ZK 和 Controller 上下文中移除 topic 相关的数据
                        completeDeleteTopic(topic)
                        info("Deletion of topic %s successfully completed".format(topic))
                    }
                    // 如果当前 topic 存在未完成删除的副本
                    else {
                        // 如果任一副本处于 ReplicaDeletionStarted 状态，则等待
                        if (controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
                            // 获取 ReplicaDeletionStarted 状态的分区副本对象
                            val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
                            val replicaIds = replicasInDeletionStartedState.map(_.replica)
                            val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
                            info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","), partitions.mkString(","), topic))
                        }
                        // 否则说明 topic 还未达到执行删除的条件，或者存在某个副本删除失败（对应 ReplicaDeletionIneligible 状态）
                        else {
                            // 如果任一副本处于 ReplicaDeletionIneligible 状态，则将对应状态重置为 OfflineReplica 状态后重试
                            if (controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
                                markTopicForDeletionRetry(topic)
                            }
                        }
                    }

                    // 检测当前 topic 是否可以删除
                    if (isTopicEligibleForDeletion(topic)) {
                        info("Deletion of topic %s (re)started".format(topic))
                        // 开始执行 topic 删除操作
                        onTopicDeletion(Set(topic))
                    } else if (isTopicIneligibleForDeletion(topic)) {
                        info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
                    }
                }
            }
        }
    }

}
