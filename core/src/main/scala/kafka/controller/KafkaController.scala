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
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.{Gauge, Meter}
import kafka.admin.{AdminUtils, PreferredReplicaLeaderElectionCommand}
import kafka.api._
import kafka.cluster.Broker
import kafka.common.{TopicAndPartition, _}
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._

/**
 * 维护 KafkaController 的上下文信息，缓存了 ZK 中记录的整个集群的元数据信息
 *
 * @param zkUtils
 */
class ControllerContext(val zkUtils: ZkUtils) {

    /** 管理 controller 与集群中 broker 之间的连接 */
    var controllerChannelManager: ControllerChannelManager = _
    val controllerLock: ReentrantLock = new ReentrantLock()

    /** 正在关闭的 broker 集合 */
    var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
    val brokerShutdownLock: Object = new Object

    /** controller 的年代信息，初始为 0，每次重新选举之后值加 1 */
    var epoch: Int = KafkaController.InitialControllerEpoch - 1

    /** 年代信息对应的 ZK 版本，初始为 0  */
    var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1

    /** 集群中全部的 topic 集合  */
    var allTopics: Set[String] = Set.empty

    /** 记录每个分区对应的 AR 集合 */
    var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty

    /** 记录每个分区的 leader 副本所在的 brokerId、ISR 集合，以及 controller 年代信息 */
    var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty

    /** 记录正在重新分配副本的分区 */
    val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap

    /** 记录了正在进行优先副本选举的分区 */
    val partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] = new mutable.HashSet

    /** 记录了当前可用的 broker 集合 */
    private var liveBrokersUnderlying: Set[Broker] = Set.empty

    /** 记录了当前可用的 brokerId 集合 */
    private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

    /**
     * 依据 brokers 更新 liveBrokersUnderlying 和 liveBrokerIdsUnderlying
     *
     * @param brokers
     */
    def liveBrokers_=(brokers: Set[Broker]) {
        liveBrokersUnderlying = brokers
        liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
    }

    // getter
    def liveBrokers: Set[Broker] = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))

    def liveBrokerIds: Set[Int] = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

    def liveOrShuttingDownBrokerIds: Set[Int] = liveBrokerIdsUnderlying

    def liveOrShuttingDownBrokers: Set[Broker] = liveBrokersUnderlying

    /**
     * 获取在指定 broker 有存在副本的分区集合
     *
     * @param brokerId
     * @return
     */
    def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
        partitionReplicaAssignment.collect {
            case (topicAndPartition, replicas) if replicas.contains(brokerId) => topicAndPartition
        }.toSet
    }

    /**
     * 获取指定 broker 集合中保存的所有副本
     *
     * @param brokerIds
     * @return
     */
    def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
        brokerIds.flatMap { brokerId =>
            partitionReplicaAssignment.collect {
                case (topicAndPartition, replicas) if replicas.contains(brokerId) =>
                    PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId)
            }
        }.toSet
    }

    /**
     * 获取指定 topic 的所有副本
     *
     * @param topic
     * @return
     */
    def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
        partitionReplicaAssignment
                .filter { case (topicAndPartition, _) => topicAndPartition.topic == topic }
                .flatMap { case (topicAndPartition, replicas) =>
                    replicas.map { r =>
                        PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
                    }
                }.toSet
    }

    /**
     * 获取指定 topic 的所有分区
     *
     * @param topic
     * @return
     */
    def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] =
        partitionReplicaAssignment.keySet.filter(topicAndPartition => topicAndPartition.topic == topic)

    /**
     * 获取所有可用的 broker 中保存的副本
     *
     * @return
     */
    def allLiveReplicas(): Set[PartitionAndReplica] = {
        replicasOnBrokers(liveBrokerIds)
    }

    /**
     * 获取指定分区集合的副本
     *
     * @param partitions
     * @return
     */
    def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
        partitions.flatMap { p =>
            val replicas = partitionReplicaAssignment(p)
            replicas.map(r => PartitionAndReplica(p.topic, p.partition, r))
        }
    }

    /**
     * 删除指定的 topic
     *
     * @param topic
     */
    def removeTopic(topic: String): Unit = {
        partitionLeadershipInfo = partitionLeadershipInfo.filter { case (topicAndPartition, _) => topicAndPartition.topic != topic }
        partitionReplicaAssignment = partitionReplicaAssignment.filter { case (topicAndPartition, _) => topicAndPartition.topic != topic }
        allTopics -= topic
    }

}

object KafkaController extends Logging {
    val stateChangeLogger = StateChangeLogger("state.change.logger")
    val InitialControllerEpoch = 1
    val InitialControllerEpochZkVersion = 1

    case class StateChangeLogger(override val loggerName: String) extends Logging

    def parseControllerId(controllerInfoString: String): Int = {
        try {
            Json.parseFull(controllerInfoString) match {
                case Some(m) =>
                    val controllerInfo = m.asInstanceOf[Map[String, Any]]
                    controllerInfo("brokerid").asInstanceOf[Int]
                case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
            }
        } catch {
            case _: Throwable =>
                // It may be due to an incompatible controller register version
                warn("Failed to parse the controller info as json. "
                        + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
                try {
                    controllerInfoString.toInt
                } catch {
                    case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
                }
        }
    }
}

/**
 * Kafka 集群存在多个 broker 节点，在这些 broker 中会选举一个成为 controller leader，
 * 负责管理集群中所有的分区和副本的状态，也是 kafka 与 ZK 进行交互的桥梁。
 *
 * 每个 broker 都会创建一个 KafkaController 对象。
 *
 * @param config
 * @param zkUtils
 * @param brokerState
 * @param time
 * @param metrics
 * @param threadNamePrefix
 */
class KafkaController(val config: KafkaConfig,
                      zkUtils: ZkUtils,
                      val brokerState: BrokerState,
                      time: Time,
                      metrics: Metrics,
                      threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

    this.logIdent = "[Controller " + config.brokerId + "]: "
    private var isRunning = true
    private val stateChangeLogger = KafkaController.stateChangeLogger
    val controllerContext = new ControllerContext(zkUtils)
    val partitionStateMachine = new PartitionStateMachine(this)
    val replicaStateMachine = new ReplicaStateMachine(this)
    private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
        onControllerResignation, config.brokerId, time)
    // have a separate scheduler for the controller to be able to start and stop independently of the
    // kafka server
    private val autoRebalanceScheduler = new KafkaScheduler(1)
    var deleteTopicManager: TopicDeletionManager = _
    val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
    private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
    private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
    private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
    private val brokerRequestBatch = new ControllerBrokerRequestBatch(this)

    private val partitionReassignedListener = new PartitionsReassignedListener(this)
    private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this)
    private val isrChangeNotificationListener = new IsrChangeNotificationListener(this)

    newGauge(
        "ActiveControllerCount",
        new Gauge[Int] {
            def value(): Int = if (isActive) 1 else 0
        }
    )

    newGauge(
        "OfflinePartitionsCount",
        new Gauge[Int] {
            def value(): Int = {
                inLock(controllerContext.controllerLock) {
                    if (!isActive)
                        0
                    else
                        controllerContext.partitionLeadershipInfo.count(p =>
                            (!controllerContext.liveOrShuttingDownBrokerIds.contains(p._2.leaderAndIsr.leader))
                                    && (!deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic))
                        )
                }
            }
        }
    )

    newGauge(
        "PreferredReplicaImbalanceCount",
        new Gauge[Int] {
            def value(): Int = {
                inLock(controllerContext.controllerLock) {
                    if (!isActive)
                        0
                    else
                        controllerContext.partitionReplicaAssignment.count {
                            case (topicPartition, replicas) =>
                                (controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != replicas.head
                                        && (!deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic))
                                        )
                        }
                }
            }
        }
    )

    def epoch: Int = controllerContext.epoch

    def clientId: String = {
        val controllerListener = config.listeners.find(_.listenerName == config.interBrokerListenerName).getOrElse(
            throw new IllegalArgumentException(s"No listener with name ${config.interBrokerListenerName} is configured."))
        "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.host, controllerListener.port)
    }

    /**
     * On clean shutdown, the controller first determines the partitions that the
     * shutting down broker leads, and moves leadership of those partitions to another broker
     * that is in that partition's ISR.
     *
     * @param id Id of the broker to shutdown.
     * @return The number of partitions that the broker still leads.
     */
    def shutdownBroker(id: Int): Set[TopicAndPartition] = {

        if (!isActive) {
            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
        }

        controllerContext.brokerShutdownLock synchronized {
            info("Shutting down broker " + id)

            inLock(controllerContext.controllerLock) {
                if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
                    throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

                controllerContext.shuttingDownBrokerIds.add(id)
                debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
                debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
            }

            val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
                inLock(controllerContext.controllerLock) {
                    controllerContext.partitionsOnBroker(id)
                            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))
                }

            allPartitionsAndReplicationFactorOnBroker.foreach {
                case (topicAndPartition, replicationFactor) =>
                    // Move leadership serially to relinquish lock.
                    inLock(controllerContext.controllerLock) {
                        controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
                            if (replicationFactor > 1) {
                                if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
                                    // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                                    // notifies all affected brokers
                                    partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                                        controlledShutdownPartitionLeaderSelector)
                                } else {
                                    // Stop the replica first. The state change below initiates ZK changes which should take some time
                                    // before which the stop replica request should be completed (in most cases)
                                    try {
                                        brokerRequestBatch.newBatch()
                                        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                                            topicAndPartition.partition, deletePartition = false)
                                        brokerRequestBatch.sendRequestsToBrokers(epoch)
                                    } catch {
                                        case e: IllegalStateException => {
                                            // Resign if the controller is in an illegal state
                                            error("Forcing the controller to resign")
                                            brokerRequestBatch.clear()
                                            controllerElector.resign()

                                            throw e
                                        }
                                    }
                                    // If the broker is a follower, updates the isr in ZK and notifies the current leader
                                    replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                                        topicAndPartition.partition, id)), OfflineReplica)
                                }
                            }
                        }
                    }
            }

            def replicatedPartitionsBrokerLeads(): Iterable[TopicAndPartition] = inLock(controllerContext.controllerLock) {
                trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
                controllerContext.partitionLeadershipInfo.filter {
                    case (topicAndPartition, leaderIsrAndControllerEpoch) =>
                        leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
                }.keys
            }

            replicatedPartitionsBrokerLeads().toSet
        }
    }

    /**
     * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
     * It does the following things on the become-controller state change -
     * 1. Register controller epoch changed listener
     * 2. Increments the controller epoch
     * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
     * leaders for all existing partitions.
     * 4. Starts the controller's channel manager
     * 5. Starts the replica state machine
     * 6. Starts the partition state machine
     * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
     * This ensures another controller election will be triggered and there will always be an actively serving controller
     */
    def onControllerFailover() {
        if (isRunning) {
            info("Broker %d starting become controller state transition".format(config.brokerId))
            readControllerEpochFromZookeeper()
            incrementControllerEpoch(zkUtils.zkClient)

            // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
            registerReassignedPartitionsListener()
            registerIsrChangeNotificationListener()
            registerPreferredReplicaElectionListener()
            partitionStateMachine.registerListeners()
            replicaStateMachine.registerListeners()

            initializeControllerContext()

            // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
            // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
            // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
            // partitionStateMachine.startup().
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

            replicaStateMachine.startup()
            partitionStateMachine.startup()

            // register the partition change listeners for all existing topics on failover
            controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
            info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
            maybeTriggerPartitionReassignment()
            maybeTriggerPreferredReplicaElection()
            if (config.autoLeaderRebalanceEnable) {
                info("starting the partition rebalance scheduler")
                autoRebalanceScheduler.startup()
                autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
                    5, config.leaderImbalanceCheckIntervalSeconds.toLong, TimeUnit.SECONDS)
            }
            deleteTopicManager.start()
        }
        else
            info("Controller has been shut down, aborting startup/failover")
    }

    /**
     * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
     * required to clean up internal controller data structures
     * Note:We need to resign as a controller out of the controller lock to avoid potential deadlock issue
     */
    def onControllerResignation() {
        debug("Controller resigning, broker id %d".format(config.brokerId))
        // de-register listeners
        deregisterIsrChangeNotificationListener()
        deregisterReassignedPartitionsListener()
        deregisterPreferredReplicaElectionListener()

        // shutdown delete topic manager
        if (deleteTopicManager != null)
            deleteTopicManager.shutdown()

        // shutdown leader rebalance scheduler
        if (config.autoLeaderRebalanceEnable)
            autoRebalanceScheduler.shutdown()

        inLock(controllerContext.controllerLock) {
            // de-register partition ISR listener for on-going partition reassignment task
            deregisterReassignedPartitionsIsrChangeListeners()
            // shutdown partition state machine
            partitionStateMachine.shutdown()
            // shutdown replica state machine
            replicaStateMachine.shutdown()
            // shutdown controller channel manager
            if (controllerContext.controllerChannelManager != null) {
                controllerContext.controllerChannelManager.shutdown()
                controllerContext.controllerChannelManager = null
            }
            // reset controller context
            controllerContext.epoch = 0
            controllerContext.epochZkVersion = 0
            brokerState.newState(RunningAsBroker)

            info("Broker %d resigned as the controller".format(config.brokerId))
        }
    }

    /**
     * Returns true if this broker is the current controller.
     */
    def isActive: Boolean = {
        inLock(controllerContext.controllerLock) {
            controllerContext.controllerChannelManager != null
        }
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
     * brokers as input. It does the following -
     * 1. Sends update metadata request to all live and shutting down brokers
     * 2. Triggers the OnlinePartition state change for all new/offline partitions
     * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
     * so, it performs the reassignment logic for each topic/partition.
     *
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
     * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
     * partitions currently new or offline (rather than every partition this controller is aware of)
     * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
     * every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
     */
    def onBrokerStartup(newBrokers: Seq[Int]) {
        info("New broker startup callback for %s".format(newBrokers.mkString(",")))
        val newBrokersSet = newBrokers.toSet
        // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
        // broker via this update.
        // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
        // common controlled shutdown case, the metadata will reach the new brokers faster
        // 1. 向集群中所有的 broker 节点发送 UpdateMetadataRequest 请求，发送的是所有的分区信息，以此来了解新添加的 broker 信息
        sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

        // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
        // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
        // 2. 将新增的 broker 上的副本状态设置为 OnlineReplica，期间会发送 LeaderAndIsrRequest 和 UpdateMetaRequest 请求
        val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
        replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)

        // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
        // to see if these brokers can become leaders for some/all of those
        // 3. 将状态为 OfflinePartition 和 NewPartition 的分区设置为 OnlinePartition
        partitionStateMachine.triggerOnlinePartitionStateChange()

        // check if reassignment of some partitions need to be restarted
        // 4. 检查进行副本重新分配
        val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
            case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains)
        }
        partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))

        // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
        // on the newly restarted brokers, there is a chance that topic deletion can resume
        // 如果新增 broker 上有待删除的 topic 副本，则唤醒 DeleteTopicsThread 线程进行删除
        val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
        if (replicasForTopicsToBeDeleted.nonEmpty) {
            info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
                    "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
                deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
            deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
        }
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Mark partitions with dead leaders as offline
     * 2. Triggers the OnlinePartition state change for all new/offline partitions
     * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
     * 4. If no partitions are effected then send UpdateMetadataRequest to live or shutting down brokers
     *
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
     * the partition state machine will refresh our cache for us when performing leader election for all new/offline
     * partitions coming online.
     */
    def onBrokerFailure(deadBrokers: Seq[Int]) {
        info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
        // 移除正在关闭的 broker
        val deadBrokersThatWereShuttingDown =
            deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
        info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
        val deadBrokersSet = deadBrokers.toSet
        // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
        // 1. 过滤得到 leader 副本在故障 broker 上的分区，设置其状态为 OfflinePartition
        val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
            deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
                    !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
        partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)

        // trigger OnlinePartition state changes for offline or new partitions
        // 2. 将 OfflinePartition 状态的分区切换成 OnlinePartition 状态
        partitionStateMachine.triggerOnlinePartitionStateChange()

        // filter out the replicas that belong to topics that are being deleted
        // 3. 过滤得到在故障 broker 上的副本，将这些副本设置为 OfflineReplica 状态
        val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
        val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
        // handle dead replicas
        replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)

        // check if topic deletion state for the dead replicas needs to be updated
        // 4. 检查故障 broker 上是否有待删除的 topic 副本，如果存在则将其转换成 ReplicaDeletionIneligible 状态，并标记 topic 不可删除
        val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
        if (replicasForTopicsToBeDeleted.nonEmpty) {
            // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
            // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
            // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
            deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
        }

        // If broker failure did not require leader re-election, inform brokers of failed broker
        // Note that during leader re-election, brokers update their metadata
        // 5. 发送 UpdateMetadataRequest 请求，更新所有的 broker 信息
        if (partitionsWithoutLeader.isEmpty) {
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
        }
    }

    /**
     * This callback is invoked by the partition state machine's topic change listener with the list of new topics
     * and partitions as input. It does the following -
     * 1. Registers partition change listener. This is not required until KAFKA-347
     * 2. Invokes the new partition callback
     * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
     */
    def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
        info("New topic creation callback for %s".format(newPartitions.mkString(",")))
        // 为每个新增的 topic 注册一个 PartitionModificationsListener
        topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
        onNewPartitionCreation(newPartitions)
    }

    /**
     * This callback is invoked by the topic change callback with the list of failed brokers as input.
     * It does the following -
     * 1. Move the newly created partitions to the NewPartition state
     * 2. Move the newly created partitions from NewPartition->OnlinePartition state
     */
    def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
        info("New partition creation callback for %s".format(newPartitions.mkString(",")))
        // 将所有新增的分区状态转换为 NewPartition
        partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
        // 将指定分区的所有副本都转换为 NewReplica
        replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
        // 将所有新增的分区状态转换为 OnlinePartition
        partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
        // 将指定分区的所有副本状态转换为 OnlineReplica
        replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
    }

    /**
     * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
     * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
     * Reassigning replicas for a partition goes through a few steps listed in the code.
     * RAR = Reassigned replicas
     * OAR = Original list of replicas for partition
     * AR = current assigned replicas
     *
     * 1. Update AR in ZK with OAR + RAR.
     * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
     * of the leader epoch in zookeeper.
     * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
     * 4. Wait until all replicas in RAR are in sync with the leader.
     * 5  Move all replicas in RAR to OnlineReplica state.
     * 6. Set AR to RAR in memory.
     * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
     * will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
     * In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
     * RAR - OAR back in the isr.
     * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
     * isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
     * After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
     * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
     * the replicas in OAR - RAR to physically delete the replicas on disk.
     * 10. Update AR in ZK with RAR.
     * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
     * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
     *
     * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
     * may go through the following transition.
     * AR                 leader/isr
     * {1,2,3}            1/{1,2,3}           (initial state)
     * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
     * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
     * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
     * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
     * {4,5,6}            4/{4,5,6}           (step 10)
     *
     * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
     * This way, if the controller crashes before that step, we can still recover.
     */
    def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
            info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
                    "reassigned not yet caught up with the leader")
            val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
            val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
            //1. Update AR in ZK with OAR + RAR.
            updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
            //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
            updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
                newAndOldReplicas.toSeq)
            //3. replicas in RAR - OAR -> NewReplica
            startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
            info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
                    "reassigned to catch up with the leader")
        } else {
            //4. Wait until all replicas in RAR are in sync with the leader.
            val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
            //5. replicas in RAR -> OnlineReplica
            reassignedReplicas.foreach { replica =>
                replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
                    replica)), OnlineReplica)
            }
            //6. Set AR to RAR in memory.
            //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
            //   a new AR (using RAR) and same isr to every broker in RAR
            moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
            //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
            //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
            stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
            //10. Update AR in ZK with RAR.
            updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
            //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
            removePartitionFromReassignedPartitions(topicAndPartition)
            info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
            //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
            // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
            deleteTopicManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
        }
    }

    private def watchIsrChangesForReassignedPartition(topic: String,
                                                      partition: Int,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
            reassignedReplicas.toSet)
        reassignedPartitionContext.isrChangeListener = isrChangeListener
        // register listener on the leader and isr path to wait until they catch up with the current leader
        zkUtils.zkClient.subscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
    }

    def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                                  reassignedPartitionContext: ReassignedPartitionsContext) {
        val newReplicas = reassignedPartitionContext.newReplicas
        val topic = topicAndPartition.topic
        val partition = topicAndPartition.partition
        try {
            val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
            assignedReplicasOpt match {
                case Some(assignedReplicas) =>
                    if (assignedReplicas == newReplicas) {
                        throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
                                " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
                    } else {
                        info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
                        // first register ISR change listener
                        watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
                        controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
                        // mark topic ineligible for deletion for the partitions being reassigned
                        deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
                        onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                    }
                case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
                        .format(topicAndPartition))
            }
        } catch {
            case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
                // remove the partition from the admin path to unblock the admin client
                removePartitionFromReassignedPartitions(topicAndPartition)
        }
    }

    /**
     * 通过分区选择器 PreferredReplicaPartitionLeaderSelector 选举 leader 副本和 ISR 集合
     *
     * @param partitions
     * @param isTriggeredByAutoRebalance
     */
    def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
        info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
        try {
            // 添加参与优先副本选举的分区
            controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions
            // 设置对应的 topic 为不可删除
            deleteTopicManager.markTopicIneligibleForDeletion(partitions.map(_.topic))
            // 设置分区为 OnlinePartition 状态，包括重选 leader、更新 ZK，以及发送 LeaderAndIsrRequest 和 UpdateMetadataRequest 请求
            partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
        } catch {
            case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
        } finally {
            // 清理相关数据
            removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
            // 将 topic 标记为可删除，并唤醒 DeleteTopicsThread 线程
            deleteTopicManager.resumeDeletionForTopics(partitions.map(_.topic))
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
     * is the controller. It merely registers the session expiration listener and starts the controller leader
     * elector
     */
    def startup(): Unit = {
        inLock(controllerContext.controllerLock) {
            info("Controller starting up")
            registerSessionExpirationListener()
            isRunning = true
            controllerElector.startup
            info("Controller startup complete")
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
     */
    def shutdown(): Unit = {
        inLock(controllerContext.controllerLock) {
            isRunning = false
        }
        onControllerResignation()
    }

    def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                    callback: AbstractResponse => Unit = null): Unit = {
        controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
    }

    def incrementControllerEpoch(zkClient: ZkClient): Unit = {
        try {
            val newControllerEpoch = controllerContext.epoch + 1
            val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
                ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
            if (!updateSucceeded)
                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
            else {
                controllerContext.epochZkVersion = newVersion
                controllerContext.epoch = newControllerEpoch
            }
        } catch {
            case _: ZkNoNodeException =>
                // if path doesn't exist, this is the first controller whose epoch should be 1
                // the following call can still fail if another controller gets elected between checking if the path exists and
                // trying to create the controller epoch path
                try {
                    zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
                    controllerContext.epoch = KafkaController.InitialControllerEpoch
                    controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
                } catch {
                    case _: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
                            "Aborting controller startup procedure")
                    case oe: Throwable => error("Error while incrementing controller epoch", oe)
                }
            case oe: Throwable => error("Error while incrementing controller epoch", oe)

        }
        info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
    }

    private def registerSessionExpirationListener(): Unit = {
        zkUtils.zkClient.subscribeStateChanges(new SessionExpirationListener())
    }

    private def initializeControllerContext() {
        // update controller cache with delete topic information
        controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster.toSet
        controllerContext.allTopics = zkUtils.getAllTopics.toSet
        controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
        controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
        controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
        // update the leader and isr cache for all existing partitions from Zookeeper
        updateLeaderAndIsrCache()
        // start the channel manager
        startChannelManager()
        initializePreferredReplicaElection()
        initializePartitionReassignment()
        initializeTopicDeletion()
        info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
        info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
        info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
    }

    private def initializePreferredReplicaElection() {
        // initialize preferred replica election state
        val partitionsUndergoingPreferredReplicaElection = zkUtils.getPartitionsUndergoingPreferredReplicaElection
        // check if they are already completed or topic was deleted
        val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
            val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)
            val topicDeleted = replicasOpt.isEmpty
            val successful =
                if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
            successful || topicDeleted
        }
        controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
        info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
        info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
        info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
    }

    private def initializePartitionReassignment() {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned
        // check if they are already completed or topic was deleted
        val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
            val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)
            val topicDeleted = replicasOpt.isEmpty
            val successful = if (!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
            topicDeleted || successful
        }.keys
        reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
        var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
        partitionsToReassign ++= partitionsBeingReassigned
        partitionsToReassign --= reassignedPartitions
        controllerContext.partitionsBeingReassigned ++= partitionsToReassign
        info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
        info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
        info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))
    }

    private def initializeTopicDeletion() {
        val topicsQueuedForDeletion = zkUtils.getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath).toSet
        val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case (_, replicas) =>
            replicas.exists(r => !controllerContext.liveBrokerIds.contains(r))
        }.keySet.map(_.topic)
        val topicsForWhichPreferredReplicaElectionIsInProgress = controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic)
        val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
        val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress |
                topicsForWhichPreferredReplicaElectionIsInProgress
        info("List of topics to be deleted: %s".format(topicsQueuedForDeletion.mkString(",")))
        info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
        // initialize the topic deletion manager
        deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion)
    }

    private def maybeTriggerPartitionReassignment() {
        controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
            initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
        }
    }

    private def maybeTriggerPreferredReplicaElection() {
        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
    }

    private def startChannelManager() {
        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, threadNamePrefix)
        controllerContext.controllerChannelManager.startup()
    }

    def updateLeaderAndIsrCache(topicAndPartitions: Set[TopicAndPartition] = controllerContext.partitionReplicaAssignment.keySet) {
        val leaderAndIsrInfo = zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, topicAndPartitions)
        for ((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
            controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
    }

    private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
        zkUtils.getLeaderAndIsrForPartition(topic, partition) match {
            case Some(leaderAndIsr) =>
                val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
                replicasNotInIsr.isEmpty
            case None => false
        }
    }

    private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                        reassignedPartitionContext: ReassignedPartitionsContext) {
        val reassignedReplicas = reassignedPartitionContext.newReplicas
        val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
        // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
        // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
        val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
        if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
            info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                    "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
            // move the leader to one of the alive and caught up new replicas
            partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
        } else {
            // check if the leader is alive or not
            if (controllerContext.liveBrokerIds.contains(currentLeader)) {
                info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                        "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
                // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
                updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
            } else {
                info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
                        "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
                partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
            }
        }
    }

    private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     oldReplicas: Set[Int]) {
        val topic = topicAndPartition.topic
        val partition = topicAndPartition.partition
        // first move the replica to offline state (the controller removes it from the ISR)
        val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
        // send stop replica command to the old replicas
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
        // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
    }

    private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                   replicas: Seq[Int]) {
        val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
        partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
        info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
        // update the assigned replica list after a successful zookeeper write
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
    }

    private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                       reassignedPartitionContext: ReassignedPartitionsContext,
                                                       newReplicas: Set[Int]) {
        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
        // replicas list
        newReplicas.foreach { replica =>
            replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
        }
    }

    private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
        brokerRequestBatch.newBatch()
        updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
            case Some(updatedLeaderIsrAndControllerEpoch) =>
                try {
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
                        topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
                    brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
                } catch {
                    case e: IllegalStateException =>
                        // Resign if the controller is in an illegal state
                        error("Forcing the controller to resign")
                        brokerRequestBatch.clear()
                        controllerElector.resign()
                        throw e
                }
                stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
                        "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
                    newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
            case None => // fail the reassignment
                stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
                        "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
                    newAssignedReplicas.mkString(","), topicAndPartition))
        }
    }

    private def registerIsrChangeNotificationListener(): util.List[String] = {
        debug("Registering IsrChangeNotificationListener")
        zkUtils.zkClient.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
    }

    private def deregisterIsrChangeNotificationListener(): Unit = {
        debug("De-registering IsrChangeNotificationListener")
        zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
    }

    private def registerReassignedPartitionsListener(): Unit = {
        zkUtils.zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
    }

    private def deregisterReassignedPartitionsListener(): Unit = {
        zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
    }

    private def registerPreferredReplicaElectionListener() {
        zkUtils.zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
    }

    private def deregisterPreferredReplicaElectionListener() {
        zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
    }

    private def deregisterReassignedPartitionsIsrChangeListeners() {
        controllerContext.partitionsBeingReassigned.foreach {
            case (topicAndPartition, reassignedPartitionsContext) =>
                val zkPartitionPath = getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
                zkUtils.zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
        }
    }

    private def readControllerEpochFromZookeeper() {
        // initialize the controller epoch and zk version by reading from zookeeper
        if (controllerContext.zkUtils.pathExists(ZkUtils.ControllerEpochPath)) {
            val epochData = controllerContext.zkUtils.readData(ZkUtils.ControllerEpochPath)
            controllerContext.epoch = epochData._1.toInt
            controllerContext.epochZkVersion = epochData._2.getVersion
            info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
        }
    }

    def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
        if (controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
            // stop watching the ISR changes for this partition
            zkUtils.zkClient.unsubscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
        }
        // read the current list of reassigned partitions from zookeeper
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned
        // remove this partition from that list
        val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
        // write the new list to zookeeper
        zkUtils.updatePartitionReassignmentData(updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
        // update the cache. NO-OP if the partition's reassignment was never started
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
    }

    def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                           newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
        try {
            val zkPath = getTopicPath(topicAndPartition.topic)
            val jsonPartitionMap = zkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => e._1.partition.toString -> e._2))
            zkUtils.updatePersistentPath(zkPath, jsonPartitionMap)
            debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
        } catch {
            case _: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
            case e2: Throwable => throw new KafkaException(e2.toString)
        }
    }

    def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                     isTriggeredByAutoRebalance: Boolean) {
        for (partition <- partitionsToBeRemoved) {
            // check the status
            val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
            val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
            if (currentLeader == preferredReplica) {
                info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
            } else {
                warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
            }
        }
        if (!isTriggeredByAutoRebalance)
            zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
    }

    /**
     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
     * metadata requests
     *
     * @param brokers The brokers that the update metadata request should be sent to
     */
    def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
        try {
            brokerRequestBatch.newBatch()
            brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
            brokerRequestBatch.sendRequestsToBrokers(epoch)
        } catch {
            case e: IllegalStateException =>
                // Resign if the controller is in an illegal state
                error("Forcing the controller to resign")
                brokerRequestBatch.clear()
                controllerElector.resign()
                throw e
        }
    }

    /**
     * Removes a given partition replica from the ISR; if it is not the current
     * leader and there are sufficient remaining replicas in ISR.
     *
     * @param topic     topic
     * @param partition partition
     * @param replicaId replica Id
     * @return the new leaderAndIsr (with the replica removed if it was present),
     *         or None if leaderAndIsr is empty.
     */
    def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
        val topicAndPartition = TopicAndPartition(topic, partition)
        debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
            controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
        var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
        var zkWriteCompleteOrUnnecessary = false
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
            zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
                case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
                    val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
                    val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
                    if (controllerEpoch > epoch)
                        throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                                "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
                                "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
                    if (leaderAndIsr.isr.contains(replicaId)) {
                        // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
                        val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
                        var newIsr = leaderAndIsr.isr.filter(b => b != replicaId)

                        // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
                        // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
                        // eventually be restored as the leader.
                        if (newIsr.isEmpty && !LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(zkUtils,
                            ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                            info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
                            newIsr = leaderAndIsr.isr
                        }

                        val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                            newIsr, leaderAndIsr.zkVersion + 1)
                        // update the new leadership decision in zookeeper or retry
                        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
                            newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

                        newLeaderAndIsr.zkVersion = newVersion
                        finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
                        controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
                        if (updateSucceeded)
                            info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
                        updateSucceeded
                    } else {
                        warn("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s"
                                .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
                        finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
                        controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
                        true
                    }
                case None =>
                    warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
                    true
            }
        }
        finalLeaderIsrAndControllerEpoch
    }

    /**
     * Does not change leader or isr, but just increments the leader epoch
     *
     * @param topic     topic
     * @param partition partition
     * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
     */
    private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
        val topicAndPartition = TopicAndPartition(topic, partition)
        debug("Updating leader epoch for partition %s.".format(topicAndPartition))
        var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
        var zkWriteCompleteOrUnnecessary = false
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
            zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
                case Some(leaderIsrAndEpoch) =>
                    val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
                    val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
                    if (controllerEpoch > epoch)
                        throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                                "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
                                "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
                    // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
                    // assigned replica list
                    val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                        leaderAndIsr.isr, leaderAndIsr.zkVersion + 1)
                    // update the new leadership decision in zookeeper or retry
                    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic,
                        partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

                    newLeaderAndIsr.zkVersion = newVersion
                    finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
                    if (updateSucceeded)
                        info("Updated leader epoch for partition %s to %d".format(topicAndPartition, newLeaderAndIsr.leaderEpoch))
                    updateSucceeded
                case None =>
                    throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
                            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
                    true
            }
        }
        finalLeaderIsrAndControllerEpoch
    }

    class SessionExpirationListener() extends IZkStateListener with Logging {
        this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "

        def handleStateChanged(state: KeeperState) {
            // do nothing, since zkclient will do reconnect for us.
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception On any error.
         */
        @throws[Exception]
        def handleNewSession() {
            info("ZK expired; shut down all controller components and try to re-elect")
            if (controllerElector.getControllerID() != config.brokerId) {
                onControllerResignation()
                inLock(controllerContext.controllerLock) {
                    controllerElector.elect
                }
            } else {
                // This can happen when there are multiple consecutive session expiration and handleNewSession() are called multiple
                // times. The first call may already register the controller path using the newest ZK session. Therefore, the
                // controller path will exist in subsequent calls to handleNewSession().
                info("ZK expired, but the current controller id %d is the same as this broker id, skip re-elect".format(config.brokerId))
            }
        }

        def handleSessionEstablishmentError(error: Throwable): Unit = {
            //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError
        }
    }

    private def checkAndTriggerPartitionRebalance(): Unit = {
        if (isActive) {
            trace("checking need to trigger partition rebalance")
            // get all the active brokers
            var preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] = null
            inLock(controllerContext.controllerLock) {
                preferredReplicasForTopicsByBrokers =
                        controllerContext.partitionReplicaAssignment.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic)).groupBy {
                            case (_, assignedReplicas) => assignedReplicas.head
                        }
            }
            debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers)
            // for each broker, check if a preferred replica election needs to be triggered
            preferredReplicasForTopicsByBrokers.foreach {
                case (leaderBroker, topicAndPartitionsForBroker) =>
                    var imbalanceRatio: Double = 0
                    var topicsNotInPreferredReplica: Map[TopicAndPartition, Seq[Int]] = null
                    inLock(controllerContext.controllerLock) {
                        topicsNotInPreferredReplica = topicAndPartitionsForBroker.filter { case (topicPartition, _) =>
                            controllerContext.partitionLeadershipInfo.contains(topicPartition) &&
                                    controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != leaderBroker
                        }
                        debug("topics not in preferred replica " + topicsNotInPreferredReplica)
                        val totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size
                        val totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size
                        imbalanceRatio = totalTopicPartitionsNotLedByBroker.toDouble / totalTopicPartitionsForBroker
                        trace("leader imbalance ratio for broker %d is %f".format(leaderBroker, imbalanceRatio))
                    }
                    // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
                    // that need to be on this broker
                    if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
                        topicsNotInPreferredReplica.keys.foreach { topicPartition =>
                            inLock(controllerContext.controllerLock) {
                                // do this check only if the broker is live and there are no partitions being reassigned currently
                                // and preferred replica election is not in progress
                                if (controllerContext.liveBrokerIds.contains(leaderBroker) &&
                                        controllerContext.partitionsBeingReassigned.isEmpty &&
                                        controllerContext.partitionsUndergoingPreferredReplicaElection.isEmpty &&
                                        !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                                        controllerContext.allTopics.contains(topicPartition.topic)) {
                                    onPreferredReplicaElection(Set(topicPartition), isTriggeredByAutoRebalance = true)
                                }
                            }
                        }
                    }
            }
        }
    }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned partitions.
 *
 * 监听 ZK 的 /admin/reassign_partitions 路径
 */
class PartitionsReassignedListener(protected val controller: KafkaController) extends ControllerZkDataListener {

    private val controllerContext = controller.controllerContext

    protected def logName = "PartitionsReassignedListener"

    /**
     * Invoked when some partitions are reassigned by the admin command
     *
     * @throws Exception On any error.
     */
    @throws[Exception]
    def doHandleDataChange(dataPath: String, data: AnyRef) {
        debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
                .format(dataPath, data))
        val partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString)
        val partitionsToBeReassigned = inLock(controllerContext.controllerLock) {
            partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
        }
        partitionsToBeReassigned.foreach { partitionToBeReassigned =>
            inLock(controllerContext.controllerLock) {
                if (controller.deleteTopicManager.isTopicQueuedUpForDeletion(partitionToBeReassigned._1.topic)) {
                    error("Skipping reassignment of partition %s for topic %s since it is currently being deleted"
                            .format(partitionToBeReassigned._1, partitionToBeReassigned._1.topic))
                    controller.removePartitionFromReassignedPartitions(partitionToBeReassigned._1)
                } else {
                    val context = ReassignedPartitionsContext(partitionToBeReassigned._2)
                    controller.initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)
                }
            }
        }
    }

    def doHandleDataDeleted(dataPath: String) {}
}

class ReassignedPartitionsIsrChangeListener(protected val controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int]) extends ControllerZkDataListener {
    private val zkUtils = controller.controllerContext.zkUtils
    private val controllerContext = controller.controllerContext

    protected def logName = "ReassignedPartitionsIsrChangeListener"

    /**
     * Invoked when some partitions need to move leader to preferred replica
     */
    def doHandleDataChange(dataPath: String, data: AnyRef) {
        inLock(controllerContext.controllerLock) {
            debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
            val topicAndPartition = TopicAndPartition(topic, partition)
            try {
                // check if this partition is still being reassigned or not
                controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
                    case Some(reassignedPartitionContext) =>
                        // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
                        val newLeaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topic, partition)
                        newLeaderAndIsrOpt match {
                            case Some(leaderAndIsr) => // check if new replicas have joined ISR
                                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                                if (caughtUpReplicas == reassignedReplicas) {
                                    // resume the partition reassignment process
                                    info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                                            .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                                            "Resuming partition reassignment")
                                    controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                                }
                                else {
                                    info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                                            .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                                            "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                                }
                            case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
                                    .format(topicAndPartition, reassignedReplicas.mkString(",")))
                        }
                    case None =>
                }
            } catch {
                case e: Throwable => error("Error while handling partition reassignment", e)
            }
        }
    }

    def doHandleDataDeleted(dataPath: String) {}

}

/**
 * Called when leader intimates of isr change
 *
 * 监听 ZK 的 /isr_change_notification 路径，当某些分区的 ISR 集合变化时通知整个集群中所有的 broker
 *
 * @param controller
 */
class IsrChangeNotificationListener(protected val controller: KafkaController) extends ControllerZkChildListener {

    protected def logName = "IsrChangeNotificationListener"

    def doHandleChildChange(parentPath: String, currentChildren: Seq[String]): Unit = {
        inLock(controller.controllerContext.controllerLock) {
            debug("ISR change notification listener fired")
            try {
                val topicAndPartitions = currentChildren.flatMap(getTopicAndPartition).toSet
                if (topicAndPartitions.nonEmpty) {
                    // 从 ZK 读取指定分区的 leader 副本、ISR 集合等信息，更新 controller 上下文
                    controller.updateLeaderAndIsrCache(topicAndPartitions)
                    // 向可用的 broker 发送 UpdateMetadataRequest 请求，更新对应的 MetadataCache
                    processUpdateNotifications(topicAndPartitions)
                }
            } finally {
                // 删除 /isr_change_notification/partitions 路径下已经处理的信息
                currentChildren.map(x => controller.controllerContext.zkUtils.deletePath(ZkUtils.IsrChangeNotificationPath + "/" + x))
            }
        }
    }

    private def processUpdateNotifications(topicAndPartitions: immutable.Set[TopicAndPartition]) {
        val liveBrokers: Seq[Int] = controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq
        debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicAndPartitions:" + topicAndPartitions)
        controller.sendUpdateMetadataRequest(liveBrokers, topicAndPartitions)
    }

    private def getTopicAndPartition(child: String): Set[TopicAndPartition] = {
        val changeZnode: String = ZkUtils.IsrChangeNotificationPath + "/" + child
        val (jsonOpt, _) = controller.controllerContext.zkUtils.readDataMaybeNull(changeZnode)
        if (jsonOpt.isDefined) {
            val json = Json.parseFull(jsonOpt.get)

            json match {
                case Some(m) =>
                    val topicAndPartitions: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
                    val isrChanges = m.asInstanceOf[Map[String, Any]]
                    val topicAndPartitionList = isrChanges("partitions").asInstanceOf[List[Any]]
                    topicAndPartitionList.foreach {
                        tp =>
                            val topicAndPartition = tp.asInstanceOf[Map[String, Any]]
                            val topic = topicAndPartition("topic").asInstanceOf[String]
                            val partition = topicAndPartition("partition").asInstanceOf[Int]
                            topicAndPartitions += TopicAndPartition(topic, partition)
                    }
                    topicAndPartitions
                case None =>
                    error("Invalid topic and partition JSON: " + jsonOpt.get + " in ZK: " + changeZnode)
                    Set.empty
            }
        } else {
            Set.empty
        }
    }
}

object IsrChangeNotificationListener {
    val version: Long = 1L
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election
 *
 * 监听 ZK 的 /admin/preferred_replica_election 路径，执行优先副本选举成为 leader
 */
class PreferredReplicaElectionListener(protected val controller: KafkaController) extends ControllerZkDataListener {

    private val controllerContext = controller.controllerContext

    protected def logName = "PreferredReplicaElectionListener"

    /**
     * Invoked when some partitions are reassigned by the admin command
     *
     * @throws Exception On any error.
     */
    @throws[Exception]
    def doHandleDataChange(dataPath: String, data: AnyRef) {
        debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s"
                .format(dataPath, data.toString))
        inLock(controllerContext.controllerLock) {
            // 获取需要进行优先副本选举的 TP 集合
            val partitionsForPreferredReplicaElection =
                PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
            if (controllerContext.partitionsUndergoingPreferredReplicaElection.nonEmpty)
                info("These partitions are already undergoing preferred replica election: %s"
                        .format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
            // 过滤正在进行优先副本选举的分区
            val partitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection
            // 过滤掉待删除的 topic 分区
            val partitionsForTopicsToBeDeleted = partitions.filter(p => controller.deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
            if (partitionsForTopicsToBeDeleted.nonEmpty) {
                error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
                        .format(partitionsForTopicsToBeDeleted))
            }
            // 对剩余的分区执行优先副本选举
            controller.onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
        }
    }

    def doHandleDataDeleted(dataPath: String) {}
}

/**
 *
 * @param newReplicas       新分配的 AR 集合
 * @param isrChangeListener 监听 ISR 集合变化的监听器
 */
case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
    override def toString: String = {
        "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
    }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr,
                                       controllerEpoch: Int) {

    override def toString: String = {
        val leaderAndIsrInfo = new StringBuilder
        leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
        leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
        leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
        leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
        leaderAndIsrInfo.toString()
    }
}

object ControllerStats extends KafkaMetricsGroup {

    private val _uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
    private val _leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))

    // KafkaServer needs to initialize controller metrics during startup. We perform initialization
    // through method calls to avoid Scala compiler warnings.
    def uncleanLeaderElectionRate: Meter = _uncleanLeaderElectionRate

    def leaderElectionTimer: KafkaTimer = _leaderElectionTimer
}
