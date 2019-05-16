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

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition, requests}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}

/**
 * 维护了 controller leader 与集群中的其他 broker 之间的网络连接，是管理整个集群的基础
 *
 * @param controllerContext
 * @param config
 * @param time
 * @param metrics
 * @param threadNamePrefix
 */
class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               threadNamePrefix: Option[String] = None) extends Logging {

    /** 管理集群中每个 broker 节点与 controller 之间的连接信息 */
    protected val brokerStateInfo = new mutable.HashMap[Int, ControllerBrokerStateInfo]
    private val brokerLock = new Object
    this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

    // 遍历处理当前集群中可用的 broker 节点
    controllerContext.liveBrokers.foreach(this.addNewBroker)

    def startup(): Unit = {
        brokerLock synchronized {
            brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
        }
    }

    def shutdown(): Unit = {
        brokerLock synchronized {
            brokerStateInfo.values.foreach(removeExistingBroker)
        }
    }

    def sendRequest(brokerId: Int,
                    apiKey: ApiKeys,
                    request: AbstractRequest.Builder[_ <: AbstractRequest],
                    callback: AbstractResponse => Unit = null) {
        brokerLock synchronized {
            val stateInfoOpt = brokerStateInfo.get(brokerId)
            stateInfoOpt match {
                case Some(stateInfo) =>
                    // 缓存到队列中
                    stateInfo.messageQueue.put(QueueItem(apiKey, request, callback))
                case None =>
                    warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
            }
        }
    }

    def addBroker(broker: Broker) {
        // be careful here. Maybe the startup() API has already started the request send thread
        brokerLock synchronized {
            if (!brokerStateInfo.contains(broker.id)) {
                addNewBroker(broker)
                startRequestSendThread(broker.id)
            }
        }
    }

    def removeBroker(brokerId: Int) {
        brokerLock synchronized {
            removeExistingBroker(brokerStateInfo(brokerId))
        }
    }

    /**
     * 新建到指定 broker 节点的连接信息，并记录到本地 brokerStateInfo 集合中
     *
     * @param broker
     */
    private def addNewBroker(broker: Broker) {
        // 创建消息队列，用于存放发往指定 broker 节点的请求
        val messageQueue = new LinkedBlockingQueue[QueueItem]
        debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
        val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
        val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
        // 创建网络连接客户端
        val networkClient = {
            val channelBuilder = ChannelBuilders.clientChannelBuilder(
                config.interBrokerSecurityProtocol,
                LoginType.SERVER,
                config.values,
                config.saslMechanismInterBrokerProtocol,
                config.saslInterBrokerHandshakeRequestEnable
            )
            val selector = new Selector(
                NetworkReceive.UNLIMITED,
                Selector.NO_IDLE_TIMEOUT_MS,
                metrics,
                time,
                "controller-channel",
                Map("broker-id" -> broker.id.toString).asJava,
                false,
                channelBuilder
            )
            new NetworkClient(
                selector,
                new ManualMetadataUpdater(Seq(brokerNode).asJava),
                config.brokerId.toString,
                1,
                0,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.requestTimeoutMs,
                time,
                false
            )
        }

        // 创建请求发送线程对象
        val threadName = threadNamePrefix match {
            case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
            case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
        }
        val requestThread = new RequestSendThread(
            config.brokerId, controllerContext, messageQueue, networkClient, brokerNode, config, time, threadName)
        requestThread.setDaemon(false)

        // 记录到 brokerStateInfo 集合
        brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
    }

    private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
        try {
            // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
            // non-threadsafe classes as described in KAFKA-4959.
            // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
            // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
            brokerState.requestSendThread.shutdown()
            // 关闭底层网络连接
            brokerState.networkClient.close()
            // 清空消息队列
            brokerState.messageQueue.clear()
            // 移除对应的 ControllerBrokerStateInfo 对象
            brokerStateInfo.remove(brokerState.brokerNode.id)
        } catch {
            case e: Throwable => error("Error while removing broker by the controller", e)
        }
    }

    protected def startRequestSendThread(brokerId: Int) {
        val requestThread = brokerStateInfo(brokerId).requestSendThread
        if (requestThread.getState == Thread.State.NEW)
            requestThread.start()
    }
}

/**
 *
 * @param apiKey   请求类型
 * @param request  请求对象
 * @param callback 响应回调函数
 */
case class QueueItem(apiKey: ApiKeys,
                     request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: AbstractResponse => Unit)

/**
 * 请求发送线程
 *
 * @param controllerId
 * @param controllerContext
 * @param queue
 * @param networkClient
 * @param brokerNode
 * @param config
 * @param time
 * @param name
 */
class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String
                       ) extends ShutdownableThread(name = name) {

    private val lock = new Object()
    private val stateChangeLogger = KafkaController.stateChangeLogger
    private val socketTimeoutMs = config.controllerSocketTimeoutMs

    override def doWork(): Unit = {

        def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))

        // 获取缓冲队列中的 QueueItem 对象，封装了请求类型、请求对象，以及响应回调函数
        val QueueItem(apiKey, requestBuilder, callback) = queue.take()

        import NetworkClientBlockingOps._ // 阻塞模式

        var clientResponse: ClientResponse = null
        try {
            lock synchronized {
                var isSendSuccessful = false // 标识请求是否发送成功
                while (isRunning.get() && !isSendSuccessful) {
                    // 当 broker 节点宕机后，会触发 ZK 的监听器调用 removeBroker 方法停止当前线程，在停止前会一直尝试重试
                    try {
                        // 阻塞等待指定 broker 节点是否允许接收请求
                        if (!brokerReady()) {
                            isSendSuccessful = false
                            backoff() // 等待 100 毫秒
                        } else {
                            // 创建并发送请求，同时阻塞等待响应
                            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder, time.milliseconds(), true)
                            clientResponse = networkClient.blockingSendAndReceive(clientRequest)(time)
                            isSendSuccessful = true
                        }
                    } catch {
                        // 如果发送失败，则断开连接，并等待一段时间后重试
                        case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
                            warn("Controller %d epoch %d fails to send request %s to broker %s. Reconnecting to broker."
                                    .format(controllerId, controllerContext.epoch, requestBuilder.toString, brokerNode.toString), e)
                            networkClient.close(brokerNode.idString)
                            isSendSuccessful = false
                            backoff()
                    }
                }

                // 解析响应
                if (clientResponse != null) {
                    // 解析请求类型
                    val api = ApiKeys.forId(clientResponse.requestHeader.apiKey)
                    // 只允许 LEADER_AND_ISR、STOP_REPLICA，以及 UPDATE_METADATA_KEY 这 3 种请求类型
                    if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA_KEY)
                        throw new KafkaException(s"Unexpected apiKey received: $apiKey")

                    val response = clientResponse.responseBody

                    stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
                            .format(controllerId, controllerContext.epoch, response.toString, brokerNode.toString))

                    // 执行响应回调函数
                    if (callback != null) {
                        callback(response)
                    }
                }
            }
        } catch {
            case e: Throwable =>
                error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString), e)
                // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
                networkClient.close(brokerNode.idString)
        }
    }

    private def brokerReady(): Boolean = {
        import NetworkClientBlockingOps._
        try {
            if (!networkClient.isReady(brokerNode)(time)) {
                if (!networkClient.blockingReady(brokerNode, socketTimeoutMs)(time))
                    throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")
                info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString))
            }
            true
        } catch {
            case e: Throwable =>
                warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString), e)
                networkClient.close(brokerNode.idString)
                false
        }
    }

}

/**
 * 实现向 broker 批量发送请求的功能
 *
 * @param controller
 */
class ControllerBrokerRequestBatch(controller: KafkaController) extends Logging {

    val controllerContext: ControllerContext = controller.controllerContext
    val controllerId: Int = controller.config.brokerId

    /** 记录发往指定 broker 的 LeaderAndIsrRequest 请求所需的信息 */
    val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]

    /** 记录发往指定 broker 的 StopReplicaRequest 请求所需的信息 */
    val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]

    /** UpdateMetadataRequest 请求对应的目标 brokerId 集合 */
    val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]

    /** UpdateMetadataRequest 请求对应的请求信息 */
    val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, PartitionStateInfo]
    private val stateChangeLogger = KafkaController.stateChangeLogger

    def newBatch() {
        // raise error if the previous batch is not empty
        if (leaderAndIsrRequestMap.nonEmpty)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
                    "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
        if (stopReplicaRequestMap.nonEmpty)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
        if (updateMetadataRequestBrokerSet.nonEmpty)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    "new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ".format(
                        updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()))
    }

    def clear() {
        leaderAndIsrRequestMap.clear()
        stopReplicaRequestMap.clear()
        updateMetadataRequestBrokerSet.clear()
        updateMetadataRequestPartitionInfoMap.clear()
    }

    /**
     * 往 leaderAndIsrRequestMap 中添加待发送的 LeaderAndIsrRequest 所需的数据，
     * 并调用 addUpdateMetadataRequestForBrokers 方法准备向集群中所有可用的 broker 发送 UpdateMetadataRequest 请求
     *
     * @param brokerIds 指定接收 LeaderAndIsrRequest 请求的 brokerId 集合
     * @param topic
     * @param partition
     * @param leaderIsrAndControllerEpoch
     * @param replicas
     * @param callback
     */
    def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                         topic: String,
                                         partition: Int,
                                         leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                         replicas: Seq[Int],
                                         callback: AbstractResponse => Unit = null) {
        val topicPartition = new TopicPartition(topic, partition)

        brokerIds.filter(_ >= 0).foreach { brokerId =>
            val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
            result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
        }

        // 准备向所有可用的 broker 发送 UpdateMetadataRequest 请求
        addUpdateMetadataRequestForBrokers(
            controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(TopicAndPartition(topic, partition)))
    }

    /**
     * 向 stopReplicaRequestMap 集合中添加 StopReplicaRequest 请求所需的数据
     *
     * @param brokerIds
     * @param topic
     * @param partition
     * @param deletePartition
     * @param callback
     */
    def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                        topic: String,
                                        partition: Int,
                                        deletePartition: Boolean,
                                        callback: (AbstractResponse, Int) => Unit = null) {
        brokerIds.filter(b => b >= 0).foreach { brokerId =>
            stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
            val v = stopReplicaRequestMap(brokerId)
            if (callback != null)
                stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
                    deletePartition, (r: AbstractResponse) => callback(r, brokerId))
            else
                stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
                    deletePartition)
        }
    }

    /**
     * Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted
     *
     * @param brokerIds
     * @param partitions
     * @param callback
     */
    def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                           partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                           callback: AbstractResponse => Unit = null) {

        // 定义回调函数
        def updateMetadataRequestPartitionInfo(partition: TopicAndPartition, beingDeleted: Boolean) {
            // 获取当前分区对应的 LeaderIsrAndControllerEpoch 信息
            val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
            leaderIsrAndControllerEpochOpt match {
                case Some(leaderIsrAndControllerEpoch) =>
                    // 获取分区的 AR 集合
                    val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
                    val partitionStateInfo = if (beingDeleted) {
                        val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
                        PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
                    } else {
                        PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
                    }
                    updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
                case None =>
                    info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
            }
        }

        val filteredPartitions = {
            // 如果指定的分区集合为空，则需要更新全部分区
            val givenPartitions = if (partitions.isEmpty)
                                      controllerContext.partitionLeadershipInfo.keySet
                                  else
                                      partitions
            // 过滤即将被删除的分区
            if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
                givenPartitions
            else
                givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
        }

        updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
        // 将分区信息添加到 updateMetadataRequestPartitionInfoMap 集合中等待发送，beingDeleted 为 false
        filteredPartitions.foreach(
            partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
        // 将即将被删除的分区信息添加到 updateMetadataRequestPartitionInfoMap 集合中等待发送，beingDeleted 为 true
        controller.deleteTopicManager.partitionsToBeDeleted.foreach(
            partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
    }

    /**
     * 创建相应请求，并添加到消息队列中，最终由 RequestSendThread 进行发送
     *
     * @param controllerEpoch
     */
    def sendRequestsToBrokers(controllerEpoch: Int) {
        try {
            // 遍历处理 leaderAndIsrRequestMap 集合
            leaderAndIsrRequestMap.foreach {
                case (broker, partitionStateInfos) =>
                    // 依据 leaderAndIsrRequestMap 集合创建 LeaderAndIsrRequest 对象
                    partitionStateInfos.foreach {
                        case (topicPartition, state) =>
                            val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
                            stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                    "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest, state.leaderIsrAndControllerEpoch, broker, topicPartition.topic, topicPartition.partition))
                    }
                    val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
                    val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
                        _.getNode(controller.config.interBrokerListenerName)
                    }
                    val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
                        val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
                        val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
                            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
                            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
                        topicPartition -> partitionState
                    }
                    val leaderAndIsrRequest = new LeaderAndIsrRequest.
                    Builder(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
                    // 将请求放入消息队列中，等待发送
                    controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest, null)
            }
            // 清空 leaderAndIsrRequestMap 集合
            leaderAndIsrRequestMap.clear()

            updateMetadataRequestPartitionInfoMap.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
                    "to brokers %s for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
                updateMetadataRequestBrokerSet.toString(), p._1)))
            val partitionStates = updateMetadataRequestPartitionInfoMap.map { case (topicPartition, partitionStateInfo) =>
                val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
                val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
                    leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
                    partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
                topicPartition -> partitionState
            }

            val version: Short =
                if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
                else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
                else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
                else 0

            val updateMetadataRequest = {
                val liveBrokers = if (version == 0) {
                    // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.
                    controllerContext.liveOrShuttingDownBrokers.map { broker =>
                        val securityProtocol = SecurityProtocol.PLAINTEXT
                        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
                        val node = broker.getNode(listenerName)
                        val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
                        new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
                    }
                } else {
                    controllerContext.liveOrShuttingDownBrokers.map { broker =>
                        val endPoints = broker.endPoints.map { endPoint =>
                            new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName)
                        }
                        new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
                    }
                }
                new UpdateMetadataRequest.Builder(
                    controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava).
                        setVersion(version)
            }

            updateMetadataRequestBrokerSet.foreach { broker =>
                controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, updateMetadataRequest, null)
            }
            updateMetadataRequestBrokerSet.clear()
            updateMetadataRequestPartitionInfoMap.clear()

            stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
                val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
                val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
                debug("The stop replica request (delete = true) sent to broker %d is %s"
                        .format(broker, stopReplicaWithDelete.mkString(",")))
                debug("The stop replica request (delete = false) sent to broker %d is %s"
                        .format(broker, stopReplicaWithoutDelete.mkString(",")))

                val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null)

                // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially
                // changes the order in which the requests are sent for the same partitions, but that's OK.
                val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
                    replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava)
                controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest)

                replicasToNotGroup.foreach { r =>
                    val stopReplicaRequest = new StopReplicaRequest.Builder(
                        controllerId, controllerEpoch, r.deletePartition,
                        Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
                    controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback)
                }
            }
            stopReplicaRequestMap.clear()
        } catch {
            case e: Throwable =>
                if (leaderAndIsrRequestMap.nonEmpty) {
                    error("Haven't been able to send leader and isr requests, current state of " +
                            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
                }
                if (updateMetadataRequestBrokerSet.nonEmpty) {
                    error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
                            s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
                }
                if (stopReplicaRequestMap.nonEmpty) {
                    error("Haven't been able to send stop replica requests, current state of " +
                            s"the map is $stopReplicaRequestMap. Exception message: $e")
                }
                throw new IllegalStateException(e)
        }
    }
}

/**
 * 表示一个与 broker 连接的相关信息
 *
 * @param networkClient     网络客户端
 * @param brokerNode        对应 broker 的网络位置信息
 * @param messageQueue      存放发往对应 broker 的请求
 * @param requestSendThread 请求发送线程
 */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread)

case class StopReplicaRequestInfo(replica: PartitionAndReplica,
                                  deletePartition: Boolean,
                                  callback: AbstractResponse => Unit = null)

class Callbacks private(var leaderAndIsrResponseCallback: AbstractResponse => Unit = null,
                        var updateMetadataResponseCallback: AbstractResponse => Unit = null,
                        var stopReplicaResponseCallback: (AbstractResponse, Int) => Unit = null)

object Callbacks {

    class CallbackBuilder {
        var leaderAndIsrResponseCbk: AbstractResponse => Unit = _
        var updateMetadataResponseCbk: AbstractResponse => Unit = _
        var stopReplicaResponseCbk: (AbstractResponse, Int) => Unit = _

        def leaderAndIsrCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
            leaderAndIsrResponseCbk = cbk
            this
        }

        def updateMetadataCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
            updateMetadataResponseCbk = cbk
            this
        }

        def stopReplicaCallback(cbk: (AbstractResponse, Int) => Unit): CallbackBuilder = {
            stopReplicaResponseCbk = cbk
            this
        }

        def build: Callbacks = {
            new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
        }
    }

}
