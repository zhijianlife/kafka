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

package kafka.server

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.api._
import kafka.cluster.{Broker, EndPoint}
import kafka.common.{BrokerEndPointNotAvailableException, Topic, TopicAndPartition}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, PartitionState, UpdateMetadataRequest}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

/**
 * A cache for the state (e.g., current leader) of each partition. This cache is updated through
 * UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 *
 * Broker 使用 MetadataCache 来缓存整个集群中全部分区状态的组件
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {

    private val stateChangeLogger = KafkaController.stateChangeLogger

    /** 缓存每个分区的状态 */
    private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]()
    private var controllerId: Option[Int] = None

    /** 记录当前可用的 broker 信息 */
    private val aliveBrokers = mutable.Map[Int, Broker]()

    /** 记录可用的节点信息 */
    private val aliveNodes = mutable.Map[Int, collection.Map[ListenerName, Node]]()
    private val partitionMetadataLock = new ReentrantReadWriteLock()

    this.logIdent = s"[Kafka Metadata Cache on broker $brokerId] "

    // This method is the main hotspot when it comes to the performance of metadata requests,
    // we should be careful about adding additional logic here.
    // filterUnavailableEndpoints exists to support v0 MetadataResponses
    private def getEndpoints(brokers: Iterable[Int], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
        val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
        brokers.foreach { brokerId =>
            val endpoint = getAliveEndpoint(brokerId, listenerName) match {
                case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
                case Some(node) => Some(node)
            }
            endpoint.foreach(result +=)
        }
        result
    }

    private def getAliveEndpoint(brokerId: Int, listenerName: ListenerName): Option[Node] =
        aliveNodes.get(brokerId).map { nodeMap =>
            nodeMap.getOrElse(listenerName,
                throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not have listener with name `$listenerName`"))
        }

    /**
     * errorUnavailableEndpoints exists to support v0 MetadataResponses
     *
     * @param topic
     * @param listenerName
     * @param errorUnavailableEndpoints
     * @return
     */
    private def getPartitionMetadata(topic: String,
                                     listenerName: ListenerName,
                                     errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
        // 遍历每个 topic 对应的分区集合
        cache.get(topic).map { partitions =>
            partitions.map { case (partitionId, partitionState) =>
                val topicPartition = TopicAndPartition(topic, partitionId)

                // 获取分区对应的 leaderAndIsr 对象
                val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
                // 获取 leader 副本所在的节点
                val maybeLeader = getAliveEndpoint(leaderAndIsr.leader, listenerName)
                // 获取分区的 AR 集合
                val replicas = partitionState.allReplicas
                // 获取 AR 集合中可用的副本
                val replicaInfo = getEndpoints(replicas, listenerName, errorUnavailableEndpoints)

                maybeLeader match {
                    // 分区的 leader 副本可能宕机了
                    case None =>
                        debug(s"Error while fetching metadata for $topicPartition: leader not available")
                        new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
                            replicaInfo.asJava, java.util.Collections.emptyList())

                    case Some(leader) =>
                        // 获取分区的 ISR 集合
                        val isr = leaderAndIsr.isr
                        // 获取 ISR 集合中可用的副本
                        val isrInfo = getEndpoints(isr, listenerName, errorUnavailableEndpoints)
                        // 检测 AR 集合中的副本是否都是可用的
                        if (replicaInfo.size < replicas.size) {
                            debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                                    s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")
                            new MetadataResponse.PartitionMetadata(
                                Errors.REPLICA_NOT_AVAILABLE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        }
                        // 检测 ISR 集合中的副本是否都是可用的
                        else if (isrInfo.size < isr.size) {
                            debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                                    s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
                            new MetadataResponse.PartitionMetadata(
                                Errors.REPLICA_NOT_AVAILABLE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        } else {
                            // AR 和 ISR 集合中的副本都是可用的
                            new MetadataResponse.PartitionMetadata(
                                Errors.NONE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        }
                }
            }
        }
    }

    /**
     * errorUnavailableEndpoints exists to support v0 MetadataResponses
     *
     * @param topics
     * @param listenerName
     * @param errorUnavailableEndpoints
     * @return
     */
    def getTopicMetadata(topics: Set[String],
                         listenerName: ListenerName,
                         errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
        inReadLock(partitionMetadataLock) {
            topics.toSeq.flatMap { topic =>
                // 获取对应的 PartitionMetadata，并将 PartitionMetadata 信息与 topic 信息进行封装，生成 TopicMetadata
                getPartitionMetadata(topic, listenerName, errorUnavailableEndpoints).map { partitionMetadata =>
                    new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
                }
            }
        }
    }

    def getAllTopics: Set[String] = {
        inReadLock(partitionMetadataLock) {
            cache.keySet.toSet
        }
    }

    def getNonExistingTopics(topics: Set[String]): Set[String] = {
        inReadLock(partitionMetadataLock) {
            topics -- cache.keySet
        }
    }

    def getAliveBrokers: Seq[Broker] = {
        inReadLock(partitionMetadataLock) {
            aliveBrokers.values.toBuffer
        }
    }

    private def addOrUpdatePartitionInfo(topic: String,
                                         partitionId: Int,
                                         stateInfo: PartitionStateInfo) {
        inWriteLock(partitionMetadataLock) {
            val infos = cache.getOrElseUpdate(topic, mutable.Map())
            infos(partitionId) = stateInfo
        }
    }

    def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
        inReadLock(partitionMetadataLock) {
            cache.get(topic).flatMap(_.get(partitionId))
        }
    }

    def getControllerId: Option[Int] = controllerId

    /**
     * This method returns the deleted TopicPartitions received from UpdateMetadataRequest
     *
     * @param correlationId
     * @param updateMetadataRequest
     * @return
     */
    def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
        inWriteLock(partitionMetadataLock) {
            // 更新 controllerId
            controllerId = updateMetadataRequest.controllerId match {
                case id if id < 0 => None
                case id => Some(id)
            }

            // 清除 aliveNodes 和 aliveBrokers，并由 UpdateMetadataRequest.liveBrokers 重新构建
            aliveNodes.clear()
            aliveBrokers.clear()
            updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
                // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
                // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
                // move to `AnyRefMap`, which has comparable performance.
                val nodes = new java.util.HashMap[ListenerName, Node]
                val endPoints = new mutable.ArrayBuffer[EndPoint]
                broker.endPoints.asScala.foreach { ep =>
                    endPoints += EndPoint(ep.host, ep.port, ep.listenerName, ep.securityProtocol)
                    nodes.put(ep.listenerName, new Node(broker.id, ep.host, ep.port))
                }
                aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
                aliveNodes(broker.id) = nodes.asScala
            }

            // 基于 UpdateMetadataRequest.partitionStates 更新缓存的每个分区的状态
            val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
            updateMetadataRequest.partitionStates.asScala.foreach {
                case (tp, info) =>
                    val controllerId = updateMetadataRequest.controllerId
                    val controllerEpoch = updateMetadataRequest.controllerEpoch
                    if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
                        // 删除对应分区的 PartitionStateInfo
                        removePartitionInfo(tp.topic, tp.partition)
                        stateChangeLogger.trace(s"Broker $brokerId deleted partition $tp from metadata cache in response to UpdateMetadata " +
                                s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
                        deletedPartitions += tp
                    } else {
                        val partitionInfo = partitionStateToPartitionStateInfo(info)
                        // 更新对应分区的 PartitionStateInfo
                        addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
                        stateChangeLogger.trace(s"Broker $brokerId cached leader info $partitionInfo for partition $tp in response to " +
                                s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
                    }
            }
            deletedPartitions
        }
    }

    private def partitionStateToPartitionStateInfo(partitionState: PartitionState): PartitionStateInfo = {
        val leaderAndIsr = LeaderAndIsr(partitionState.leader, partitionState.leaderEpoch, partitionState.isr.asScala.map(_.toInt).toList, partitionState.zkVersion)
        val leaderInfo = LeaderIsrAndControllerEpoch(leaderAndIsr, partitionState.controllerEpoch)
        PartitionStateInfo(leaderInfo, partitionState.replicas.asScala.map(_.toInt))
    }

    def contains(topic: String): Boolean = {
        inReadLock(partitionMetadataLock) {
            cache.contains(topic)
        }
    }

    private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
        cache.get(topic).exists { infos =>
            infos.remove(partitionId)
            if (infos.isEmpty) cache.remove(topic)
            true
        }
    }

}
