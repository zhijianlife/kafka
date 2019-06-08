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
 * A cache for the state (e.g., current leader) of each partition.
 * This cache is updated through UpdateMetadataRequest from the controller.
 * Every broker maintains the same cache, asynchronously.
 *
 * Broker 节点使用 MetadataCache 缓存整个集群上全部分区的状态信息，
 * 通过 controller 发送来的 UpdateMetadataRequest 请求进行维护，所有的 broker 维护的 cache 是一致的
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {

    private val stateChangeLogger = KafkaController.stateChangeLogger

    /** 缓存每个分区的状态信息 */
    private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]() // [topic, [分区 ID, 分区状态信息]]

    /** kafka controller leader 的 ID */
    private var controllerId: Option[Int] = None

    /** 记录当前可用的 broker 信息 */
    private val aliveBrokers = mutable.Map[Int, Broker]()
    /** 记录当前可用的 broker 节点信息 */
    private val aliveNodes = mutable.Map[Int, collection.Map[ListenerName, Node]]()

    private val partitionMetadataLock = new ReentrantReadWriteLock()

    this.logIdent = s"[Kafka Metadata Cache on broker $brokerId] "

    /**
     * This method is the main hotspot when it comes to the performance of metadata requests,
     * we should be careful about adding additional logic here.
     *
     * 获取指定 brokers 对应的可用节点信息
     *
     * @param brokers
     * @param listenerName
     * @param filterUnavailableEndpoints support v0 MetadataResponses
     * @return
     */
    private def getEndpoints(brokers: Iterable[Int], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
        val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
        brokers.foreach { brokerId =>
            val endpoint = this.getAliveEndpoint(brokerId, listenerName) match {
                case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
                case Some(node) => Some(node)
            }
            endpoint.foreach(result +=)
        }
        result
    }

    /**
     * 获取 brokerId 对应的可用节点信息，如果不可用则返回异常
     *
     * @param brokerId
     * @param listenerName
     * @return
     */
    private def getAliveEndpoint(brokerId: Int, listenerName: ListenerName): Option[Node] =
        aliveNodes.get(brokerId).map { nodeMap =>
            nodeMap.getOrElse(listenerName,
                throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not have listener with name `$listenerName`"))
        }

    /**
     * 获取指定 topic 下分区的元数据信息，对于 AR 集合或 ISR 集合存在不可用副本的分区，则返回 REPLICA_NOT_AVAILABLE 错误
     *
     * @param topic
     * @param listenerName
     * @param errorUnavailableEndpoints support v0 MetadataResponses
     * @return
     */
    private def getPartitionMetadata(topic: String,
                                     listenerName: ListenerName,
                                     errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
        // 遍历每个 topic 对应的分区集合
        cache.get(topic).map { partitions =>
            partitions.map { case (partitionId, partitionState) =>
                val topicPartition = TopicAndPartition(topic, partitionId)

                // 获取分区对应的 LeaderAndIsr 对象，其中封装了对应分区的 leader 副本 ID 和 ISR 集合等信息
                val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
                // 获取 leader 副本所在的节点信息
                val maybeLeader = this.getAliveEndpoint(leaderAndIsr.leader, listenerName)
                // 获取分区的 AR 集合
                val replicas = partitionState.allReplicas
                // 获取 AR 集合中可用的副本对应的节点信息
                val replicaInfo = this.getEndpoints(replicas, listenerName, errorUnavailableEndpoints)

                maybeLeader match {
                    // 分区 leader 副本不可用
                    case None =>
                        debug(s"Error while fetching metadata for $topicPartition: leader not available")
                        new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE,
                            partitionId, Node.noNode(), replicaInfo.asJava, java.util.Collections.emptyList())
                    case Some(leader) =>
                        // 获取分区的 ISR 集合
                        val isr = leaderAndIsr.isr
                        // 获取 ISR 集合中可用的副本对应的节点信息
                        val isrInfo = this.getEndpoints(isr, listenerName, errorUnavailableEndpoints)
                        if (replicaInfo.size < replicas.size) {
                            // 如果 AR 集合中存在不可用的副本，则返回 REPLICA_NOT_AVAILABLE 错误
                            debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                                    s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")
                            new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        } else if (isrInfo.size < isr.size) {
                            // 如果 ISR 集合中存在不可用的的副本，则返回 REPLICA_NOT_AVAILABLE 错误
                            debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                                    s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
                            new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        } else {
                            // AR 集合和 ISR 集合中的副本都是可用的
                            new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava, isrInfo.asJava)
                        }
                }
            }
        }
    }

    /**
     * 获取指定 topic 的元数据信息
     *
     * @param topics
     * @param listenerName
     * @param errorUnavailableEndpoints support v0 MetadataResponses
     * @return
     */
    def getTopicMetadata(topics: Set[String],
                         listenerName: ListenerName,
                         errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
        inReadLock(partitionMetadataLock) {
            topics.toSeq.flatMap { topic =>
                // 获取指定 topic 下分区元数据信息，并与 topic 一起构造 topic 元数据对象返回
                this.getPartitionMetadata(topic, listenerName, errorUnavailableEndpoints).map { partitionMetadata =>
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
            // 更新本地缓存的 kafka controller 的 ID
            controllerId = updateMetadataRequest.controllerId match {
                case id if id < 0 => None
                case id => Some(id)
            }

            // 清除本地缓存的集群可用的 broker 节点信息，并由 UpdateMetadataRequest 请求重新构建
            aliveNodes.clear()
            aliveBrokers.clear()
            updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
                // aliveNodes 是一个请求热点，所以这里使用 java.util.HashMap 来提升性能，如果是 scala 2.10 之后可以使用 AnyRefMap 代替
                val nodes = new java.util.HashMap[ListenerName, Node]
                val endPoints = new mutable.ArrayBuffer[EndPoint]
                broker.endPoints.asScala.foreach { ep =>
                    endPoints += EndPoint(ep.host, ep.port, ep.listenerName, ep.securityProtocol)
                    nodes.put(ep.listenerName, new Node(broker.id, ep.host, ep.port))
                }
                aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
                aliveNodes(broker.id) = nodes.asScala
            }

            // 基于 UpdateMetadataRequest 请求更新每个分区的状态信息，并返回需要被移除的分区集合
            val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
            updateMetadataRequest.partitionStates.asScala.foreach {
                case (tp, info) =>
                    val controllerId = updateMetadataRequest.controllerId
                    val controllerEpoch = updateMetadataRequest.controllerEpoch
                    // 如果请求标记对应的 topic 分区需要被删除
                    if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
                        // 删除本地缓存的对应 topic 分区的状态信息
                        this.removePartitionInfo(tp.topic, tp.partition)
                        stateChangeLogger.trace(s"Broker $brokerId deleted partition $tp from metadata cache in response to UpdateMetadata " +
                                s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
                        deletedPartitions += tp
                    } else {
                        // PartitionState -> PartitionStateInfo
                        val partitionInfo = this.partitionStateToPartitionStateInfo(info)
                        // 更新本地缓存的对应 topic 分区的状态信息
                        this.addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
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
