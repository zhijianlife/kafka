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

import java.net.SocketTimeoutException
import java.util

import kafka.admin.AdminUtils
import kafka.api.{KAFKA_0_10_0_IV0, KAFKA_0_10_1_IV1, KAFKA_0_10_1_IV2, KAFKA_0_9_0}
import kafka.cluster.BrokerEndPoint
import kafka.common.KafkaStorageException
import kafka.log.LogConfig
import kafka.server.ReplicaFetcherThread._
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{AbstractRequest, FetchResponse, ListOffsetRequest, ListOffsetResponse, FetchRequest => JFetchRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.Map

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicationQuotaManager)
        extends AbstractFetcherThread(name = name,
            clientId = name,
            sourceBroker = sourceBroker,
            fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
            isInterruptible = false) {

    type REQ = FetchRequest
    type PD = PartitionData

    private val fetchRequestVersion: Short =
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
        else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
        else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
        else 0
    private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
    private val replicaId = brokerConfig.brokerId
    private val maxWait = brokerConfig.replicaFetchWaitMaxMs
    private val minBytes = brokerConfig.replicaFetchMinBytes
    private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
    private val fetchSize = brokerConfig.replicaFetchMaxBytes

    private def clientId: String = name

    private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

    // we need to include both the broker id and the fetcher id
    // as the metrics tag to avoid metric name conflicts with
    // more than one fetcher thread to the same broker
    private val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
            brokerConfig.interBrokerSecurityProtocol,
            LoginType.SERVER,
            brokerConfig.values,
            brokerConfig.saslMechanismInterBrokerProtocol,
            brokerConfig.saslInterBrokerHandshakeRequestEnable
        )
        val selector = new Selector(
            NetworkReceive.UNLIMITED,
            brokerConfig.connectionsMaxIdleMs,
            metrics,
            time,
            "replica-fetcher",
            Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
            false,
            channelBuilder
        )
        new NetworkClient(
            selector,
            new ManualMetadataUpdater(),
            clientId,
            1,
            0,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            brokerConfig.replicaSocketReceiveBufferBytes,
            brokerConfig.requestTimeoutMs,
            time,
            false
        )
    }

    override def shutdown(): Unit = {
        super.shutdown()
        networkClient.close()
    }

    /**
     * 将 fetch 返回的消息追加到 follower 副本的 Log 中，并更新 follower 副本的 HW
     *
     * @param topicPartition
     * @param fetchOffset
     * @param partitionData
     */
    def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
        try {
            val replica = replicaMgr.getReplica(topicPartition).get
            val records = partitionData.toRecords

            maybeWarnIfOversizedRecords(records, topicPartition)

            if (fetchOffset != replica.logEndOffset.messageOffset)
                throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicPartition, fetchOffset, replica.logEndOffset.messageOffset))
            if (logger.isTraceEnabled)
                trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
                        .format(replica.brokerId, replica.logEndOffset.messageOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))
            // 将消息追加到 Log 中，因为 leader 已经为消息分配了 offset，所以 follower 无需在对消息分配 offset
            replica.log.get.append(records, assignOffsets = false)
            if (logger.isTraceEnabled)
                trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
                        .format(replica.brokerId, replica.logEndOffset.messageOffset, records.sizeInBytes, topicPartition))
            val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
            // for the follower replica, we do not need to keep
            // its segment base offset the physical position,
            // these values will be computed upon making the leader
            // 更新 follower 副本的 HW
            replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
            if (logger.isTraceEnabled)
                trace(s"Follower ${replica.brokerId} set replica high watermark for partition $topicPartition to $followerHighWatermark")
            if (quota.isThrottled(topicPartition))
                quota.record(records.sizeInBytes)
        } catch {
            case e: KafkaStorageException =>
                fatal(s"Disk error while replicating data for $topicPartition", e)
                Runtime.getRuntime.halt(1)
        }
    }

    def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
        // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
        if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
            error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
                    "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
                    "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
                    "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
    }

    /**
     * Handle a partition whose offset is out of range and return a new fetch offset.
     *
     * 处理 follower 副本请求的 offset 超出了 leader 副本的 offset 范围，
     * 可能是超出了 leader 副本的 LEO，也可能是小于 startOffset，前者一般是因为将不在 ISR 集合中的 follower 副本选举成为了 leader 副本
     */
    def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
        val replica = replicaMgr.getReplica(topicPartition).get

        /*
         * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
         * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
         * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
         * and it may discover that the current leader's end offset is behind its own end offset.
         *
         * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
         *
         * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
         *
         * 发送 ListOffsetRequest 请求，获取 leader 副本的 LEO
         */
        val leaderEndOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP, brokerConfig.brokerId)

        // 判断 leader 副本的 LEO 是否落后于 follower 副本
        if (leaderEndOffset < replica.logEndOffset.messageOffset) {
            // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
            // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
            // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
            // 依据配置决定是否需要停机
            if (!LogConfig.fromProps(brokerConfig.originals,
                AdminUtils.fetchEntityConfig(replicaMgr.zkUtils, ConfigType.Topic, topicPartition.topic)).uncleanLeaderElectionEnable) {
                // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.
                fatal("Exiting because log truncation is not allowed for partition %s,".format(topicPartition) +
                        " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
                                .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
                System.exit(1)
            }

            warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
                    .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
            // 将分区对应的 Log 阶段到 leader 副本的 LEO 位置，从该位置开始重新与 leader 进行同步
            replicaMgr.logManager.truncateTo(Map(topicPartition -> leaderEndOffset))
            // 返回下次获取消息的 offset
            leaderEndOffset
        }
        else {
            /*
             * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
             * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
             * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
             * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
             * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
             * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
             * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
             * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
             *
             * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
             * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
             * start offset.
             * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
             * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
             * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
             * brokers and producers.
             *
             * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
             * and the current leader's log start offset.
             *
             * 发送 ListOffsetRequest 请求，获取 leader 副本的 startOffset
             */
            val leaderStartOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP, brokerConfig.brokerId)
            warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
                    .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
            // 选择下次获取消息的起始 offset
            val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
            // Only truncate log when current leader's log start offset is greater than follower's log end offset.
            // 将 Log 全部截断，并创建新的 activeSegment
            if (leaderStartOffset > replica.logEndOffset.messageOffset)
                replicaMgr.logManager.truncateFullyAndStartAt(topicPartition, leaderStartOffset)
            // 返回下次获取消息的 offset
            offsetToFetch
        }
    }

    /**
     * any logic for partitions whose leader has changed
     *
     * 用于处理 OFFSET_OUT_OF_RANGE 错误以外的错误，
     * 将对应分区的同步操作暂停一段时间，暂停发送 FetchRequest 请求
     *
     * @param partitions
     */
    def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
        delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
    }

    protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
        val clientResponse = sendRequest(fetchRequest.underlying)
        val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
        fetchResponse.responseData.asScala.toSeq.map { case (key, value) =>
            key -> new PartitionData(value)
        }
    }

    private def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = {
        import kafka.utils.NetworkClientBlockingOps._
        try {
            // 阻塞等待节点状态变为 ready，超时则抛出异常
            if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
            else {
                val clientRequest = networkClient.newClientRequest(
                    sourceBroker.id.toString, requestBuilder, time.milliseconds(), true)
                // 发送请求并阻塞等到响应
                networkClient.blockingSendAndReceive(clientRequest)(time)
            }
        }
        catch {
            case e: Throwable =>
                networkClient.close(sourceBroker.id.toString)
                throw e
        }

    }

    private def earliestOrLatestOffset(topicPartition: TopicPartition, earliestOrLatest: Long, consumerId: Int): Long = {
        val requestBuilder = if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) {
            val partitions = Map(topicPartition -> (earliestOrLatest: java.lang.Long))
            new ListOffsetRequest.Builder(consumerId).
                    setTargetTimes(partitions.asJava).
                    setVersion(1)
        } else {
            val partitions = Map(topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1))
            new ListOffsetRequest.Builder(consumerId).
                    setOffsetData(partitions.asJava).
                    setVersion(0)
        }
        val clientResponse = sendRequest(requestBuilder)
        val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]
        val partitionData = response.responseData.get(topicPartition)
        Errors.forCode(partitionData.errorCode) match {
            case Errors.NONE =>
                if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
                    partitionData.offset
                else
                    partitionData.offsets.get(0)
            case errorCode => throw errorCode.exception
        }
    }

    protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
        val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]

        partitionMap.foreach {
            case (topicPartition, partitionFetchState) =>
                // We will not include a replica in the fetch request if it should be throttled.
                if (partitionFetchState.isActive && !shouldFollowerThrottle(quota, topicPartition))
                    requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize))
        }

        // 创建 FetchRequest 请求
        val requestBuilder = new JFetchRequest.Builder(maxWait, minBytes, requestMap).
                setReplicaId(replicaId).setMaxBytes(maxBytes)
        requestBuilder.setVersion(fetchRequestVersion)
        new FetchRequest(requestBuilder)
    }

    /**
     * To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
     * the quota is exceeded and the replica is not in sync.
     */
    private def shouldFollowerThrottle(quota: ReplicaQuota, topicPartition: TopicPartition): Boolean = {
        val isReplicaInSync = fetcherLagStats.isReplicaInSync(topicPartition.topic, topicPartition.partition)
        quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
    }
}

object ReplicaFetcherThread {

    private[server] class FetchRequest(val underlying: JFetchRequest.Builder) extends AbstractFetcherThread.FetchRequest {
        def isEmpty: Boolean = underlying.fetchData().isEmpty

        def offset(topicPartition: TopicPartition): Long = underlying.fetchData().asScala(topicPartition).offset
    }

    private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

        def errorCode: Short = underlying.errorCode

        def toRecords: MemoryRecords = {
            underlying.records.asInstanceOf[MemoryRecords]
        }

        def highWatermark: Long = underlying.highWatermark

        def exception: Option[Throwable] = Errors.forCode(errorCode) match {
            case Errors.NONE => None
            case e => Some(e.exception)
        }

    }

}
