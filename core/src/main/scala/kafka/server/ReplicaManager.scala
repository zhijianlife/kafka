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

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.{Gauge, Meter}
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{Log, LogAppendInfo, LogManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ControllerMovedException => _, NotLeaderForPartitionException => _, OffsetOutOfRangeException => _, ReplicaNotAvailableException => _, UnknownTopicOrPartitionException => _, _}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, PartitionState, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
    def error: Errors = exception match {
        case None => Errors.NONE
        case Some(e) => Errors.forException(e)
    }
}

/**
 * Result metadata of a log read operation on the log
 *
 * @param info             @FetchDataInfo returned by the @Log read
 * @param hw               high watermark of the local replica
 * @param readSize         amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error            Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         hw: Long,
                         leaderLogEndOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         exception: Option[Throwable] = None) {

    def error: Errors = exception match {
        case None => Errors.NONE
        case Some(e) => Errors.forException(e)
    }

    override def toString =
        s"Fetch Data: [$info], HW: [$hw], leaderLogEndOffset: [$leaderLogEndOffset], readSize: [$readSize], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE, hw: Long = -1L, records: Records)

object LogReadResult {
    val UnknownLogReadResult = LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
        hw = -1L,
        leaderLogEndOffset = -1L,
        fetchTimeMs = -1L,
        readSize = -1)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Short], errorCode: Short) {

    override def toString: String = {
        "update results: [%s], global error: [%d]".format(responseMap, errorCode)
    }
}

object ReplicaManager {
    val HighWatermarkFilename = "replication-offset-checkpoint"
    val IsrChangePropagationBlackOut = 5000L
    val IsrChangePropagationInterval = 60000L
}

/**
 * 管理一个 broker 上的 partition 信息
 *
 * @param config
 * @param metrics
 * @param time
 * @param zkUtils
 * @param scheduler
 * @param logManager
 * @param isShuttingDown
 * @param quotaManager
 * @param threadNamePrefix
 */
class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManager: ReplicationQuotaManager,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

    /**
     * epoch of the controller that last changed the leader
     *
     * 记录 KafkaController 的年代信息，当重新选择 Controller Leader 时该字段会递增，
     * 当有来自 KafkaController 的请求时，会校验年代信息，防止处理来自老的 KafkaController 的请求
     */
    @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1

    /** 本地 broker ID */
    private val localBrokerId = config.brokerId

    /** 保存了当前 broker 分配的所有分区信息 */
    private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp => new Partition(tp.topic, tp.partition, time, this)))
    private val replicaStateChangeLock = new Object

    /** 管理多个向 leader 副本发送 FetchRequest 请求的 ReplicaFetcherThread */
    val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)

    /** */
    private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)

    /** 缓存 log 目录与 OffsetCheckpoint 之间的对应关系 */
    val highWatermarkCheckpoints: Predef.Map[String, OffsetCheckpoint] = config.logDirs.map(
        dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
    private var hwThreadInitialized = false
    this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
    val stateChangeLogger: KafkaController.StateChangeLogger = KafkaController.stateChangeLogger

    /** 记录 ISR 集合发生变化的分区信息 */
    private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
    private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
    private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

    /** 管理 DelayedProduce 的炼狱 */
    val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce] = DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", localBrokerId, config.producerPurgatoryPurgeIntervalRequests)
    /** 管理 DelayedFetch 的炼狱 */
    val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch] = DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", localBrokerId, config.fetchPurgatoryPurgeIntervalRequests)

    val leaderCount: Gauge[Int] = newGauge(
        "LeaderCount",
        new Gauge[Int] {
            def value: Int = {
                getLeaderPartitions.size
            }
        }
    )
    val partitionCount: Gauge[Int] = newGauge(
        "PartitionCount",
        new Gauge[Int] {
            def value: Int = allPartitions.size
        }
    )
    val underReplicatedPartitions: Gauge[Int] = newGauge(
        "UnderReplicatedPartitions",
        new Gauge[Int] {
            def value: Int = underReplicatedPartitionCount()
        }
    )
    val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
    val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)

    def underReplicatedPartitionCount(): Int = {
        getLeaderPartitions.count(_.isUnderReplicated)
    }

    /**
     * 启动 highwatermark-checkpoint 定时任务，周期性记录每个 Replica 的 HW，
     * 并保存到 log 目录的 replication-offset-checkpoint 文件中
     */
    def startHighWaterMarksCheckPointThread(): Unit = {
        // 保证启动一次
        if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
            scheduler.schedule("highwatermark-checkpoint",
                checkpointHighWatermarks,
                period = config.replicaHighWatermarkCheckpointIntervalMs,
                unit = TimeUnit.MILLISECONDS)
    }

    def recordIsrChange(topicPartition: TopicPartition) {
        isrChangeSet synchronized {
            isrChangeSet += topicPartition
            lastIsrChangeMs.set(System.currentTimeMillis())
        }
    }

    /**
     * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
     * 1. There is ISR change not propagated yet.
     * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
     * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
     * other brokers when large amount of ISR change occurs.
     */
    def maybePropagateIsrChanges() {
        val now = System.currentTimeMillis()
        isrChangeSet synchronized {
            /*
             * isr-change-propagation 任务会定期将 ISR 发生变化的分区记录到 ZK，KafkaController 对相应路径添加了 Watcher，
             * 当 Watcher 被触发后会向管理的 broker 发送 UpdateMetadataRequest，下面的 if 筛选是为防止频繁触发 Watcher
             */
            if (isrChangeSet.nonEmpty &&
                    // 最后一次有 ISR 集合发生变化的时间距离现在已经超过 5 秒
                    (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now
                            // 上次写入 ZK 的时间距离现在已经超过 1 分钟
                            || lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
                // 将 isrChangeSet 写 ZK
                ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
                isrChangeSet.clear()
                lastIsrPropagationMs.set(now)
            }
        }
    }

    def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

    /**
     * Try to complete some delayed produce requests with the request key;
     * this can be triggered when:
     *
     * 1. The partition HW has changed (for acks = -1)
     * 2. A follower replica's fetch operation is received (for acks > 1)
     */
    def tryCompleteDelayedProduce(key: DelayedOperationKey) {
        val completed = delayedProducePurgatory.checkAndComplete(key)
        debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
    }

    /**
     * Try to complete some delayed fetch requests with the request key;
     * this can be triggered when:
     *
     * 1. The partition HW has changed (for regular fetch)
     * 2. A new message set is appended to the local log (for follower fetch)
     */
    def tryCompleteDelayedFetch(key: DelayedOperationKey) {
        val completed = delayedFetchPurgatory.checkAndComplete(key)
        debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
    }

    def startup() {
        // start ISR expiration thread
        // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
        // 周期性调用 maybeShrinkIsr 检测每个分区是否需要缩减 ISR 集合
        scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
        // 周期性将 ISR 集合发生变化的分区记录到 ZK
        scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
    }

    def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean): Short = {
        stateChangeLogger.trace(s"Broker $localBrokerId handling stop replica (delete=$deletePartition) for partition $topicPartition")
        val errorCode = Errors.NONE.code
        getPartition(topicPartition) match {
            case Some(_) =>
                // 删除分区对应的副本及其 Log
                if (deletePartition) {
                    val removedPartition = allPartitions.remove(topicPartition)
                    if (removedPartition != null) {
                        // 删除副本
                        removedPartition.delete() // this will delete the local log
                        val topicHasPartitions = allPartitions.keys.exists(tp => topicPartition.topic == tp.topic)
                        if (!topicHasPartitions)
                            BrokerTopicStats.removeMetrics(topicPartition.topic)
                    }
                }
            // allPartitions 中不存在对应的分区，直接尝试删除 Log
            case None =>
                // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
                // This could happen when topic is being deleted while broker is down and recovers.
                if (deletePartition && logManager.getLog(topicPartition).isDefined)
                    logManager.asyncDelete(topicPartition)
                stateChangeLogger.trace(s"Broker $localBrokerId ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
        }
        stateChangeLogger.trace(s"Broker $localBrokerId finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
        errorCode
    }

    /**
     * 当收到 KafkaController 的 StopReplicaRequest 请求时，会关闭指定的副本，
     * 并根据 StopReplicaRequest 中的字段决定是否删除副本对应的 Log。
     *
     * @param stopReplicaRequest
     * @return
     */
    def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
        replicaStateChangeLock synchronized {
            val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
            // 校验请求中的 controllerEpoch 信息
            if (stopReplicaRequest.controllerEpoch() < controllerEpoch) {
                stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
                        .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
                (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
            } else {
                val partitions = stopReplicaRequest.partitions.asScala
                controllerEpoch = stopReplicaRequest.controllerEpoch
                // First stop fetchers for all partitions, then stop the corresponding replicas
                // 停止对指定分区的 fetch 操作
                replicaFetcherManager.removeFetcherForPartitions(partitions)
                for (topicPartition <- partitions) {
                    // 关闭指定分区的副本
                    val errorCode = stopReplica(topicPartition, stopReplicaRequest.deletePartitions())
                    responseMap.put(topicPartition, errorCode)
                }
                (responseMap, Errors.NONE.code)
            }
        }
    }

    def getOrCreatePartition(topicPartition: TopicPartition): Partition = allPartitions.getAndMaybePut(topicPartition)

    def getPartition(topicPartition: TopicPartition): Option[Partition] = Option(allPartitions.get(topicPartition))

    def getReplicaOrException(topicPartition: TopicPartition): Replica = {
        getReplica(topicPartition).getOrElse {
            throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
        }
    }

    def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica = {
        val partitionOpt = getPartition(topicPartition)
        partitionOpt match {
            case None =>
                throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist on $localBrokerId")
            case Some(partition) =>
                partition.leaderReplicaIfLocal match {
                    case Some(leaderReplica) => leaderReplica
                    case None =>
                        throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
                }
        }
    }

    def getReplica(topicPartition: TopicPartition, replicaId: Int = localBrokerId): Option[Replica] =
        getPartition(topicPartition).flatMap(_.getReplica(replicaId))

    /**
     * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
     * the callback function will be triggered either when timeout or the required acks are satisfied
     */
    def appendRecords(timeout: Long,
                      requiredAcks: Short,
                      internalTopicsAllowed: Boolean,
                      entriesPerPartition: Map[TopicPartition, MemoryRecords],
                      responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {

        if (isValidRequiredAcks(requiredAcks)) {
            val sTime = time.milliseconds
            val localProduceResults = appendToLocalLog(internalTopicsAllowed, entriesPerPartition, requiredAcks)
            debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

            val produceStatus = localProduceResults.map { case (topicPartition, result) =>
                topicPartition ->
                        ProducePartitionStatus(
                            result.info.lastOffset + 1, // required offset
                            new PartitionResponse(result.error, result.info.firstOffset, result.info.logAppendTime)) // response status
            }

            if (delayedRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
                // create delayed produce operation
                val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
                val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

                // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
                val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

                // try to complete the request immediately, otherwise put it into the purgatory
                // this is because while the delayed produce operation is being created, new
                // requests may arrive and hence make this operation completable.
                delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

            } else {
                // we can respond immediately
                val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
                responseCallback(produceResponseStatus)
            }
        } else {
            // If required.acks is outside accepted range, something is wrong with the client
            // Just return an error and don't handle the request at all
            val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
                topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
                    LogAppendInfo.UnknownLogAppendInfo.firstOffset, Record.NO_TIMESTAMP)
            }
            responseCallback(responseStatus)
        }
    }

    // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
    //
    // 1. required acks = -1
    // 2. there is data to append
    // 3. at least one partition append was successful (fewer errors than partitions)
    private def delayedRequestRequired(requiredAcks: Short,
                                       entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                       localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
        requiredAcks == -1 &&
                entriesPerPartition.nonEmpty &&
                localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
    }

    private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
        requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
    }

    /**
     * Append the messages to the local replica logs
     */
    private def appendToLocalLog(internalTopicsAllowed: Boolean,
                                 entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                 requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
        trace("Append [%s] to local log ".format(entriesPerPartition))
        // 需消息进行迭代处理
        entriesPerPartition.map {
            // 处理分区及其对应的消息集合
            case (topicPartition, records) =>
                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
                BrokerTopicStats.getBrokerAllTopicsStats.totalProduceRequestRate.mark()

                // reject appending to internal topics if it is not allowed
                // 如果追加的是内部 topic，依据参数 internalTopicsAllowed 决定是否追加
                if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
                    (topicPartition, LogAppendResult(
                        LogAppendInfo.UnknownLogAppendInfo,
                        Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
                } else {
                    try {
                        // 获取 TP 对应的 Partition 对象
                        val partitionOpt = getPartition(topicPartition)
                        val info = partitionOpt match {
                            // 调用 Partition.appendRecordsToLeader 方法追加消息
                            case Some(partition) => partition.appendRecordsToLeader(records, requiredAcks)
                            // 找不到对应的 Partition 对象
                            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d".format(topicPartition, localBrokerId))
                        }

                        val numAppendedMessages =
                            if (info.firstOffset == -1L || info.lastOffset == -1L)
                                0
                            else
                                info.lastOffset - info.firstOffset + 1

                        // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
                        // 统计追加的消息数量
                        BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
                        BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(records.sizeInBytes)
                        BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
                        BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

                        trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
                                .format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
                        // 返回每个分区写入的消息结果
                        (topicPartition, LogAppendResult(info))
                    } catch {
                        // NOTE: Failed produce requests metric is not incremented for known exceptions
                        // it is supposed to indicate un-expected failures of a broker in handling a produce request
                        case e: KafkaStorageException =>
                            fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
                            Runtime.getRuntime.halt(1)
                            (topicPartition, null)
                        case e@(_: UnknownTopicOrPartitionException |
                                _: NotLeaderForPartitionException |
                                _: RecordTooLargeException |
                                _: RecordBatchTooLargeException |
                                _: CorruptRecordException |
                                _: InvalidTimestampException) =>
                            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
                        case t: Throwable =>
                            BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).failedProduceRequestRate.mark()
                            BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
                            error("Error processing append operation on partition %s".format(topicPartition), t)
                            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
                    }
                }
        }
    }

    /**
     * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
     * the callback function will be triggered either when timeout or required fetch info is satisfied
     */
    def fetchMessages(timeout: Long,
                      replicaId: Int,
                      fetchMinBytes: Int,
                      fetchMaxBytes: Int,
                      hardMaxBytesLimit: Boolean,
                      fetchInfos: Seq[(TopicPartition, PartitionData)],
                      quota: ReplicaQuota = UnboundedQuota,
                      responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit) {
        val isFromFollower = replicaId >= 0
        val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
        val fetchOnlyCommitted: Boolean = !Request.isValidBrokerId(replicaId)

        // read from local logs
        val logReadResults = readFromLocalLog(
            replicaId = replicaId,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            readOnlyCommitted = fetchOnlyCommitted,
            fetchMaxBytes = fetchMaxBytes,
            hardMaxBytesLimit = hardMaxBytesLimit,
            readPartitionInfo = fetchInfos,
            quota = quota)

        // if the fetch comes from the follower,
        // update its corresponding log end offset
        if (Request.isValidBrokerId(replicaId))
            updateFollowerLogReadResults(replicaId, logReadResults)

        // check if this fetch request can be satisfied right away
        val logReadResultValues = logReadResults.map { case (_, v) => v }
        val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
        val errorReadingData = logReadResultValues.foldLeft(false)((errorIncurred, readResult) =>
            errorIncurred || (readResult.error != Errors.NONE))

        // respond immediately if 1) fetch request does not want to wait
        //                        2) fetch request does not require any data
        //                        3) has enough data to respond
        //                        4) some error happens while reading data
        if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
            val fetchPartitionData = logReadResults.map { case (tp, result) =>
                tp -> FetchPartitionData(result.error, result.hw, result.info.records)
            }
            responseCallback(fetchPartitionData)
        } else {
            // construct the fetch results from the read results
            val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
                val fetchInfo = fetchInfos.collectFirst {
                    case (tp, v) if tp == topicPartition => v
                }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
                (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
            }
            val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
                fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
            val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, responseCallback)

            // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
            val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

            // try to complete the request immediately, otherwise put it into the purgatory;
            // this is because while the delayed fetch operation is being created, new requests
            // may arrive and hence make this operation completable.
            delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
        }
    }

    /**
     * Read from multiple topic partitions at the given offset up to maxSize bytes
     */
    def readFromLocalLog(replicaId: Int, //
                         fetchOnlyFromLeader: Boolean, // 是否只读 leader 副本的消息
                         readOnlyCommitted: Boolean, // 是否只读已完成提交的消息（即 HW 之前的消息），如果是消费者的请求则该参数是 true，如果是 follower 则该参数是 false
                         fetchMaxBytes: Int, // 最大 fetch 字节数
                         hardMaxBytesLimit: Boolean,
                         readPartitionInfo: Seq[(TopicPartition, PartitionData)], // 每个分区读取的起始 offset 和最大字节数
                         quota: ReplicaQuota): Seq[(TopicPartition, LogReadResult)] = {

        def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
            val offset = fetchInfo.offset
            val partitionFetchSize = fetchInfo.maxBytes

            BrokerTopicStats.getBrokerTopicStats(tp.topic).totalFetchRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats.totalFetchRequestRate.mark()

            try {
                trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
                        s"remaining response limit $limitBytes" +
                        (if (minOneMessage) s", ignoring response/partition size limits" else ""))

                // decide whether to only fetch from leader
                // 获取要读取消息的副本
                val localReplica = if (fetchOnlyFromLeader)
                                       getLeaderReplicaIfLocal(tp)
                                   else
                                       getReplicaOrException(tp)

                // decide whether to only fetch committed data (i.e. messages below high watermark)
                // 决定读取消息的 offset 上限
                val maxOffsetOpt = if (readOnlyCommitted)
                                       Some(localReplica.highWatermark.messageOffset)
                                   else
                                       None

                /*
                 * Read the LogOffsetMetadata prior to performing the read from the log.
                 * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
                 * Using the log end offset after performing the read can lead to a race condition
                 * where data gets appended to the log immediately after the replica has consumed from it
                 * This can cause a replica to always be out of sync.
                 */
                val initialLogEndOffset = localReplica.logEndOffset.messageOffset
                val initialHighWatermark = localReplica.highWatermark.messageOffset
                val fetchTimeMs = time.milliseconds
                val logReadInfo = localReplica.log match {
                    // 从 Log 中读取消息
                    case Some(log) =>
                        val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

                        // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
                        val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage)

                        // If the partition is being throttled, simply return an empty set.
                        if (shouldLeaderThrottle(quota, tp, replicaId))
                            FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
                        // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
                        // progress in such cases and don't need to report a `RecordTooLargeException`
                        else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
                                 FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
                        else fetch

                    case None =>
                        error(s"Leader for partition $tp does not have a local log")
                        FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
                }

                LogReadResult(info = logReadInfo,
                    hw = initialHighWatermark,
                    leaderLogEndOffset = initialLogEndOffset,
                    fetchTimeMs = fetchTimeMs,
                    readSize = partitionFetchSize,
                    exception = None)
            } catch {
                // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
                // is supposed to indicate un-expected failure of a broker in handling a fetch request
                case e@(_: UnknownTopicOrPartitionException |
                        _: NotLeaderForPartitionException |
                        _: ReplicaNotAvailableException |
                        _: OffsetOutOfRangeException) =>
                    LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        hw = -1L,
                        leaderLogEndOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        exception = Some(e))
                case e: Throwable =>
                    BrokerTopicStats.getBrokerTopicStats(tp.topic).failedFetchRequestRate.mark()
                    BrokerTopicStats.getBrokerAllTopicsStats.failedFetchRequestRate.mark()
                    error(s"Error processing fetch operation on partition $tp, offset $offset", e)
                    LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        hw = -1L,
                        leaderLogEndOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        exception = Some(e))
            }
        } // ~ end of read

        var limitBytes = fetchMaxBytes
        val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
        var minOneMessage = !hardMaxBytesLimit
        readPartitionInfo.foreach {
            case (tp, fetchInfo) =>
                val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
                val messageSetSize = readResult.info.records.sizeInBytes
                // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
                if (messageSetSize > 0) minOneMessage = false
                limitBytes = math.max(0, limitBytes - messageSetSize)
                result += (tp -> readResult)
        }
        result
    }

    /**
     * To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
     * the quota is exceeded and the replica is not in sync.
     */
    def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
        val isReplicaInSync = getPartition(topicPartition).flatMap { partition =>
            partition.getReplica(replicaId).map(partition.inSyncReplicas.contains)
        }.getOrElse(false)
        quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
    }

    def getMagic(topicPartition: TopicPartition): Option[Byte] =
        getReplica(topicPartition).flatMap(_.log.map(_.config.messageFormatVersion.messageFormatVersion))

    def maybeUpdateMetadataCache(correlationId: Int,
                                 updateMetadataRequest: UpdateMetadataRequest,
                                 metadataCache: MetadataCache): Seq[TopicPartition] = {
        replicaStateChangeLock synchronized {
            // 校验 controllerEpoch
            if (updateMetadataRequest.controllerEpoch < controllerEpoch) {
                val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
                        "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
                    correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch, controllerEpoch)
                stateChangeLogger.warn(stateControllerEpochErrorMessage)
                throw new ControllerMovedException(stateControllerEpochErrorMessage)
            } else {
                // 更新 MetadataCache
                val deletedPartitions = metadataCache.updateCache(correlationId, updateMetadataRequest)
                controllerEpoch = updateMetadataRequest.controllerEpoch
                deletedPartitions
            }
        }
    }

    /**
     *
     * @param correlationId
     * @param leaderAndISRRequest
     * @param metadataCache
     * @param onLeadershipChange
     * @return
     */
    def becomeLeaderOrFollower(correlationId: Int,
                               leaderAndISRRequest: LeaderAndIsrRequest,
                               metadataCache: MetadataCache,
                               onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
        leaderAndISRRequest.partitionStates.asScala.foreach {
            case (topicPartition, stateInfo) =>
                stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                        .format(localBrokerId, stateInfo, correlationId,
                            leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
        }
        replicaStateChangeLock synchronized {
            // 记录每个分区角色切换的状态码
            val responseMap = new mutable.HashMap[TopicPartition, Short]
            // 校验 KafkaController 年代信息，忽略来自已经过期的 KafkaController 的请求
            if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
                stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
                        "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId, correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
                BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
            } else {
                val controllerId = leaderAndISRRequest.controllerId
                controllerEpoch = leaderAndISRRequest.controllerEpoch

                // First check partition's leader epoch
                val partitionState = new mutable.HashMap[Partition, PartitionState]()
                leaderAndISRRequest.partitionStates.asScala.foreach {
                    case (topicPartition, stateInfo) =>
                        // 从当前 broker 分配的所有分区信息中寻找 TP 对应的分区信息，没有则创建
                        val partition = getOrCreatePartition(topicPartition)
                        val partitionLeaderEpoch = partition.getLeaderEpoch
                        // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
                        // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
                        // 校验 leader 副本的年代信息
                        if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
                            // 判断该分区是否被分配到了当前 broker
                            if (stateInfo.replicas.contains(localBrokerId))
                            // 保存与当前 broker 相关的分区信息和 PartitionState
                                partitionState.put(partition, stateInfo)
                            else {
                                stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                                        "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                                        .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                                            topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
                                responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                            }
                        } else {
                            // Otherwise record the error code in response
                            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                                    "epoch %d for partition [%s,%d] since its associated leader epoch %d is not higher than the current leader epoch %d")
                                    .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                                        topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
                            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
                        }
                }

                // 依据 PartitionState 指定的角色进行分类
                val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) => stateInfo.leader == localBrokerId }
                val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys

                // 将指定分区的副本切换成 leader 副本
                val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
                                                 makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
                                             else
                                                 Set.empty[Partition]

                // 将指定分区的副本切换成 follower 副本
                val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
                                                   makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
                                               else
                                                   Set.empty[Partition]

                // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
                // have been completely populated before starting the checkpointing there by avoiding weird race conditions
                if (!hwThreadInitialized) {
                    // 启动 highwatermark-checkpoint 任务
                    startHighWaterMarksCheckPointThread()
                    hwThreadInitialized = true
                }

                // 关闭空闲的 fetcher 线程
                replicaFetcherManager.shutdownIdleFetcherThreads()

                // 回调函数
                onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
                BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
            }
        }
    }

    /**
     * Make the current broker to become leader for a given set of partitions by:
     *
     * 1. Stop fetchers for these partitions
     * 2. Update the partition metadata in cache
     * 3. Add these partitions to the leader partitions set
     *
     * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
     * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
     * return the set of partitions that are made leader due to this method
     *
     * TODO: the above may need to be fixed later
     */
    private def makeLeaders(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {

        partitionState.keys.foreach { partition =>
            stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                    "starting the become-leader transition for partition %s")
                    .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        }

        // 初始化每个分区的错误码为 NONE
        for (partition <- partitionState.keys)
            responseMap.put(partition.topicPartition, Errors.NONE.code)

        val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

        try {
            // First stop fetchers for all the partitions
            // 在此 broker 上的副本之前可能是 follower，所以要先暂停对这些副本的 fetch 操作
            replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))

            // Update the partition information to be the leader
            partitionState.foreach {
                case (partition, partitionStateInfo) =>
                    // 调用 Partition.makeLeader 方法，将分区的 Local Replica 切换成 leader 副本
                    if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
                    // 记录成功从其他状态切换成 leader 副本的分区
                        partitionsToMakeLeaders += partition
                    else
                        stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
                                "controller %d epoch %d for partition %s since it is already the leader for the partition.")
                                .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
            }
            partitionsToMakeLeaders.foreach { partition =>
                stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
                        "%d epoch %d with correlation id %d for partition %s")
                        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
            }
        } catch {
            case e: Throwable =>
                partitionState.keys.foreach { partition =>
                    val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
                            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition)
                    stateChangeLogger.error(errorMsg, e)
                }
                // Re-throw the exception for it to be caught in KafkaApis
                throw e
        }

        partitionState.keys.foreach { partition =>
            stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                    "for the become-leader transition for partition %s")
                    .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        }

        partitionsToMakeLeaders
    }

    /**
     * Make the current broker to become follower for a given set of partitions by:
     *
     * 1. Remove these partitions from the leader partitions set.
     * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
     * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
     * 4. Truncate the log and checkpoint offsets for these partitions.
     * 5. Clear the produce and fetch requests in the purgatory
     * 6. If the broker is not shutting down, add the fetcher to the new leaders.
     *
     * The ordering of doing these steps make sure that the replicas in transition will not
     * take any more messages before checkpointing offsets so that all messages before the checkpoint
     * are guaranteed to be flushed to disks
     *
     * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
     * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
     * return the set of partitions that are made follower due to this method
     *
     * 将 Local Replica 切换为 follower 模式
     */
    private def makeFollowers(controllerId: Int,
                              epoch: Int,
                              partitionState: Map[Partition, PartitionState],
                              correlationId: Int,
                              responseMap: mutable.Map[TopicPartition, Short],
                              metadataCache: MetadataCache): Set[Partition] = {
        partitionState.keys.foreach { partition =>
            stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                    "starting the become-follower transition for partition %s")
                    .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        }

        // 将每个分区对应的错误码初始化为 NONE
        for (partition <- partitionState.keys)
            responseMap.put(partition.topicPartition, Errors.NONE.code)

        val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

        try {

            // TODO: Delete leaders from LeaderAndIsrRequest
            partitionState.foreach {
                case (partition, partitionStateInfo) =>
                    // 检测 Leader 所在的 broker 是否存活
                    val newLeaderBrokerId = partitionStateInfo.leader
                    metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
                        // Only change partition state when the leader is available
                        case Some(_) =>
                            // 调用 Partition.makeFollower 方法，将分区的 Local Replica 切换成 follower 副本
                            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
                            // 记录成功从其他状态切换到 follower 副本的分区
                                partitionsToMakeFollower += partition
                            else
                                stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                                        "controller %d epoch %d for partition %s since the new leader %d is the same as the old leader")
                                        .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                                            partition.topicPartition, newLeaderBrokerId))
                        case None =>
                            // The leader broker should always be present in the metadata cache.
                            // If not, we should record the error message and abort the transition process for this partition
                            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
                                    " %d epoch %d for partition %s but cannot become follower since the new leader %d is unavailable.")
                                    .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                                        partition.topicPartition, newLeaderBrokerId))
                            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
                            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
                            // 即使 leader 副本所在的 broker 不可用，也要创建 Local Replica，主要是为了在 checkpoint 文件中记录此分区的 HW
                            partition.getOrCreateReplica()
                    }
            }

            // 停止与旧的 leader 同步的 fetch 线程
            replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
            partitionsToMakeFollower.foreach { partition =>
                stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
                        "%d epoch %d with correlation id %d for partition %s")
                        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
            }

            /*
             * 由于 leader 已经发生变化，所以新旧 leader 在 [HW, LEO] 之间的消息可能不一致，
             * 但是 HW 之前的消息是一致的，所以将 Log 截断到 HW。
             * 可能会出现 Unclean leader election 的场景
             */
            logManager.truncateTo(partitionsToMakeFollower.map { partition =>
                (partition.topicPartition, partition.getOrCreateReplica().highWatermark.messageOffset)
            }.toMap)

            // 尝试完成该分区相关的 DepayedOperation
            partitionsToMakeFollower.foreach { partition =>
                val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
                tryCompleteDelayedProduce(topicPartitionOperationKey)
                tryCompleteDelayedFetch(topicPartitionOperationKey)
            }

            partitionsToMakeFollower.foreach { partition =>
                stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition %s as part of " +
                        "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
                    partition.topicPartition, correlationId, controllerId, epoch))
            }

            // 检测 ReplicaManager 的运行状态
            if (isShuttingDown.get()) {
                partitionsToMakeFollower.foreach { partition =>
                    stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
                            "controller %d epoch %d for partition %s since it is shutting down").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
                }
            }
            // 重新启用与新 leader 副本同步的 Fetcher 线程
            else {
                // we do not need to check if the leader exists again since this has been done at the beginning of this process
                val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
                    partition.topicPartition -> BrokerAndInitialOffset(
                        metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerListenerName),
                        partition.getReplica().get.logEndOffset.messageOffset)).toMap
                replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

                partitionsToMakeFollower.foreach { partition =>
                    stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
                            "%d epoch %d with correlation id %d for partition %s")
                            .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
                }
            }
        } catch {
            case e: Throwable =>
                val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
                        "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
                stateChangeLogger.error(errorMsg, e)
                // Re-throw the exception for it to be caught in KafkaApis
                throw e
        }

        partitionState.keys.foreach { partition =>
            stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                    "for the become-follower transition for partition %s")
                    .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        }

        partitionsToMakeFollower
    }

    private def maybeShrinkIsr(): Unit = {
        trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
        allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
    }

    /**
     * 主要做 4 件事情：
     * 1. 更新 leader 副本上维护的 follower 副本的各项状态
     * 2. 随着 follower 副本不断 fetch 消息，最终追上 leader 副本，可能对 ISR 集合进行扩张，如果 ISR 集合发生了变化，则将新的 ISR 集合记录到 ZK 上
     * 3. 检测是否需要后移 leader 的 HW
     * 4. 检测 DelayedProducePurgatory 中相关 key 对应的 DelayedProduce，满足条件则将其执行完成
     *
     * @param replicaId
     * @param readResults
     */
    private def updateFollowerLogReadResults(replicaId: Int, readResults: Seq[(TopicPartition, LogReadResult)]) {
        debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
        // 遍历 Log 的读取结果
        readResults.foreach {
            case (topicPartition, readResult) =>
                getPartition(topicPartition) match {
                    case Some(partition) =>
                        // 调用 Partition.updateReplicaLogReadResult 方法，更新 follower 副本的状态，以及调用 maybeExpandIsr 方法扩张 ISR 集合
                        partition.updateReplicaLogReadResult(replicaId, readResult)

                        // for producer requests with ack > 1, we need to check
                        // if they can be unblocked after some follower's log end offsets have moved
                        // 尝试执行 DelayedProduce
                        tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicPartition))
                    case None =>
                        warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicPartition))
                }
        }
    }

    private def getLeaderPartitions: List[Partition] = {
        allPartitions.values.filter(_.leaderReplicaIfLocal.isDefined).toList
    }

    def getHighWatermark(topicPartition: TopicPartition): Option[Long] = {
        getPartition(topicPartition).flatMap { partition =>
            partition.leaderReplicaIfLocal.map(_.highWatermark.messageOffset)
        }
    }

    /**
     * Flushes the highwatermark value for all partitions to the highwatermark file
     */
    def checkpointHighWatermarks() {
        // 获取全部的 Replica 对象
        val replicas = allPartitions.values.flatMap(_.getReplica(localBrokerId))
        // 按照副本所在的 log 目录进行分组
        val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
        // 遍历处理
        for ((dir, reps) <- replicasByDir) {
            // 收集当前 log 目录下全部副本的 HW
            val hwms: Map[TopicPartition, Long] = reps.map(r => r.partition.topicPartition -> r.highWatermark.messageOffset).toMap
            try {
                // 更新对应 log 目录下的 replication-offset-checkpoint 文件
                highWatermarkCheckpoints(dir).write(hwms)
            } catch {
                case e: IOException =>
                    fatal("Error writing to highwatermark file: ", e)
                    Runtime.getRuntime.halt(1)
            }
        }
    }

    /**
     * High watermark do not need to be checkpointed only when under unit tests
     *
     * @param checkpointHW
     */
    def shutdown(checkpointHW: Boolean = true) {
        info("Shutting down")
        replicaFetcherManager.shutdown()
        delayedFetchPurgatory.shutdown()
        delayedProducePurgatory.shutdown()
        if (checkpointHW)
            checkpointHighWatermarks()
        info("Shut down completely")
    }
}
