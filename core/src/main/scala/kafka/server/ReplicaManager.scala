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
 * @param info      @FetchDataInfo returned by the @Log read
 * @param hw        high watermark of the local replica
 * @param readSize  amount of data that was read from the log i.e. size of the fetch
 * @param exception Exception if error encountered while reading from the log
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
 * 管理一个 broker 节点上的分区信息
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
class ReplicaManager(val config: KafkaConfig, // 相关配置对象
                     metrics: Metrics,
                     time: Time, // 时间戳工具
                     val zkUtils: ZkUtils, // ZK 工具类
                     scheduler: Scheduler, // 定时任务调度器
                     val logManager: LogManager, // 用于对分区日志数据执行读写操作
                     val isShuttingDown: AtomicBoolean, // 标记 kafka 服务是否正在执行关闭操作
                     quotaManager: ReplicationQuotaManager,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

    /**
     * 记录 kafka controller 的年代信息，当重新选择 controller leader 时会递增该字段，
     * 用于校验来自 controller 的请求的年代信息，防止处理来自老的 controller 的请求
     */
    @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1

    /** 本地 broker 的 ID */
    private val localBrokerId = config.brokerId

    /** 记录当前 broker 管理的所有分区信息，如果不存在则创建 */
    private val allPartitions = new Pool[TopicPartition, Partition](/*Some(tp => new Partition(tp.topic, tp.partition, time, this))*/)

    private val replicaStateChangeLock = new Object

    /** 管理向 leader 副本发送 FetchRequest 请求的 ReplicaFetcherThread 线程 */
    val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)

    /** 标记 highwatermark-checkpoint 定时任务是否已经启动 */
    private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)

    /** 记录每个 log 目录与对应 topic 分区 HW 值的映射关系 */
    val highWatermarkCheckpoints: Predef.Map[String, OffsetCheckpoint] = config.logDirs.map(dir =>
        (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap

    /** 标记 highwatermark-checkpoint 定时任务是否已经启动 */
    private var hwThreadInitialized = false

    this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
    val stateChangeLogger: KafkaController.StateChangeLogger = KafkaController.stateChangeLogger

    /** 记录 ISR 集合发生变化的 topic 分区信息 */
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
        if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
            scheduler.schedule(
                "highwatermark-checkpoint",
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
             * 定期将 ISR 发生变化的分区记录到 ZK，kafka controller 对相应 ZK 路径添加了 Watcher，
             * 当 Watcher 被触发后会向所管理的 broker 节点发送 UpdateMetadataRequest 请求
             */
            if (isrChangeSet.nonEmpty &&
                    // 最后一次有 ISR 集合发生变化的时间距离现在已经超过 5 秒
                    (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now
                            // 上次写入 ZK 的时间距离现在已经超过 1 分钟
                            || lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
                // 将 ISR 集合发生变更的 topic 分区信息记录到 ZK
                ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
                isrChangeSet.clear()
                lastIsrPropagationMs.set(now)
            }
        }
    }

    def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

    /**
     * 尝试执行监听给定 key 的 DelayedProduce 延时任务
     *
     * this can be triggered when:
     * 1. The partition HW has changed (for acks = -1)
     * 2. A follower replica's fetch operation is received (for acks > 1)
     */
    def tryCompleteDelayedProduce(key: DelayedOperationKey) {
        val completed = delayedProducePurgatory.checkAndComplete(key)
        debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
    }

    /**
     * 尝试执行监听给定 key 的 DelayedFetch 延时任务
     *
     * this can be triggered when:
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
        // 定时检测当前 broker 节点管理的每个分区是否需要缩减 ISR 集合，并执行缩减操作
        scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
        // 定时将 ISR 集合发生变化的 topic 分区记录到 ZK
        scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
    }

    def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean): Short = {
        stateChangeLogger.trace(s"Broker $localBrokerId handling stop replica (delete=$deletePartition) for partition $topicPartition")
        val errorCode = Errors.NONE.code
        getPartition(topicPartition) match {
            case Some(_) =>
                // 如果 deletePartition = true，则删除分区对应的副本及其 Log
                if (deletePartition) {
                    // 从本地移除指定的 topic 分区
                    val removedPartition = allPartitions.remove(topicPartition)
                    if (removedPartition != null) {
                        // 删除分区的 Log 文件，并清空本地缓存的相关信息
                        removedPartition.delete() // this will delete the local log
                        val topicHasPartitions = allPartitions.keys.exists(tp => topicPartition.topic == tp.topic)
                        if (!topicHasPartitions)
                            BrokerTopicStats.removeMetrics(topicPartition.topic)
                    }
                }
            // 本地未缓存对应的分区（一般发生在对应的 topic 已经被删除，但是期间 broker 宕机了），直接尝试删除 Log
            case None =>
                if (deletePartition && logManager.getLog(topicPartition).isDefined)
                    logManager.asyncDelete(topicPartition)
                stateChangeLogger.trace(s"Broker $localBrokerId ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
        }
        stateChangeLogger.trace(s"Broker $localBrokerId finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
        errorCode
    }

    /**
     * 当收到 kafka controller 的 StopReplicaRequest 请求时，会关闭指定的副本，
     * 并根据 StopReplicaRequest 中的字段决定是否删除副本对应的 Log。
     *
     * @param stopReplicaRequest
     * @return
     */
    def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
        replicaStateChangeLock synchronized {
            val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
            // 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
            if (stopReplicaRequest.controllerEpoch() < controllerEpoch) {
                stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
                        .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
                (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
            } else {
                val partitions = stopReplicaRequest.partitions.asScala
                // 更新本地记录的 kafka controller leader 的年代信息
                controllerEpoch = stopReplicaRequest.controllerEpoch
                // 停止对指定分区的数据同步 fetcher 线程
                replicaFetcherManager.removeFetcherForPartitions(partitions)
                for (topicPartition <- partitions) {
                    // 关闭指定分区的副本
                    val errorCode = this.stopReplica(topicPartition, stopReplicaRequest.deletePartitions())
                    responseMap.put(topicPartition, errorCode)
                }
                (responseMap, Errors.NONE.code)
            }
        }
    }

    def getOrCreatePartition(topicPartition: TopicPartition): Partition = allPartitions.getAndMaybePut(topicPartition)

    def getPartition(topicPartition: TopicPartition): Option[Partition] = Option(allPartitions.get(topicPartition))

    /**
     * 获取指定分区位于当前 broker 节点上的本地副本对象，如果不存在则抛出异常
     *
     * @param topicPartition
     * @return
     */
    def getReplicaOrException(topicPartition: TopicPartition): Replica = {
        getReplica(topicPartition).getOrElse {
            throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
        }
    }

    /**
     * 如果指定分区的 leader 副本位于当前 broker 上，则返回该副本对象
     *
     * @param topicPartition
     * @return
     */
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

    /**
     * 获取指定分区的指定副本对象
     *
     * @param topicPartition
     * @param replicaId
     * @return
     */
    def getReplica(topicPartition: TopicPartition, replicaId: Int = localBrokerId): Option[Replica] =
        getPartition(topicPartition).flatMap(_.getReplica(replicaId))

    /**
     * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
     * the callback function will be triggered either when timeout or the required acks are satisfied
     *
     * 追加消息到 leader 副本，并依据 acks 参数等待指定数量的 follower 副本复制
     */
    def appendRecords(timeout: Long,
                      requiredAcks: Short,
                      internalTopicsAllowed: Boolean,
                      entriesPerPartition: Map[TopicPartition, MemoryRecords],
                      responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {

        // 如果 acks 参数合法
        if (this.isValidRequiredAcks(requiredAcks)) {
            val sTime = time.milliseconds
            // 将消息追加到 Log 对象中
            val localProduceResults = this.appendToLocalLog(internalTopicsAllowed, entriesPerPartition, requiredAcks)
            debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

            // 封装数据追加结果
            val produceStatus = localProduceResults.map { case (topicPartition, result) =>
                topicPartition -> ProducePartitionStatus(
                    result.info.lastOffset + 1, // 下一次请求日志的 offset
                    new PartitionResponse(result.error, result.info.firstOffset, result.info.logAppendTime)) // response status
            }

            // 如果需要生成 DelayedProduce 延时任务
            if (this.delayedRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
                // 创建 DelayedProduce 延时任务对象，将回调响应函数封装到延时任务对象中
                val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
                val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

                // 创建当前延时任务监听的一系列 key 对象，监听本次追加操作的所有 topic 分区
                val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

                // 尝试执行延时任务，如果还未到期则将任务交由炼狱管理
                delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
            } else {
                // 无需生成 DelayedProduce 延时任务，立即响应
                val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
                responseCallback(produceResponseStatus)
            }
        } else {
            // 对应的 acks 参数错误，构造 INVALID_REQUIRED_ACKS 响应
            val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
                topicPartition -> new PartitionResponse(
                    Errors.INVALID_REQUIRED_ACKS, LogAppendInfo.UnknownLogAppendInfo.firstOffset, Record.NO_TIMESTAMP)
            }
            // 回调响应
            responseCallback(responseStatus)
        }
    }

    /**
     * If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
     *
     * 1. required acks = -1
     * 2. there is data to append
     * 3. at least one partition append was successful (fewer errors than partitions)
     *
     * @param requiredAcks
     * @param entriesPerPartition
     * @param localProduceResults
     * @return
     */
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
                                 entriesPerPartition: Map[TopicPartition, MemoryRecords], // 对应分区需要追加的消息数据
                                 requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
        trace("Append [%s] to local log ".format(entriesPerPartition))
        // 遍历处理每个分区及其待追加的消息数据
        entriesPerPartition.map {
            case (topicPartition, records) =>
                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
                BrokerTopicStats.getBrokerAllTopicsStats.totalProduceRequestRate.mark()

                // 如果追加的对象是内部 topic，依据参数 internalTopicsAllowed 决定是否追加
                if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
                    (topicPartition, LogAppendResult(
                        LogAppendInfo.UnknownLogAppendInfo,
                        Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
                } else {
                    try {
                        // 获取 topic 分区对应的 Partition 对象
                        val partitionOpt = this.getPartition(topicPartition)
                        val info = partitionOpt match {
                            // 往 leader 副本对应的 Log 追加消息数据
                            case Some(partition) => partition.appendRecordsToLeader(records, requiredAcks)
                            // 找不到 topic 分区对应的 Partition 对象
                            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d".format(topicPartition, localBrokerId))
                        }

                        val numAppendedMessages =
                            if (info.firstOffset == -1L || info.lastOffset == -1L) 0 else info.lastOffset - info.firstOffset + 1

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
        // 标记是否是来自 follower 的 fetch 操作
        val isFromFollower = replicaId >= 0
        // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
        val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
        // 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
        val fetchOnlyCommitted: Boolean = !Request.isValidBrokerId(replicaId)

        // 读取指定位置和大小的消息数据
        val logReadResults = this.readFromLocalLog(
            replicaId = replicaId,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            readOnlyCommitted = fetchOnlyCommitted,
            fetchMaxBytes = fetchMaxBytes,
            hardMaxBytesLimit = hardMaxBytesLimit,
            readPartitionInfo = fetchInfos,
            quota = quota)

        // 如果当前是来自 follower 的同步消息数据请求，则更新 follower 副本的状态，
        // 并尝试扩张 ISR 集合，同时尝试触发监听对应 topic 分区的 DelayedProduce 延时任务
        if (Request.isValidBrokerId(replicaId))
            this.updateFollowerLogReadResults(replicaId, logReadResults)

        val logReadResultValues = logReadResults.map { case (_, v) => v }
        val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
        val errorReadingData = logReadResultValues.foldLeft(false)(
            (errorIncurred, readResult) => errorIncurred || (readResult.error != Errors.NONE))

        /*
         * respond immediately if:
         * 1) fetch request does not want to wait
         * 2) fetch request does not require any data
         * 3) has enough data to respond
         * 4) some error happens while reading data
         */
        if (timeout <= 0 // 请求希望立即响应
                || fetchInfos.isEmpty // 请求不期望有响应数据
                || bytesReadable >= fetchMinBytes // 已经有足够的数据可以响应
                || errorReadingData) { // 读取数据出现错误
            val fetchPartitionData = logReadResults.map { case (tp, result) =>
                tp -> FetchPartitionData(result.error, result.hw, result.info.records)
            }
            // 立即响应
            responseCallback(fetchPartitionData)
        } else {
            // 构造 DelayedFetch 延时任务
            val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
                val fetchInfo = fetchInfos.collectFirst {
                    case (tp, v) if tp == topicPartition => v
                }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
                (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
            }
            val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
                fetchOnlyFromLeader, fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
            val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, responseCallback)

            // 构造延时任务关注的 key，即相应的 topic 分区对象
            val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

            // 交由炼狱管理
            delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
        }
    }

    /**
     * Read from multiple topic partitions at the given offset up to maxSize bytes
     */
    def readFromLocalLog(replicaId: Int, // 副本 ID
                         fetchOnlyFromLeader: Boolean, // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
                         readOnlyCommitted: Boolean, // 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
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

                // 获取待读取消息的副本对象，一般都是从本地副本读取
                val localReplica = if (fetchOnlyFromLeader) getLeaderReplicaIfLocal(tp) else getReplicaOrException(tp)
                // 决定读取消息的 offset 上界，如果是来自消费者的请求，则上界为 HW，如果是来自 follower 的请求，则上界为 LEO
                val maxOffsetOpt = if (readOnlyCommitted) Some(localReplica.highWatermark.messageOffset) else None

                /*
                 * Read the LogOffsetMetadata prior to performing the read from the log.
                 * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
                 * Using the log end offset after performing the read can lead to a race condition
                 * where data gets appended to the log immediately after the replica has consumed from it
                 * This can cause a replica to always be out of sync.
                 */
                val initialLogEndOffset = localReplica.logEndOffset.messageOffset // LEO
                val initialHighWatermark = localReplica.highWatermark.messageOffset // HW
                val fetchTimeMs = time.milliseconds
                val logReadInfo = localReplica.log match {
                    case Some(log) =>
                        val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)
                        // 从 Log 中读取消息数据
                        val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage)

                        // If the partition is being throttled, simply return an empty set.
                        if (shouldLeaderThrottle(quota, tp, replicaId))
                            FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
                        // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
                        // progress in such cases and don't need to report a `RecordTooLargeException`
                        else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
                                 FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
                        else fetch

                    // 对应副本的 Log 对象不存在
                    case None =>
                        error(s"Leader for partition $tp does not have a local log")
                        FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
                }

                // 封装结果返回
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
        // 遍历读取每个 topic 分区的消息数据
        readPartitionInfo.foreach {
            case (tp, fetchInfo) =>
                val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
                val messageSetSize = readResult.info.records.sizeInBytes
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

    /**
     * 更新所有分区的状态信息，并返回需要被移除的分区集合
     *
     * @param correlationId
     * @param updateMetadataRequest
     * @param metadataCache
     * @return
     */
    def maybeUpdateMetadataCache(correlationId: Int,
                                 updateMetadataRequest: UpdateMetadataRequest,
                                 metadataCache: MetadataCache): Seq[TopicPartition] = {
        replicaStateChangeLock synchronized {
            // 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
            if (updateMetadataRequest.controllerEpoch < controllerEpoch) {
                val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
                        "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
                    correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch, controllerEpoch)
                stateChangeLogger.warn(stateControllerEpochErrorMessage)
                throw new ControllerMovedException(stateControllerEpochErrorMessage)
            } else {
                // 更新所有分区的状态信息，并返回需要被移除的分区集合
                val deletedPartitions = metadataCache.updateCache(correlationId, updateMetadataRequest)
                // 更新本地缓存的 controller 的年代信息
                controllerEpoch = updateMetadataRequest.controllerEpoch
                deletedPartitions
            }
        }
    }

    /**
     * 指导指定副本的角色转换
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
            val responseMap = new mutable.HashMap[TopicPartition, Short] // 记录每个分区角色切换操作的状态码
            // 校验 controller 的年代信息，避免处理来自已经过期的 controller 的请求
            if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
                stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
                        "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId, correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
                BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
            } else {
                val controllerId = leaderAndISRRequest.controllerId
                // 1. 更新本地缓存的 kafka controller leader 的年代信息
                controllerEpoch = leaderAndISRRequest.controllerEpoch

                // 2. 校验请求的 leader 副本的年代信息，以及是否由当前 broker 管理，将满足条件的分区信息记录到 partitionState 集合中
                val partitionState = new mutable.HashMap[Partition, PartitionState]()
                leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
                    // 获取/创建指定 topic 分区的 Partition 对象
                    val partition = this.getOrCreatePartition(topicPartition)
                    // 获取 leader 副本的年代信息
                    val partitionLeaderEpoch = partition.getLeaderEpoch
                    // 校验 leader 副本的年代信息，需要保证请求中的 leader 副本的年代信息大于本地缓存的 topic 分区 leader 副本的年代信息
                    if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
                        // 如果请求的分区副本位于当前 broker 节点上，记录到 partitionState 集合中
                        if (stateInfo.replicas.contains(localBrokerId))
                            partitionState.put(partition, stateInfo)
                        else {
                            // 请求的分区副本不在当前 broker 节点上，响应 UNKNOWN_TOPIC_OR_PARTITION 错误
                            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                                    "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                                    .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                                        topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
                            responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                        }
                    } else {
                        // 请求中的 leader 副本的年代信息小于等于本地记录的对应 topic 分区 leader 副本的年代信息，响应 STALE_CONTROLLER_EPOCH 错误
                        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                                "epoch %d for partition [%s,%d] since its associated leader epoch %d is not higher than the current leader epoch %d")
                                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                                    topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
                        responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
                    }
                }

                // 3. 将请求对象中的分区集合分割成 leader 和 follower 两类，并执行角色切换
                val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) => stateInfo.leader == localBrokerId }
                val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys

                // 3.1 将指定分区的副本切换成 leader 角色
                val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
                                                 this.makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
                                             else
                                                 Set.empty[Partition]

                // 3.2 将指定分区的副本切换成 follower 角色
                val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
                                                   this.makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
                                               else
                                                   Set.empty[Partition]

                // 4. 如果 highwatermark-checkpoint 定时任务尚未启动，则执行启动
                if (!hwThreadInitialized) {
                    // 启动 highwatermark-checkpoint 定时任务
                    this.startHighWaterMarksCheckPointThread()
                    hwThreadInitialized = true
                }

                // 5. 关闭空闲的 fetcher 线程
                replicaFetcherManager.shutdownIdleFetcherThreads()

                // 6. 执行回调函数，完成 GroupCoordinator 的迁移操作
                onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

                // 7. 封装结果对象返回
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
                            partitionState: Map[Partition, PartitionState], // 记录需要切换成 leader 角色的分区副本信息
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {

        partitionState.keys.foreach { partition =>
            stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                    "starting the become-leader transition for partition %s")
                    .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
        }

        // 初始化每个 topic 分区的错误码为 NONE
        for (partition <- partitionState.keys)
            responseMap.put(partition.topicPartition, Errors.NONE.code)

        val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()
        try {
            // 如果对应的副本当前是 follower 角色，需要要先停止这些副本的消息同步工作
            replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))

            // 遍历处理 partitionState 集合，将其中记录的分区转换成 leader 角色
            partitionState.foreach {
                case (partition, partitionStateInfo) =>
                    // 调用 Partition#makeLeader 方法，将分区的本地副本切换成 leader 角色
                    if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
                        partitionsToMakeLeaders += partition // 记录成功完成 leader 角色切换的副本对应的分区
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

        // 初始化每个 topic 分区的错误码为 NONE
        for (partition <- partitionState.keys)
            responseMap.put(partition.topicPartition, Errors.NONE.code)

        val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
        try {
            // TODO: Delete leaders from LeaderAndIsrRequest
            partitionState.foreach {
                case (partition, partitionStateInfo) =>
                    // 检测 leader 副本所在的 broker 是否可用
                    val newLeaderBrokerId = partitionStateInfo.leader
                    metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
                        // 仅对 leader 副本所在 broker 节点可用的副本执行角色切换
                        case Some(_) =>
                            // 调用 Partition#makeFollower 方法，将分区的本地副本切换成 follower 角色
                            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
                                partitionsToMakeFollower += partition // // 记录成功完成 follower 角色切换的副本对应的分区
                            else
                                stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                                        "controller %d epoch %d for partition %s since the new leader %d is the same as the old leader")
                                        .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch, partition.topicPartition, newLeaderBrokerId))
                        // 对应 leader 副本所在的 broker 节点失效
                        case None =>
                            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
                                    " %d epoch %d for partition %s but cannot become follower since the new leader %d is unavailable.")
                                    .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch, partition.topicPartition, newLeaderBrokerId))
                            // 即使 leader 副本所在的 broker 不可用，也要创建本地副本对象，主要是为了在 checkpoint 文件中记录此分区的 HW 值
                            partition.getOrCreateReplica()
                    }
            }

            // 停止与旧的 leader 副本同步的 fetcher 线程
            replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
            partitionsToMakeFollower.foreach { partition =>
                stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
                        "%d epoch %d with correlation id %d for partition %s")
                        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
            }

            /*
             * 由于 leader 副本发生变化，所以新旧 leader 在 [HW, LEO] 之间的消息可能不一致，
             * 但是 HW 之前的消息是一致的，所以将 Log 截断到 HW 位置，可能会出现 unclean leader election 的场景
             */
            logManager.truncateTo(partitionsToMakeFollower.map { partition =>
                (partition.topicPartition, partition.getOrCreateReplica().highWatermark.messageOffset)
            }.toMap)

            // 尝试完成监听对应分区的 DelayedProduce 和 DelayedFetch 延时任务
            partitionsToMakeFollower.foreach { partition =>
                val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
                this.tryCompleteDelayedProduce(topicPartitionOperationKey)
                this.tryCompleteDelayedFetch(topicPartitionOperationKey)
            }

            partitionsToMakeFollower.foreach { partition =>
                stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition %s as part of " +
                        "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId, partition.topicPartition, correlationId, controllerId, epoch))
            }

            // 检测 ReplicaManager 的运行状态
            if (isShuttingDown.get()) {
                partitionsToMakeFollower.foreach { partition =>
                    stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
                            "controller %d epoch %d for partition %s since it is shutting down").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
                }
            }
            // 重新启用与新 leader 副本同步的 fetcher 线程
            else {
                val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
                    partition.topicPartition -> BrokerAndInitialOffset(
                        metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerListenerName),
                        partition.getReplica().get.logEndOffset.messageOffset)).toMap
                // 为需要同步的分区创建并启动同步线程，从指定的 offset 开始与 leader 副本进行同步
                replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

                partitionsToMakeFollower.foreach { partition =>
                    stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
                            "%d epoch %d with correlation id %d for partition %s")
                            .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
                }
            }
        } catch {
            case e: Throwable =>
                val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
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

    /**
     * 遍历当前 broker 节点管理的分区，并尝试对分区 ISR 集合执行缩减操作
     */
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
        // 遍历处理对应 topic 分区的日志数据读取结果
        readResults.foreach {
            case (topicPartition, readResult) =>
                getPartition(topicPartition) match {
                    case Some(partition) =>
                        // 更新指定 follower 副本的状态，并尝试扩张对应分区的 ISR 集合，以及后移 leader 副本的 HW 值
                        partition.updateReplicaLogReadResult(replicaId, readResult)
                        // 尝试执行 DelayedProduce 延时任务，因为此时对应 topic 分区下已经有新的消息成功写入
                        this.tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicPartition))
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
        // 获取所有分区全部的本地副本 Replica 对象
        val replicas = allPartitions.values.flatMap(_.getReplica(localBrokerId))
        // 按照副本所在的 log 目录进行分组
        val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
        // 遍历将位于相同 log 目录下的分区 HW 值，写入到对应的 replication-offset-checkpoint 文件中
        for ((dir, reps) <- replicasByDir) {
            // 获取每个 topic 分区对应的 HW 值
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
