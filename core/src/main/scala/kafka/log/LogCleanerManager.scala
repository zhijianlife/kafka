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

package kafka.log

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.common.LogCleaningAbortedException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.OffsetCheckpoint
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.{immutable, mutable}

/**
 * 日志压缩状态
 */
private[log] sealed trait LogCleaningState

private[log] case object LogCleaningInProgress extends LogCleaningState

private[log] case object LogCleaningAborted extends LogCleaningState

private[log] case object LogCleaningPaused extends LogCleaningState

/**
 * Manage the state of each partition being cleaned.
 * If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 * While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 * the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 * While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 * requested to be resumed.
 *
 * 负责管理每个 Log 的压缩状态，以及维护和更新 cleaner checkpoint 信息
 */
private[log] class LogCleanerManager(val logDirs: Array[File],
                                     val logs: Pool[TopicPartition, Log]
                                    ) extends Logging with KafkaMetricsGroup {

    import LogCleanerManager._

    override val loggerName: String = classOf[LogCleaner].getName

    /** 记录每个 TP 分区上次清理工作的 offset */
    private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

    /**
     * the offset checkpoints holding the last cleaned point for each log
     *
     * 维护 data 数据目录与 cleaner-offset-checkpoint 文件之间的映射关系
     */
    private val checkpoints = logDirs.map(
        dir => (dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)))).toMap

    /**
     * 记录正在进行清理操作的 topic 分区的清理状态
     */
    private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

    /**
     * a global lock used to control all access to the in-progress set and the offset checkpoints
     *
     * 保证 checkpoints 集合和 inProgress 集合的线程安全
     */
    private val lock = new ReentrantLock

    /**
     * for coordinating the pausing and the cleaning of a partition
     *
     * 用于线程阻塞等待压缩状态由 LogCleaningAborted 转换成 LogCleaningPaused
     */
    private val pausedCleaningCond = lock.newCondition()

    /* a gauge for tracking the cleanable ratio of the dirtiest log */
    @volatile private var dirtiestLogCleanableRatio = 0.0
    newGauge("max-dirty-percent", new Gauge[Int] {
        def value: Int = (100 * dirtiestLogCleanableRatio).toInt
    })

    /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
    @volatile private var timeOfLastRun: Long = Time.SYSTEM.milliseconds
    newGauge("time-since-last-run-ms", new Gauge[Long] {
        def value: Long = Time.SYSTEM.milliseconds - timeOfLastRun
    })

    /**
     * 读取
     *
     * @return the position processed for all logs.
     */
    def allCleanerCheckpoints: Map[TopicPartition, Long] =
        checkpoints.values.flatMap(_.read()).toMap

    /**
     * Choose the log to clean next and add it to the in-progress set.
     * We recompute this each time from the full set of logs to allow logs
     * to be dynamically added to the pool of logs the log manager maintains.
     *
     * 选取下一个最需要进行日志清理的 LogToClean 对象
     */
    def grabFilthiestCompactedLog(time: Time): Option[LogToClean] = {
        inLock(lock) {
            val now = time.milliseconds
            this.timeOfLastRun = now

            // 读取 log 目录下的 cleaner-offset-checkpoint 文件，获取每个 topic 分区上次清理操作的 offset 边界
            val lastClean = allCleanerCheckpoints
            val dirtyLogs = logs.filter {
                // 过滤掉 cleanup.policy 配置为 delete 的 Log 对象，因为不需要压缩
                case (_, log) => log.config.compact // match logs that are marked as compacted
            }.filterNot {
                // 过滤掉所有正在执行清理工作的 Log 对象
                case (topicPartition, _) => inProgress.contains(topicPartition) // skip any logs already in-progress
            }.map {
                // 将需要被清理的区间封装成 LogToClean 对象
                case (topicPartition, log) => // create a LogToClean instance for each
                    // 计算需要执行清理操作的 offset 区间
                    val (firstDirtyOffset, firstUncleanableDirtyOffset) =
                        LogCleanerManager.cleanableOffsets(log, topicPartition, lastClean, now)
                    // 构建清理区间对应的 LogToClean 对象
                    LogToClean(topicPartition, log, firstDirtyOffset, firstUncleanableDirtyOffset)
            }.filter(ltc => ltc.totalBytes > 0) // 忽略待清理区间数据为空的 LogToClean 对象

            // 获取待清理区间最大的 cleanableRatio 比率
            this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
            // 过滤掉所有 cleanableRatio 小于等于配置值（对应 min.cleanable.dirty.ratio 配置）的 LogToClean 对象
            val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
            if (cleanableLogs.isEmpty) {
                None
            } else {
                // 基于需要清理的数据占比选择最需要执行清理的 LogToClean 对象
                val filthiest = cleanableLogs.max
                // 更新对应 topic 分区的清理状态为 LogCleaningInProgress
                inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
                Some(filthiest)
            }
        }
    }

    /**
     * 获取所有启用 compact 和 delete 清理策略的 Log，并将其对应的 topic 分区状态设置为 LogCleaningInProgress
     */
    def deletableLogs(): Iterable[(TopicPartition, Log)] = {
        inLock(lock) {
            // 获取所有未处于正在清理中，同时标注需要执行 delete 和 compact 的 Log 对象
            val toClean = logs.filter { case (topicPartition, log) =>
                !inProgress.contains(topicPartition) && isCompactAndDelete(log)
            }
            // 将这些 Log 对象对应的 topic 分区状态设置为 LogCleaningInProgress
            toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
            toClean
        }

    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     * This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
     */
    def abortCleaning(topicPartition: TopicPartition) {
        inLock(lock) {
            abortAndPauseCleaning(topicPartition)
            resumeCleaning(topicPartition)
        }
        info(s"The cleaning for partition $topicPartition is aborted")
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     *  1. If the partition is not in progress, mark it as paused.
     *  2. Otherwise, first mark the state of the partition as aborted.
     *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
     * throws a LogCleaningAbortedException to stop the cleaning task.
     *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
     *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
     */
    def abortAndPauseCleaning(topicPartition: TopicPartition) {
        inLock(lock) {
            inProgress.get(topicPartition) match {
                case None =>
                    inProgress.put(topicPartition, LogCleaningPaused)
                case Some(state) =>
                    state match {
                        case LogCleaningInProgress =>
                            inProgress.put(topicPartition, LogCleaningAborted)
                        case s =>
                            throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
                    }
            }
            while (!isCleaningInState(topicPartition, LogCleaningPaused))
                pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
        }
        info(s"The cleaning for partition $topicPartition is aborted and paused")
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    def resumeCleaning(topicPartition: TopicPartition) {
        inLock(lock) {
            inProgress.get(topicPartition) match {
                case None =>
                    throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
                case Some(state) =>
                    state match {
                        case LogCleaningPaused =>
                            inProgress.remove(topicPartition)
                        case s =>
                            throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
                    }
            }
        }
        info(s"Compaction for partition $topicPartition is resumed")
    }

    /**
     * Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
     */
    private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
        inProgress.get(topicPartition) match {
            case None => false
            case Some(state) =>
                if (state == expectedState)
                    true
                else
                    false
        }
    }

    /**
     * Check if the cleaning for a partition is aborted. If so, throw an exception.
     */
    def checkCleaningAborted(topicPartition: TopicPartition) {
        inLock(lock) {
            if (isCleaningInState(topicPartition, LogCleaningAborted))
                throw new LogCleaningAbortedException()
        }
    }

    /**
     * 更新 cleaner-offset-checkpoint 文件
     *
     * @param dataDir
     * @param update
     */
    def updateCheckpoints(dataDir: File, update: Option[(TopicPartition, Long)]) {
        inLock(lock) {
            // 获取指定 log 目录下 cleaner-offset-checkpoint 文件对应的 OffsetCheckpoint 对象
            val checkpoint = checkpoints(dataDir)
            // 对 key 相同的 value 进行覆盖
            val existing = checkpoint.read().filterKeys(logs.keys) ++ update
            // 更新 cleaner-offset-checkpoint 文件
            checkpoint.write(existing)
        }
    }

    def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
        inLock(lock) {
            if (logs.get(topicPartition).config.compact) {
                val checkpoint = checkpoints(dataDir)
                val existing = checkpoint.read()

                if (existing.getOrElse(topicPartition, 0L) > offset)
                    checkpoint.write(existing + (topicPartition -> offset))
            }
        }
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
     */
    def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long) {
        inLock(lock) {
            inProgress(topicPartition) match {
                case LogCleaningInProgress =>
                    updateCheckpoints(dataDir, Option(topicPartition, endOffset))
                    inProgress.remove(topicPartition)
                case LogCleaningAborted =>
                    inProgress.put(topicPartition, LogCleaningPaused)
                    pausedCleaningCond.signalAll()
                case s =>
                    throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
            }
        }
    }

    def doneDeleting(topicPartition: TopicPartition): Unit = {
        inLock(lock) {
            inProgress.remove(topicPartition)
        }
    }
}

private[log] object LogCleanerManager extends Logging {

    def isCompactAndDelete(log: Log): Boolean = {
        log.config.compact && log.config.delete
    }

    /**
     * 计算需要被清理的 offset 区间
     *
     * @param log       the log
     * @param lastClean the map of checkpointed offsets
     * @param now       the current time in milliseconds of the cleaning operation
     * @return the lower (inclusive) and upper (exclusive) offsets
     */
    def cleanableOffsets(log: Log, // 待清理的 Log 对象
                         topicPartition: TopicPartition, // 对应的 topic 分区对象
                         lastClean: immutable.Map[TopicPartition, Long], // 记录每个 topic 分区上一次清理操作的结束 offset
                         now: Long): (Long, Long) = {

        // 获取当前 topic 分区上次清理的 offset，即下一次需要被清理的 Log 的起始 offset
        val lastCleanOffset: Option[Long] = lastClean.get(topicPartition)

        // 获取当前 Log 对象 SkipList 中首个 LogSegment 对应的 baseOffset
        val logStartOffset = log.logSegments.head.baseOffset
        // 计算下一次执行清理操作的起始 offset
        val firstDirtyOffset = {
            // 如果 cleaner-offset-checkpoint 中没有当前 topic 分区的相关记录或记录的 offset 小于 logStartOffset，
            // 则以当前 Log 对象 SkipList 中的起始 logStartOffset 作为下一次需要被清理的起始 offset 位置
            val offset = lastCleanOffset.getOrElse(logStartOffset)
            if (offset < logStartOffset) {
                // don't bother with the warning if compact and delete are enabled.
                if (!isCompactAndDelete(log))
                    warn(s"Resetting first dirty offset to log start offset $logStartOffset since the checkpointed offset $offset is invalid.")
                logStartOffset
            } else {
                offset
            }
        }

        // 获取需要被清理的 LogSegment 对象，即在 firstDirtyOffset 到 activeSegment 之间的 LogSegment 对象集合
        val dirtyNonActiveSegments = log.logSegments(firstDirtyOffset, log.activeSegment.baseOffset)
        // 获取配置的清理滞后时间（对应 min.compaction.lag.ms 配置）
        val compactionLagMs = math.max(log.config.compactionLagMs, 0L)

        // 计算本次不应该被清理的 LogSegment 对应的最小 offset 值
        val firstUncleanableDirtyOffset: Long = Seq(
            // activeSegment 不能执行清理操作，避免竞态条件
            Option(log.activeSegment.baseOffset),

            // 寻找最大消息时间戳距离当前时间戳在清理滞后时间（compactionLagMs）范围内的 LogSegment 对应的最小 offset 值
            if (compactionLagMs > 0) {
                dirtyNonActiveSegments.find { s =>
                    // 如果 LogSegment 的最大消息时间戳距离当前在 compactionLagMs 范围内，则不能执行清理操作
                    val isUncleanable = s.largestTimestamp > now - compactionLagMs
                    debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - compactionLagMs}; is uncleanable=$isUncleanable")
                    isUncleanable
                } map (_.baseOffset)
            } else None
        ).flatten.min

        debug(s"Finding range of cleanable offsets for log=${log.name} topicPartition=$topicPartition. Last clean offset=$lastCleanOffset now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset activeSegment.baseOffset=${log.activeSegment.baseOffset}")

        (firstDirtyOffset, firstUncleanableDirtyOffset)
    }
}
