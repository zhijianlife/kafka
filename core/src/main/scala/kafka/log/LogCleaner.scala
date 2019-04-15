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
import java.nio._
import java.util.Date
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.LogEntryFilter
import org.apache.kafka.common.record.{FileRecords, LogEntry, MemoryRecords}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * 负责从 Log 对象中移除过期的消息，这里过期是指两条消息包含相同的 key，对于 offset 较小的消息视为过期
 *
 * The cleaner is responsible for removing obsolete（过期的） records from logs which have the dedupe（重复删除的） retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 *
 * Each log can be thought of being split into two sections of segments:
 * 1. a "clean" section which has previously been cleaned;
 * 2. followed by a "dirty" section that has not yet been cleaned.
 *
 * The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section:
 * 1. The uncleanable section is excluded from cleaning. The active log segment is always uncleanable.
 * 2. If there is a compaction lag time (压缩滞后时间) set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads.
 * Each thread chooses the dirtiest log that has the "dedupe" retention policy and cleans that.
 * The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log.
 * See kafka.log.OffsetMap for details of the implementation of the mapping.
 *
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a 
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner. 
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely.
 * This period of time is configurable on a per-topic basis and is measured from the time the segment enters the clean portion of the log
 * (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 *
 * @param config  Configuration parameters for the cleaner
 * @param logDirs The directories where offset checkpoints reside
 * @param logs    The pool of logs
 * @param time    A way to control the passage of time
 */
class LogCleaner(val config: CleanerConfig, // cleaner 线程相关配置
                 val logDirs: Array[File], // 对应 log.dirs 相关配置
                 val logs: Pool[TopicPartition, Log], // TP 与对应 Log 对象之间的映射关系
                 time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup {

    /** 负责管理每个 Log 的压缩状态，以及维护和更新 cleaner checkpoint 信息 */
    private[log] val cleanerManager = new LogCleanerManager(logDirs, logs)

    /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
    private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond,
        checkIntervalMs = 300,
        throttleDown = true,
        "cleaner-io",
        "bytes",
        time = time)

    /** 管理 CleanerThread 线程 */
    private val cleaners = (0 until config.numThreads).map(new CleanerThread(_))

    /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
    newGauge("max-buffer-utilization-percent",
        new Gauge[Int] {
            def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
        })
    /* a metric to track the recopy rate of each thread's last cleaning */
    newGauge("cleaner-recopy-percent",
        new Gauge[Int] {
            def value: Int = {
                val stats = cleaners.map(_.lastStats)
                val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
                (100 * recopyRate).toInt
            }
        })
    /* a metric to track the maximum cleaning time for the last cleaning from each thread */
    newGauge("max-clean-time-secs",
        new Gauge[Int] {
            def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
        })

    /**
     * Start the background cleaning
     */
    def startup() {
        info("Starting the log cleaner")
        cleaners.foreach(_.start())
    }

    /**
     * Stop the background cleaning
     */
    def shutdown() {
        info("Shutting down the log cleaner.")
        cleaners.foreach(_.shutdown())
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     */
    def abortCleaning(topicPartition: TopicPartition) {
        cleanerManager.abortCleaning(topicPartition)
    }

    /**
     * Update checkpoint file, removing topics and partitions that no longer exist
     */
    def updateCheckpoints(dataDir: File) {
        cleanerManager.updateCheckpoints(dataDir, update = None)
    }

    /**
     * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
     */
    def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
        cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     */
    def abortAndPauseCleaning(topicPartition: TopicPartition) {
        cleanerManager.abortAndPauseCleaning(topicPartition)
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    def resumeCleaning(topicPartition: TopicPartition) {
        cleanerManager.resumeCleaning(topicPartition)
    }

    /**
     * For testing, a way to know when work has completed. This method waits until the
     * cleaner has processed up to the given offset on the specified topic/partition
     *
     * @param topicPartition The topic and partition to be cleaned
     * @param offset         The first dirty offset that the cleaner doesn't have to clean
     * @param maxWaitMs      The maximum time in ms to wait for cleaner
     * @return A boolean indicating whether the work has completed before timeout
     */
    def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
        def isCleaned: Boolean = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)

        var remainingWaitMs = maxWaitMs
        while (!isCleaned && remainingWaitMs > 0) {
            val sleepTime = math.min(100, remainingWaitMs)
            Thread.sleep(sleepTime)
            remainingWaitMs -= sleepTime
        }
        isCleaned
    }

    /**
     * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
     * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
     */
    private class CleanerThread(threadId: Int)
            extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {

        override val loggerName: String = classOf[LogCleaner].getName

        if (config.dedupeBufferSize / config.numThreads > Int.MaxValue)
            warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

        /** 真正执行清理操作的对象 */
        val cleaner = new Cleaner(
            id = threadId,
            offsetMap = new SkimpyOffsetMap(
                memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt, hashAlgorithm = config.hashAlgorithm),
            ioBufferSize = config.ioBufferSize / config.numThreads / 2,
            maxIoBufferSize = config.maxMessageSize,
            dupBufferLoadFactor = config.dedupeBufferLoadFactor,
            throttler = throttler,
            time = time,
            checkDone = checkDone)

        @volatile var lastStats: CleanerStats = new CleanerStats()
        private val backOffWaitLatch = new CountDownLatch(1)

        private def checkDone(topicPartition: TopicPartition) {
            if (!isRunning.get())
                throw new ThreadShutdownException
            cleanerManager.checkCleaningAborted(topicPartition)
        }

        /**
         * The main loop for the cleaner thread
         */
        override def doWork() {
            this.cleanOrSleep()
        }

        override def shutdown(): Unit = {
            initiateShutdown()
            backOffWaitLatch.countDown()
            awaitShutdown()
        }

        /**
         * Clean a log if there is a dirty log available, otherwise sleep for a bit
         */
        private def cleanOrSleep() {
            // 选取下一个最需要进行日志清理的 LogToClean 对象
            val cleaned = cleanerManager.grabFilthiestCompactedLog(time) match {
                // 没有需要被清理的 LogToClean 对象，休息一会后继续尝试
                case None =>
                    false
                // 执行消息清理操作
                case Some(cleanable) =>
                    var endOffset = cleanable.firstDirtyOffset
                    try {
                        // 调用 Cleaner#clean 方法执行清理工作
                        val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
                        recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleanerStats)
                        endOffset = nextDirtyOffset
                    } catch {
                        case _: LogCleaningAbortedException => // task can be aborted, let it go.
                    } finally {
                        // 对 Log 的清理状态进行转换，如果当前 topic 分区的清理状态是 LogCleaningInProgress，则更新 cleaner-offset-checkpoint 文件
                        cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
                    }
                    true
            }

            // 获取所有启用 compact 和 delete 清理策略的 Log 对象，并将其对应的 topic 分区状态设置为 LogCleaningInProgress
            val deletable: Iterable[(TopicPartition, Log)] = cleanerManager.deletableLogs()
            deletable.foreach {
                case (topicPartition, log) =>
                    try {
                        // 对设置了清理策略为 delete 的 LogSegment 执行删除操作，删除过期或过大的 LogSegment 对象。
                        log.deleteOldSegments()
                    } finally {
                        // 移除这些 topic 分区对应的 LogCleaningInProgress 状态
                        cleanerManager.doneDeleting(topicPartition)
                    }
            }

            // 如果没有需要执行清理的 LogToClean 对象，则休息一会后继续重试
            if (!cleaned)
                backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS)
        }

        /**
         * Log out statistics on a single run of the cleaner.
         */
        def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
            this.lastStats = stats

            def mb(bytes: Double): Double = bytes / (1024 * 1024)

            val message =
                "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) +
                        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead),
                            stats.elapsedSecs,
                            mb(stats.bytesRead / stats.elapsedSecs)) +
                        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead),
                            stats.elapsedIndexSecs,
                            mb(stats.mapBytesRead) / stats.elapsedIndexSecs,
                            100 * stats.elapsedIndexSecs / stats.elapsedSecs) +
                        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
                        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead),
                            stats.elapsedSecs - stats.elapsedIndexSecs,
                            mb(stats.bytesRead) / (stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble / stats.elapsedSecs) +
                        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
                        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) +
                        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble / stats.bytesRead),
                            100.0 * (1.0 - stats.messagesWritten.toDouble / stats.messagesRead))
            info(message)
            if (stats.invalidMessagesRead > 0) {
                warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
            }
        }

    }

}

/**
 * This class holds the actual logic for cleaning a log
 *
 * @param id                  An identifier used for logging
 * @param offsetMap           The map used for deduplication
 * @param ioBufferSize        The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize     The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler           The throttler instance to use for limiting I/O rate.
 * @param time                The time instance
 * @param checkDone           Check if the cleaning for a partition is finished or aborted.
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int, // 指定读写 LogSegment 的 ByteBuffer 大小
                           maxIoBufferSize: Int, // 指定消息的最大长度
                           dupBufferLoadFactor: Double, // 指定 SkimpyOffsetMap 的最大占用比例
                           throttler: Throttler,
                           time: Time,
                           checkDone: TopicPartition => Unit // 检测 Log 的清理状态
                          ) extends Logging {

    override val loggerName: String = classOf[LogCleaner].getName

    this.logIdent = "Cleaner " + id + ": "

    /* buffer used for read i/o */
    private var readBuffer = ByteBuffer.allocate(ioBufferSize)

    /* buffer used for write i/o */
    private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

    require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads")

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned and the statistics for this round of cleaning
     */
    private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
        // 记录消息清理的状态信息
        val stats = new CleanerStats()

        info("Beginning cleaning of log %s.".format(cleanable.log.name))
        val log = cleanable.log // 需要被清理的 Log 对象

        info("Building offset map for %s...".format(cleanable.log.name))
        // 清理操作的 offset 上界
        val upperBoundOffset = cleanable.firstUncleanableOffset

        // 1. 遍历处理待清理区间的 LogSegment，填充 offsetMap 对象，主要记录每个消息 key 及其对应清理区间内的最大 offset
        this.buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats)
        val endOffset = offsetMap.latestOffset + 1
        stats.indexDone()

        // 2. 计算删除标识
        val deleteHorizonMs = log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
            case None => 0L
            case Some(seg) => seg.lastModified - log.config.deleteRetentionMs // delete.retention.ms
        }

        // determine the timestamp up to which the log will be cleaned
        // this is the lower of the last active segment and the compaction lag
        val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)

        // 3. 对 [0, endOffset) 区间的 LogSegment 进行分组，并以组为单位执行清理操作
        info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
        for (group <- this.groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize, cleanable.firstUncleanableOffset))
            this.cleanSegments(log, group, offsetMap, deleteHorizonMs, stats)

        // record buffer utilization
        stats.bufferUtilization = offsetMap.utilization

        stats.allDone()

        (endOffset, stats)
    }

    /**
     * Clean a group of segments into a single replacement segment
     *
     * @param log             The log being cleaned
     * @param segments        The group of segments being cleaned
     * @param map             The offset map to use for cleaning segments
     * @param deleteHorizonMs The time to retain delete tombstones
     * @param stats           Collector for cleaning statistics
     */
    private[log] def cleanSegments(log: Log,
                                   segments: Seq[LogSegment],
                                   map: OffsetMap,
                                   deleteHorizonMs: Long,
                                   stats: CleanerStats) {
        // 创建组内第一个 LogSegment 的 log 文件对应的“.cleaned”文件
        val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix)
        logFile.delete()
        // 创建 index 文件对应的“.cleaned”文件
        val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix)
        // 创建 timeindex 文件对应的“.cleaned”文件
        val timeIndexFile = new File(segments.head.timeIndex.file.getPath + Log.CleanedFileSuffix)
        indexFile.delete()
        timeIndexFile.delete()
        val records = FileRecords.open(logFile, false, log.initFileSize(), log.config.preallocate)
        val index = new OffsetIndex(indexFile, segments.head.baseOffset, segments.head.index.maxIndexSize)
        val timeIndex = new TimeIndex(timeIndexFile, segments.head.baseOffset, segments.head.timeIndex.maxIndexSize)
        // 创建清理后数据对应的 LogSegment 对象
        val cleaned = new LogSegment(records, index, timeIndex,
            segments.head.baseOffset, segments.head.indexIntervalBytes, log.config.randomSegmentJitter, time)

        try {
            // 遍历处理需要清理的 LogSegment，将清理后的数据记录到 cleaned 中
            for (old <- segments) {
                val retainDeletes = old.lastModified > deleteHorizonMs
                info("Cleaning segment %s in log %s (largest timestamp %s) into %s, %s deletes."
                        .format(old.baseOffset, log.name, new Date(old.largestTimestamp), cleaned.baseOffset, if (retainDeletes) "retaining" else "discarding"))
                this.cleanInto(log.topicPartition, old, cleaned, map, retainDeletes, log.config.maxMessageSize, stats)
            }

            // 对 index 文件进行截断，剔除无效的字节
            index.trimToValidSize()

            // 对 timeindex 文件进行截断，剔除无效的字节
            cleaned.onBecomeInactiveSegment()
            timeIndex.trimToValidSize()

            // 将 LogSegment 相关的文件刷盘
            cleaned.flush()

            // update the modification date to retain the last modified date of the original files
            val modified = segments.last.lastModified
            cleaned.lastModified = modified

            // 使用清理后的 LogSegment 替换清理之前的 LogSegment 集合
            info("Swapping in cleaned segment %d for segment(s) %s in log %s.".format(cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))
            log.replaceSegments(cleaned, segments)
        } catch {
            case e: LogCleaningAbortedException =>
                cleaned.delete()
                throw e
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param topicPartition    The topic and partition of the log segment to clean
     * @param source            The dirty log segment
     * @param dest              The cleaned log segment
     * @param map               The key=>offset mapping
     * @param retainDeletes     Should delete tombstones be retained while cleaning this segment
     * @param maxLogMessageSize The maximum message size of the corresponding topic
     * @param stats             Collector for cleaning statistics
     */
    private[log] def cleanInto(topicPartition: TopicPartition, // 当前操作的 Log 对应的 topic 分区对象
                               source: LogSegment, // 需要被清理的 LogSegment
                               dest: LogSegment, // 清理后得到 LogSegment
                               map: OffsetMap, // offsetMap
                               retainDeletes: Boolean, // source.lastModified > deleteHorizonMs，当删除对应的 LogSegment 时，删除标记是否应该被保留
                               maxLogMessageSize: Int,
                               stats: CleanerStats) {

        // 定义消息过滤器
        val logCleanerFilter = new LogEntryFilter {
            def shouldRetain(logEntry: LogEntry): Boolean = shouldRetainMessage(source, map, retainDeletes, logEntry, stats)
        }

        var position = 0
        // 遍历处理待清理的 LogSegment 中的消息
        while (position < source.log.sizeInBytes) {
            // 校验对应 topic 分区的清理状态不为 LogCleaningAborted
            this.checkDone(topicPartition)
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
            readBuffer.clear()
            writeBuffer.clear()

            // 读取消息到 buffer
            source.log.readInto(readBuffer, position)
            val records = MemoryRecords.readableRecords(readBuffer)
            throttler.maybeThrottle(records.sizeInBytes)
            // 对消息进行过滤，对需要保留的消息写入到 buffer 中
            val result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize)
            stats.readMessages(result.messagesRead, result.bytesRead)
            stats.recopyMessages(result.messagesRetained, result.bytesRetained)

            position += result.bytesRead

            // 对于需要保留的消息，将其追加到清理后的 LogSegment 对象中
            val outputBuffer = result.output
            if (outputBuffer.position > 0) {
                outputBuffer.flip()
                val retained = MemoryRecords.readableRecords(outputBuffer)
                dest.append(
                    firstOffset = retained.deepEntries.iterator.next().offset,
                    largestOffset = result.maxOffset,
                    largestTimestamp = result.maxTimestamp,
                    shallowOffsetOfMaxTimestamp = result.shallowOffsetOfMaxTimestamp,
                    records = retained)
                throttler.maybeThrottle(outputBuffer.limit)
            }

            // 如果未能读取一条完整的消息，则需要对 buffer 进行扩容
            if (readBuffer.limit > 0 && result.messagesRead == 0)
                growBuffers(maxLogMessageSize)
        }
        // 对 buffer 进行重置
        this.restoreBuffers()
    }

    /**
     * 判断是否应该保留当前消息
     *
     * @param source
     * @param map
     * @param retainDeletes source.lastModified > deleteHorizonMs，当删除对应的 LogSegment 时，删除标记是否应该被保留
     * @param entry
     * @param stats
     * @return
     */
    private def shouldRetainMessage(source: LogSegment,
                                    map: OffsetMap,
                                    retainDeletes: Boolean,
                                    entry: LogEntry, // 当前处理的消息
                                    stats: CleanerStats): Boolean = {
        // 如果当前消息的 offset 已经超过 offsetMap 中记录的最大 offset，则需要保留
        val pastLatestOffset = entry.offset > map.latestOffset
        if (pastLatestOffset)
            return true

        /*
         * 仅保留同时满足以下条件的消息：
         * 1. 消息必须具备 key，且 key 包含的 offsetMap 中
         * 2. 消息的 offset 要大于等于 offsetMap 中记录的对应的 offset;
         * 3. 如果对应的消息是删除标记，只有在允许保留该标记是才会保留。
         */
        if (entry.record.hasKey) {
            val key = entry.record.key
            val foundOffset = map.get(key)
            val redundant = foundOffset >= 0 && entry.offset < foundOffset
            // source.lastModified <= deleteHorizonMs(dirtyFirstOffset 的最近修改时间 - log.config.deleteRetentionMs)
            val obsoleteDelete = !retainDeletes && entry.record.hasNullValue
            !redundant && !obsoleteDelete
        }
        // 对于不包含 key 的消息则不保留
        else {
            stats.invalidMessage()
            false
        }
    }

    /**
     * Double the I/O buffer capacity
     */
    def growBuffers(maxLogMessageSize: Int) {
        val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize)
        if (readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
            throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxBufferSize))
        val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize)
        info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
        this.readBuffer = ByteBuffer.allocate(newSize)
        this.writeBuffer = ByteBuffer.allocate(newSize)
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    def restoreBuffers() {
        if (this.readBuffer.capacity > this.ioBufferSize)
            this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
        if (this.writeBuffer.capacity > this.ioBufferSize)
            this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
    }

    /**
     * Group the segments in a log into groups totaling less than a given size.
     * the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single destination segment.
     * This prevents segment sizes from shrinking too much.
     *
     * @param segments     The log segments to group
     * @param maxSize      the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @return A list of grouped segments
     */
    private[log] def groupSegmentsBySize(segments: Iterable[LogSegment],
                                         maxSize: Int,
                                         maxIndexSize: Int,
                                         firstUncleanableOffset: Long): List[Seq[LogSegment]] = {
        // 记录分组后的数据
        var grouped = List[List[LogSegment]]()

        // 开始执行分组操作
        var segs = segments.toList
        while (segs.nonEmpty) {
            var group = List(segs.head)
            var logSize = segs.head.size
            var indexSize = segs.head.index.sizeInBytes
            var timeIndexSize = segs.head.timeIndex.sizeInBytes
            segs = segs.tail
            while (segs.nonEmpty &&
                    logSize + segs.head.size <= maxSize &&
                    indexSize + segs.head.index.sizeInBytes <= maxIndexSize &&
                    timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&
                    lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue) {
                group = segs.head :: group
                logSize += segs.head.size
                indexSize += segs.head.index.sizeInBytes
                timeIndexSize += segs.head.timeIndex.sizeInBytes
                segs = segs.tail
            }
            grouped ::= group.reverse
        }
        grouped.reverse
    }

    /**
     * We want to get the last offset in the first log segment in segs.
     * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
     * scanning the segment from the last index entry.
     * Therefore, we estimate the last offset of the first log segment by using
     * the base offset of the next segment in the list.
     * If the next segment doesn't exist, first Uncleanable Offset will be used.
     *
     * @param segs - remaining segments to group.
     * @return The estimated last offset for the first segment in segs
     */
    private def lastOffsetForFirstSegment(segs: List[LogSegment], firstUncleanableOffset: Long): Long = {
        if (segs.size > 1) {
            /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
             * the worst case offset */
            segs(1).baseOffset - 1
        } else {
            //for the last segment in the list, use the first uncleanable offset.
            firstUncleanableOffset - 1
        }
    }

    /**
     * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
     *
     * @param log   The log to use
     * @param start The offset at which dirty messages begin
     * @param end   The ending offset for the map that is being built
     * @param map   The map in which to store the mappings
     * @param stats Collector for cleaning statistics
     */
    private[log] def buildOffsetMap(log: Log,
                                    start: Long, // 清理区间起始 offset
                                    end: Long, // 清理区间结束 offset
                                    map: OffsetMap, // 记录消息 key 及其对应的最大 offset
                                    stats: CleanerStats) {
        map.clear()
        // 获取 [start, end) 之间的 LogSegment 对象，这些对象是本次需要执行清理操作的
        val dirty = log.logSegments(start, end).toBuffer
        info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

        var full = false // 标识 map 是否被填充满了
        for (segment <- dirty if !full) {
            // 检查当前分区的压缩状态，确保不是 LogCleaningAborted 状态
            this.checkDone(log.topicPartition)
            // 处理当前 LogSegment 中的消息集合，以消息的 key 作为 key，以遍历范围内最大 offset 作为 value，填充 offsetMap
            full = this.buildOffsetMapForSegment(log.topicPartition, segment, map, start, log.config.maxMessageSize, stats)
            if (full)
                debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
        }
        info("Offset map for log %s complete.".format(log.name))
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map     The map in which to store the key=>offset mapping
     * @param stats   Collector for cleaning statistics
     * @return If the map was filled whilst loading from this segment
     */
    private def buildOffsetMapForSegment(topicPartition: TopicPartition,
                                         segment: LogSegment,
                                         map: OffsetMap,
                                         start: Long,
                                         maxLogMessageSize: Int,
                                         stats: CleanerStats): Boolean = {
        // 获取清理区间起始 offset 对应的消息物理地址
        var position = segment.index.lookup(start).position
        // 当前 map 的最大容量
        val maxDesiredMapSize = (map.slots * dupBufferLoadFactor).toInt
        // 遍历处理 LogSegment 中的消息
        while (position < segment.log.sizeInBytes) {
            // 再次校验当前分区的状态，确保不是 LogCleaningAborted 状态
            this.checkDone(topicPartition)
            readBuffer.clear()
            // 读取消息集合
            segment.log.readInto(readBuffer, position)
            val records = MemoryRecords.readableRecords(readBuffer)
            throttler.maybeThrottle(records.sizeInBytes)

            val startPosition = position
            // 深层迭代遍历消息集合
            for (entry <- records.deepEntries.asScala) {
                val message = entry.record
                // 仅处理具备 key，且 offset 位于 start 之后的消息
                if (message.hasKey && entry.offset >= start) {
                    if (map.size < maxDesiredMapSize)
                    // 将消息的 key 及其 offset 放入 map 中，这里会覆盖 offset 较小的 key
                        map.put(message.key, entry.offset)
                    else
                    // 标识 map 已满
                        return true
                }
                stats.indexMessagesRead(1)
            }
            val bytesRead = records.validBytes
            // 向前移动地址
            position += bytesRead
            stats.indexBytesRead(bytesRead)
            // 如果 position 未向前移动，则说明未读取到一个完整的消息，需要对 buffer 进行扩容
            if (position == startPosition)
                this.growBuffers(maxLogMessageSize)
        } // ~ end while
        // 重置 buffer
        this.restoreBuffers()
        false
    }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
    val startTime: Long = time.milliseconds
    var mapCompleteTime: Long = -1L
    var endTime: Long = -1L
    var bytesRead: Long = 0L
    var bytesWritten: Long = 0L
    var mapBytesRead: Long = 0L
    var mapMessagesRead: Long = 0L
    var messagesRead: Long = 0L
    var invalidMessagesRead: Long = 0L
    var messagesWritten: Long = 0L
    var bufferUtilization: Double = 0.0d

    def readMessages(messagesRead: Int, bytesRead: Int) {
        this.messagesRead += messagesRead
        this.bytesRead += bytesRead
    }

    def invalidMessage() {
        invalidMessagesRead += 1
    }

    def recopyMessages(messagesWritten: Int, bytesWritten: Int) {
        this.messagesWritten += messagesWritten
        this.bytesWritten += bytesWritten
    }

    def indexMessagesRead(size: Int) {
        mapMessagesRead += size
    }

    def indexBytesRead(size: Int) {
        mapBytesRead += size
    }

    def indexDone() {
        mapCompleteTime = time.milliseconds
    }

    def allDone() {
        endTime = time.milliseconds
    }

    def elapsedSecs: Double = (endTime - startTime) / 1000.0

    def elapsedIndexSecs: Double = (mapCompleteTime - startTime) / 1000.0

}

/**
 * Helper class for a log, its topic/partition, the first cleanable position, and the first uncleanable dirty position
 */
private case class LogToClean(topicPartition: TopicPartition, // Log 文件对应的 TP 对象
                              log: Log,
                              firstDirtyOffset: Long, // 清理区间的起始 offset
                              uncleanableOffset: Long // 清理区间的结束 offset （不包括）
                             ) extends Ordered[LogToClean] {

    val cleanBytes: Long = log.logSegments(-1, firstDirtyOffset).map(_.size).sum
    private[this] val firstUncleanableSegment = log.logSegments(uncleanableOffset, log.activeSegment.baseOffset).headOption.getOrElse(log.activeSegment)
    val firstUncleanableOffset: Long = firstUncleanableSegment.baseOffset
    val cleanableBytes: Long = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset)).map(_.size).sum
    val totalBytes: Long = cleanBytes + cleanableBytes
    val cleanableRatio: Double = cleanableBytes / totalBytes.toDouble

    override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
