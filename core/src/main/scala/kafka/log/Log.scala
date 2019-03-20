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

import java.io.{File, IOException}
import java.text.NumberFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.regex.Pattern

import com.yammer.metrics.core.Gauge
import kafka.api.KAFKA_0_10_0_IV0
import kafka.common._
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InvalidOffsetException => _, OffsetOutOfRangeException => _, _}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection.Seq

object LogAppendInfo {
    val UnknownLogAppendInfo = LogAppendInfo(-1, -1, Record.NO_TIMESTAMP, -1L, Record.NO_TIMESTAMP,
        NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset          The first offset in the message set
 * @param lastOffset           The last offset in the message set
 * @param maxTimestamp         The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime        The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec          The source codec used in the message set (send by the producer)
 * @param targetCodec          The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount         The number of shallow messages
 * @param validBytes           The number of valid bytes
 * @param offsetsMonotonic     Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Long, // 第一条消息的 offset
                         var lastOffset: Long, // 最后一条消息的 offset
                         var maxTimestamp: Long, // 消息中最大的时间戳
                         var offsetOfMaxTimestamp: Long, // 最大时间戳消息对应的 offset
                         var logAppendTime: Long, // 消息追加到 Log 的时间戳
                         sourceCodec: CompressionCodec, // 生产者使用的压缩方式
                         targetCodec: CompressionCodec, // 服务端使用的压缩方式
                         shallowCount: Int, // 外层消息数
                         validBytes: Int, // 验证的字节数
                         offsetsMonotonic: Boolean // 标识生产者为消息分配的内部 offset 是否是单调递增的
                        )

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval for a given segment.
 *
 * 对多个 LogSegment 对象的顺序组合，形成一个逻辑上的日志。使用了 SkipList 对 LogSegment 进行管理，方便快速定位。
 *
 * @param dir           The directory in which log segments are created.
 * @param config        The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler     The thread pool scheduler used for background actions
 * @param time          The time instance used for checking the clock
 *
 */
@threadsafe
class Log(@volatile var dir: File, // 当前 Log 对应的目录，目录中的每个日志和索引文件对应一个 LogSegment
          @volatile var config: LogConfig, // 相关配置信息
          @volatile var recoveryPoint: Long = 0L, // 恢复操作的起始 offset，之前的消息已经全部落盘
          scheduler: Scheduler,
          time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup {

    import kafka.log.Log._

    /** A lock that guards all modifications to the log */
    private val lock = new Object

    /** 最近一次执行 flush 的时间 */
    private val lastflushedTime = new AtomicLong(time.milliseconds)

    def initFileSize(): Int = {
        if (config.preallocate) config.segmentSize else 0
    }

    /**
     * 用于产生分配给当前消息的 offset，也是当前副本的 LogEndOffset:
     * - messageOffset 记录了当前 Log 最后一个 offset 值
     * - segmentBaseOffset 记录了 activeSegment 的 baseOffset
     * - relativePositionInSegment 记录了 activeSegment 的大小
     */
    @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

    /** 当前 Log 包含的 LogSegment 集合，SkipList 结构 */
    private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

    locally {
        val startMs = time.milliseconds

        loadSegments()

        /* Calculate the offset of the next message */
        nextOffsetMetadata = new LogOffsetMetadata(
            activeSegment.nextOffset(), activeSegment.baseOffset, activeSegment.size.toInt)

        info("Completed load of log %s with %d log segments and log end offset %d in %d ms"
                .format(name, segments.size(), logEndOffset, time.milliseconds - startMs))
    }

    val topicPartition: TopicPartition = Log.parseTopicPartitionName(dir)

    private val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)

    newGauge("NumLogSegments",
        new Gauge[Int] {
            def value: Int = numberOfSegments
        },
        tags)

    newGauge("LogStartOffset",
        new Gauge[Long] {
            def value: Long = logStartOffset
        },
        tags)

    newGauge("LogEndOffset",
        new Gauge[Long] {
            def value: Long = logEndOffset
        },
        tags)

    newGauge("Size",
        new Gauge[Long] {
            def value: Long = size
        },
        tags)

    /** The name of this log */
    def name: String = dir.getName

    /**
     * Load the log segments from the log files on disk
     */
    private def loadSegments() {
        // create the log directory if it doesn't exist
        dir.mkdirs()
        var swapFiles = Set[File]()

        // 1. 删除 ".delete" 和 ".cleaned" 文件，将 ".swap" 文件加入到 swap 集合中
        for (file <- dir.listFiles if file.isFile) {
            if (!file.canRead)
                throw new IOException("Could not read file " + file)
            val filename = file.getName
            if (filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
                /*
                 * 删除 ".delete" 和 ".cleaned" 文件
                 * - ".delete" 是指标识需要被删除的日志文件和索引文件
                 * - ".cleaned" 是指在执行日志压缩过程中宕机，其中的数据状态不明确，无法正确恢复
                 */
                file.delete()
            } else if (filename.endsWith(SwapFileSuffix)) {
                /*
                 * 如果是 ".swap" 文件，则说明日志压缩过程完成，但是在执行 swap 过程中宕机，
                 * 这种文件保存了日志压缩后的完整数据，可以进行恢复：
                 *
                 * 1. 如果是 log 文件，则删除 index 文件，稍后会完成 swap 操作
                 * 2. 如果是 index 文件，则直接删除，因为后续可以重建
                 */

                // 移除 swap 后缀
                val baseName = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
                // 如果是 index 文件，则直接删除，因为后续可以重建
                if (baseName.getPath.endsWith(IndexFileSuffix)) {
                    file.delete()
                }
                // 如果是 log 文件，则删除对应的 index 文件
                else if (baseName.getPath.endsWith(LogFileSuffix)) {
                    val index = new File(CoreUtils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix))
                    index.delete()
                    swapFiles += file // 将当前文件加入到 swap 集合中
                }
            }
        }

        // 2. 加载全部的日志文件和索引文件
        for (file <- dir.listFiles if file.isFile) {
            val filename = file.getName
            // 处理 ".index" 和 ".timeindex" 文件
            if (filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix)) {
                // 如果索引文件没有对应的 log 文件，则删除索引文件
                val logFile =
                    if (filename.endsWith(TimeIndexFileSuffix))
                        new File(file.getAbsolutePath.replace(TimeIndexFileSuffix, LogFileSuffix))
                    else
                        new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))
                if (!logFile.exists) {
                    warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
                    file.delete()
                }
            }
            // 处理 ".log" 文件
            else if (filename.endsWith(LogFileSuffix)) {
                // if its a log file, load the corresponding log segment
                val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
                // 创建 ".index" 文件对象
                val indexFile = Log.indexFilename(dir, start)
                // 创建 ".timeindex" 文件对象
                val timeIndexFile = Log.timeIndexFilename(dir, start)

                val indexFileExists = indexFile.exists()
                val timeIndexFileExists = timeIndexFile.exists()
                // 创建对应的 LogSegment 对象
                val segment = new LogSegment(dir = dir,
                    startOffset = start,
                    indexIntervalBytes = config.indexInterval,
                    maxIndexSize = config.maxIndexSize,
                    rollJitterMs = config.randomSegmentJitter,
                    time = time,
                    fileAlreadyExists = true)

                // 如果对应的索引文件存在
                if (indexFileExists) {
                    try {
                        // 校验 index 文件的完整性
                        segment.index.sanityCheck()
                        // Resize the time index file to 0 if it is newly created.
                        if (!timeIndexFileExists)
                            segment.timeIndex.resize(0)
                        segment.timeIndex.sanityCheck()
                    } catch {
                        case e: java.lang.IllegalArgumentException =>
                            warn(s"Found a corrupted index file due to ${e.getMessage}}. deleting ${timeIndexFile.getAbsolutePath}, " + s"${indexFile.getAbsolutePath} and rebuilding index...")
                            indexFile.delete()
                            timeIndexFile.delete()
                            segment.recover(config.maxMessageSize)
                    }
                }
                // 如果对应的索引文件不存在，则重建
                else {
                    error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
                    segment.recover(config.maxMessageSize)
                }
                // 记录 LogSegment 到 segments 集合中
                segments.put(start, segment)
            }
        }

        // 3. 遍历处理步骤 1 中记录的 swap 文件
        for (swapFile <- swapFiles) {
            val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
            val fileName = logFile.getName
            // 基于 log 文件名得到对应的 baseOffset
            val startOffset = fileName.substring(0, fileName.length - LogFileSuffix.length).toLong
            val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
            val index = new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
            val timeIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TimeIndexFileSuffix) + SwapFileSuffix)
            val timeIndex = new TimeIndex(timeIndexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
            // 创建对应的 LogSegment 对象
            val swapSegment = new LogSegment(FileRecords.open(swapFile),
                index = index,
                timeIndex = timeIndex,
                baseOffset = startOffset,
                indexIntervalBytes = config.indexInterval,
                rollJitterMs = config.randomSegmentJitter,
                time = time)
            info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
            // 依据日志文件重建索引文件，同时验证日志文件中的消息的合法性
            swapSegment.recover(config.maxMessageSize)
            // 查找 swapSegment 对应的日志压缩前的 LogSegment 集合
            val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset())
            // 将 swapSegment 对象加入到 segments 中，将 oldSegments 中所有的 LogSegment 对象从 segments 中删除
            // 并删除对应的日志文件和索引文件，最后移除文件的 ".swap" 后缀
            replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
        }

        // 4. 如果 segments 为空，则需要创建一个 activeSegment
        if (logSegments.isEmpty) {
            // no existing segments, create a new mutable segment beginning at offset 0
            segments.put(0L, new LogSegment(dir = dir,
                startOffset = 0,
                indexIntervalBytes = config.indexInterval,
                maxIndexSize = config.maxIndexSize,
                rollJitterMs = config.randomSegmentJitter,
                time = time,
                fileAlreadyExists = false,
                initFileSize = this.initFileSize(),
                preallocate = config.preallocate))
        } else {
            // 如果 segments 不为空
            if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
                // 处理 broker 异常关闭时到的数据异常，需要验证 [recoveryPoint, activeSegment] 中的所有消息，并移除验证失败的消息
                this.recoverLog()
                // reset the index size of the currently active log segment to allow more entries
                activeSegment.index.resize(config.maxIndexSize)
                activeSegment.timeIndex.resize(config.maxIndexSize)
            }
        }
    }

    private def updateLogEndOffset(messageOffset: Long) {
        nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
    }

    private def recoverLog() {
        // if we have the clean shutdown marker, skip recovery
        if (hasCleanShutdownFile) {
            this.recoveryPoint = activeSegment.nextOffset()
            return
        }

        // okay we need to actually recovery this log
        val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
        while (unflushed.hasNext) {
            val curr = unflushed.next
            info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
            val truncatedBytes =
                try {
                    curr.recover(config.maxMessageSize)
                } catch {
                    case _: InvalidOffsetException =>
                        val startOffset = curr.baseOffset
                        warn("Found invalid offset during recovery for log " + dir.getName + ". Deleting the corrupt segment and " +
                                "creating an empty one with starting offset " + startOffset)
                        curr.truncateTo(startOffset)
                }
            if (truncatedBytes > 0) {
                // we had an invalid message, delete all remaining log
                warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
                unflushed.foreach(deleteSegment)
            }
        }
    }

    /**
     * Check if we have the "clean shutdown" file
     */
    private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

    /**
     * The number of segments in the log.
     * Take care! this is an O(n) operation.
     */
    def numberOfSegments: Int = segments.size

    /**
     * Close this log
     */
    def close() {
        debug("Closing log " + name)
        lock synchronized {
            logSegments.foreach(_.close())
        }
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     *
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param records       The log records to append
     * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
     * @throws KafkaStorageException If the append fails due to an I/O error.
     * @return Information about the appended messages including the first and last offset.
     */
    def append(records: MemoryRecords, assignOffsets: Boolean = true): LogAppendInfo = {
        // 解析、校验 records，封装成 LogAppendInfo 对象
        val appendInfo = this.analyzeAndValidateRecords(records)

        // 如果外层消息数为 0，则直接返回
        if (appendInfo.shallowCount == 0)
            return appendInfo

        // 将未通过验证的字节部分剔除
        var validRecords = this.trimInvalidBytes(records, appendInfo)

        try {
            // they are valid, insert them in the log
            lock synchronized {

                // 如果需要分配 offset
                if (assignOffsets) {
                    // 获取当前 Log 最后一个 offset 值，以此开始向后分配 offset
                    val offset = new LongRef(nextOffsetMetadata.messageOffset)
                    appendInfo.firstOffset = offset.value
                    val now = time.milliseconds
                    val validateAndOffsetAssignResult = try {
                        // 进一步验证
                        LogValidator.validateMessagesAndAssignOffsets(
                            validRecords,
                            offset,
                            now,
                            appendInfo.sourceCodec,
                            appendInfo.targetCodec,
                            config.compact,
                            config.messageFormatVersion.messageFormatVersion,
                            config.messageTimestampType,
                            config.messageTimestampDifferenceMaxMs)
                    } catch {
                        case e: IOException =>
                            throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
                    }
                    validRecords = validateAndOffsetAssignResult.validatedRecords
                    appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
                    appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
                    appendInfo.lastOffset = offset.value - 1
                    // 如果时间戳类型为 LOG_APPEND_TIME，则修改时间戳
                    if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
                        appendInfo.logAppendTime = now

                    // 如果在 validateMessagesAndAssignOffsets 中修改了消息的长度，则需要重新验证，防止消息过长
                    if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
                        for (logEntry <- validRecords.shallowEntries.asScala) {
                            if (logEntry.sizeInBytes > config.maxMessageSize) {
                                // we record the original message set size instead of the trimmed size
                                // to be consistent with pre-compression bytesRejectedRate recording
                                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                                throw new RecordTooLargeException(
                                    "Message size is %d bytes which exceeds the maximum configured message size of %s.".format(logEntry.sizeInBytes, config.maxMessageSize))
                            }
                        }
                    }
                }
                // 不需要分配 offset
                else {
                    // we are taking the offsets we are given
                    if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
                        throw new IllegalArgumentException("Out of order offsets found in " + records.deepEntries.asScala.map(_.offset))
                }

                // check messages set size may be exceed config.segmentSize
                if (validRecords.sizeInBytes > config.segmentSize) {
                    throw new RecordBatchTooLargeException(
                        "Message set size is %d bytes which exceeds the maximum configured segment size of %s.".format(validRecords.sizeInBytes, config.segmentSize))
                }

                // 获取或创建 activeSegment
                val segment = this.maybeRoll(
                    messagesSize = validRecords.sizeInBytes,
                    maxTimestampInMessages = appendInfo.maxTimestamp,
                    maxOffsetInMessages = appendInfo.lastOffset)


                // 往 activeSegment 追加消息
                segment.append(firstOffset = appendInfo.firstOffset,
                    largestOffset = appendInfo.lastOffset,
                    largestTimestamp = appendInfo.maxTimestamp,
                    shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
                    records = validRecords)

                // 更新 LEO 中记录的当前 Log 最后一个 offset 值
                this.updateLogEndOffset(appendInfo.lastOffset + 1)

                trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
                        .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validRecords))

                // 如果刷盘时间间隔达到阈值，则执行刷盘
                if (unflushedMessages >= config.flushInterval)
                // 将 [recoveryPoint, logEndOffset) 之间的数据刷盘
                    this.flush()

                appendInfo
            }
        } catch {
            case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
        }
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid
     * </ol>
     *
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private def analyzeAndValidateRecords(records: MemoryRecords): LogAppendInfo = {
        var shallowMessageCount = 0 // 外层消息数
        var validBytesCount = 0 // 通过验证的消息字节数
        var firstOffset = -1L // 第一条消息的 offset
        var lastOffset = -1L // 最后一条消息的 offset
        var sourceCodec: CompressionCodec = NoCompressionCodec // 生产者使用的压缩方式
        var monotonic = true // 标识生产者为消息分配的内部 offset 是否是单调递增的
        var maxTimestamp = Record.NO_TIMESTAMP
        var offsetOfMaxTimestamp = -1L

        // 基于浅层迭代器迭代，对于压缩的消息不会解压缩
        for (entry <- records.shallowEntries.asScala) {
            // 记录第一条消息的 offset
            if (firstOffset < 0)
                firstOffset = entry.offset

            // 如果是单调递增的话，则在遍历过程中 lastOffset 应该使用小于当前的 offset
            if (lastOffset >= entry.offset)
                monotonic = false

            // 记录最后一条消息的 offset
            lastOffset = entry.offset

            val record = entry.record

            // 如果消息的大小大于允许的最大值，则抛出异常
            val messageSize = entry.sizeInBytes
            if (messageSize > config.maxMessageSize) {
                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %s.".format(messageSize, config.maxMessageSize))
            }

            // CRC 校验
            record.ensureValid()

            // 记录当前消息集合中时间戳最大的消息，及其对应的 offset
            if (record.timestamp > maxTimestamp) {
                maxTimestamp = record.timestamp
                offsetOfMaxTimestamp = lastOffset
            }

            // 外层消息数加 1
            shallowMessageCount += 1
            // 更新已验证的字节数
            validBytesCount += messageSize

            // 解析生产者使用的压缩方式
            val messageCodec = CompressionCodec.getCompressionCodec(record.compressionType.id)
            if (messageCodec != NoCompressionCodec)
                sourceCodec = messageCodec
        }

        // 解析服务端使用的压缩方式
        val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)

        // 封装成 LogAppendInfo 对象返回
        LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp,
            Record.NO_TIMESTAMP, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * 剔除未通过验证的字节
     *
     * @param records The records to trim
     * @param info    The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
        val validBytes = info.validBytes
        if (validBytes < 0)
            throw new CorruptRecordException(
                "Illegal length of message set " + validBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
        if (validBytes == records.sizeInBytes) {
            records
        } else {
            // trim invalid bytes
            val validByteBuffer = records.buffer.duplicate()
            validByteBuffer.limit(validBytes)
            MemoryRecords.readableRecords(validByteBuffer)
        }
    }

    /**
     * Read messages from the log.
     *
     * @param startOffset   The offset to begin reading at
     * @param maxLength     The maximum number of bytes to read
     * @param maxOffset     The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
     * @return The fetch data information including fetch starting offset metadata and messages read.
     */
    def read(startOffset: Long,
             maxLength: Int,
             maxOffset: Option[Long] = None,
             minOneMessage: Boolean = false): FetchDataInfo = {

        trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

        // 将 nextOffsetMetadata 保存成局部变量，从而避免加锁带来的竞态条件
        val currentNextOffsetMetadata = nextOffsetMetadata
        val next = currentNextOffsetMetadata.messageOffset
        if (startOffset == next)
            return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY)

        // 查找 baseOffset 小于 startOffset 且最大的 LogSegment
        var entry = segments.floorEntry(startOffset)

        // attempt to read beyond the log end offset is an error
        if (startOffset > next || entry == null)
            throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

        // Do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        while (entry != null) {
            // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
            // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
            // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
            // end of the active segment.
            val maxPosition = {
                // 如果当前读取的是 activeSegment，这里的逻辑重点参考一下书本
                if (entry == segments.lastEntry) {
                    val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
                    // Check the segment again in case a new segment has just rolled out.
                    if (entry != segments.lastEntry)
                    // New log segment has rolled out, we can read up to the file end.
                    // 期间正好写线程执行了 roll 操作，当前 activeSegment 已经被刷盘，不在活跃了，所以可以直接读取到结尾
                        entry.getValue.size
                    else
                        exposedPos
                } else {
                    // 如果当前是非 activeSegment，则直接读取到结尾
                    entry.getValue.size
                }
            }

            // 调用 LogSegment.read 方法读取消息
            val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
            if (fetchInfo == null) {
                // 如果没有读取到消息，则读取下一个 LogSegment
                entry = segments.higherEntry(entry.getKey)
            } else {
                return fetchInfo
            }
        }

        // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
        // this can happen when all messages with offset larger than start offsets have been deleted.
        // In this case, we will return the empty set with log end offset metadata
        // 没有找到 startOffset 之后的消息
        FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }

    /**
     * Get an offset based on the given timestamp
     * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
     * given timestamp.
     *
     * If no such message is found, the log end offset is returned.
     *
     * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
     * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
     *
     * @param targetTimestamp The given timestamp for offset fetching.
     * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
     *         None if no such message is found.
     */
    def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
        debug(s"Searching offset for timestamp $targetTimestamp")

        if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
                targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
                targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
            throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
                    s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
                    s"required version $KAFKA_0_10_0_IV0")

        // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
        // constant time access while being safe to use with concurrent collections unlike `toArray`.
        val segmentsCopy = logSegments.toBuffer
        // For the earliest and latest, we do not need to return the timestamp.
        if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
            return Some(TimestampOffset(Record.NO_TIMESTAMP, segmentsCopy.head.baseOffset))
        else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
                 return Some(TimestampOffset(Record.NO_TIMESTAMP, logEndOffset))

        val targetSeg = {
            // Get all the segments whose largest timestamp is smaller than target timestamp
            val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
            // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
            if (earlierSegs.length < segmentsCopy.length)
                Some(segmentsCopy(earlierSegs.length))
            else
                None
        }

        targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp))
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, return unknown offset metadata
     */
    def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
        try {
            val fetchDataInfo = read(offset, 1)
            fetchDataInfo.fetchOffsetMetadata
        } catch {
            case _: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
        }
    }

    /**
     * Delete any log segments matching the given predicate function,
     * starting with the oldest segment and moving forward until a segment doesn't match.
     *
     * @param predicate A function that takes in a single log segment and returns true if it is deletable
     * @return The number of segments deleted
     */
    private def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
        lock synchronized {
            // 依据时间检查当前 Log 中的 LogSegment 是否满足 predicate 条件
            val deletable = this.deletableSegments(predicate)
            val numToDelete = deletable.size
            if (numToDelete > 0) {
                // 如果当前 Log 中所有的 LogSegment 都需要被删除，则在删除之前创建一个新的 activeSegment
                if (segments.size == numToDelete)
                    this.roll()
                // 遍历删除这些需要删除的 LogSegment 文件
                deletable.foreach(deleteSegment)
            }
            numToDelete
        }
    }

    /**
     * Find segments starting from the oldest until the the user-supplied predicate is false.
     * A final segment that is empty will never be returned (since we would just end up re-creating it).
     *
     * @param predicate A function that takes in a single log segment and returns true iff it is deletable
     * @return the segments ready to be deleted
     */
    private def deletableSegments(predicate: LogSegment => Boolean): Iterable[LogSegment] = {
        val lastEntry = segments.lastEntry
        if (lastEntry == null) Seq.empty
        // 遍历 logSegments 中所有满足删除条件的 LogSegment
        else logSegments.takeWhile(
            s => predicate(s) // 如果当前 LogSegment 中最大消息的时间戳相对于当前时间已经超过 retention.ms，则允许删除
                    && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0)) // 且当前 LogSegment 中有数据
    }

    /**
     * Delete any log segments that have either expired due to time based retention
     * or because the log size is > retentionSize
     */
    def deleteOldSegments(): Int = {
        if (!config.delete) return 0
        this.deleteRetentionMsBreachedSegments() + this.deleteRetentionSizeBreachedSegments()
    }

    /**
     * 依据 retention.ms 配置检测 Log 中的 LogSegment 是否过期，并删除过期的 LogSegment
     *
     * @return
     */
    private def deleteRetentionMsBreachedSegments(): Int = {
        if (config.retentionMs < 0) return 0
        val startMs = time.milliseconds
        this.deleteOldSegments(startMs - _.largestTimestamp > config.retentionMs)
    }

    /**
     * 依据 retention.bytes 配置检测 Log 的大小是否过大，删除部分文件保证 Log 的大小在允许范围之内
     *
     * @return
     */
    private def deleteRetentionSizeBreachedSegments(): Int = {
        if (config.retentionSize < 0 || size < config.retentionSize) return 0
        var diff = size - config.retentionSize // Log 的大小减去允许的大小（retention.bytes）

        def shouldDelete(segment: LogSegment): Boolean = {
            if (diff - segment.size >= 0) {
                // 减去超出大小的部分
                diff -= segment.size
                true
            } else {
                false
            }
        }

        // 删除 Log 中超出大小的部分
        this.deleteOldSegments(shouldDelete)
    }

    /**
     * The size of the log in bytes
     */
    def size: Long = logSegments.map(_.size).sum

    /**
     * The earliest message offset in the log
     */
    def logStartOffset: Long = logSegments.head.baseOffset

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

    /**
     * The offset of the next message that will be appended to the log
     */
    def logEndOffset: Long = nextOffsetMetadata.messageOffset

    /**
     * Roll the log over to a new empty log segment if necessary.
     *
     * @param messagesSize           The messages set size in bytes
     * @param maxTimestampInMessages The maximum timestamp in the messages.
     *                               logSegment will be rolled if one of the following conditions met
     *                               <ol>
     *                               <li> The logSegment is full
     *                               <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
     *                               the first message does not have a timestamp)
     *                               <li> The index is full
     *                               </ol>
     * @return The currently active segment after (perhaps) rolling to a new segment
     */
    private def maybeRoll(messagesSize: Int,
                          maxTimestampInMessages: Long,
                          maxOffsetInMessages: Long): LogSegment = {
        val segment = activeSegment
        val now = time.milliseconds
        val reachedRollMs = segment.timeWaitedForRoll(now, maxTimestampInMessages) > config.segmentMs - segment.rollJitterMs
        if (segment.size > config.segmentSize - messagesSize // 当前 activeSegment 大小加上本次带追加的消息的大小，超过允许的 LogSegment 的最大长度
                || (segment.size > 0 && reachedRollMs) // 当前 activeSegment 的活跃时间已将超过了允许的最大活跃时间
                || segment.index.isFull // 索引文件满了
                || segment.timeIndex.isFull
                || !segment.canConvertToRelativeOffset(maxOffsetInMessages)) {
            debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
                    s"index_size = ${segment.index.entries}/${segment.index.maxEntries}, " +
                    s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
                    s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

            /*
             * maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
             * Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
             * first offset in the set. Determining the true first offset in the set requires decompression, which the follower
             * is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
             * This was problematic in the case that two consecutive messages differed in offset by Integer.MAX_VALUE.toLong + 2 or more.
             * In this case, the prior behavior would roll a new log segment whose base offset was too low to contain the next message.
             * This edge case is possible when a replica is recovering a highly compacted topic from scratch.
             *
             * 创建新的 activeSegment
             */
            this.roll(maxOffsetInMessages - Integer.MAX_VALUE)
        } else {
            segment
        }
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @return The newly rolled segment
     */
    def roll(expectedNextOffset: Long = 0): LogSegment = {
        val start = time.nanoseconds
        lock synchronized {
            val newOffset = Math.max(expectedNextOffset, logEndOffset)
            // 获取日志文件（名称是 ${LEO}.log）
            val logFile = Log.logFile(dir, newOffset)
            // 获取索引文件（名称是 ${LEO}.index）
            val indexFile = indexFilename(dir, newOffset)
            // 获取时间索引文件（名称是 ${LEO}.timeindex）
            val timeIndexFile = timeIndexFilename(dir, newOffset)
            // 遍历检查，如果文件已经存在则删除
            for (file <- List(logFile, indexFile, timeIndexFile); if file.exists) {
                warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
                file.delete()
            }

            // 处理上一任 activeSegment
            segments.lastEntry() match {
                case null =>
                case entry =>
                    val seg = entry.getValue
                    // 追加最大时间戳和对应的 offset 到 timeindex
                    seg.onBecomeInactiveSegment()
                    // 对日志文件和索引文件进行阶段处理，仅保留有效字节
                    seg.index.trimToValidSize()
                    seg.timeIndex.trimToValidSize()
                    seg.log.trim()
            }

            // 创建新的 activeSegment
            val segment = new LogSegment(
                dir,
                startOffset = newOffset,
                indexIntervalBytes = config.indexInterval,
                maxIndexSize = config.maxIndexSize,
                rollJitterMs = config.randomSegmentJitter,
                time = time,
                fileAlreadyExists = false,
                initFileSize = initFileSize(),
                preallocate = config.preallocate)

            // 添加新创建的 LogSegment 到 segments 跳跃表
            val prev = this.addSegment(segment)

            // 如果已经存在对应 baseOffset 的 LogSegment，则抛出异常
            if (prev != null)
                throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
            // We need to update the segment base offset and append position data of the metadata when log rolls.
            // The next offset should not change.
            // 更新 nextOffsetMetadata，主要是更新 segmentBaseOffset 和 relativePositionInSegment
            this.updateLogEndOffset(nextOffsetMetadata.messageOffset)

            // schedule an asynchronous flush of the old segment
            // 执行 flush 操作
            scheduler.schedule("flush-log", () => this.flush(newOffset))

            info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0 * 1000.0)))

            // 返回新的 activeSegment
            segment
        }
    }

    /**
     * The number of messages appended to the log since the last flush
     */
    def unflushedMessages(): Long = this.logEndOffset - this.recoveryPoint

    /**
     * Flush all log segments
     */
    def flush(): Unit = this.flush(this.logEndOffset)

    /**
     * Flush log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    def flush(offset: Long): Unit = {
        // 如果尾部 offset 小于等于 recoveryPoint，则直接返回，因为已经全部落盘了
        if (offset <= this.recoveryPoint)
            return
        debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " + time.milliseconds + " unflushed = " + unflushedMessages)
        // 遍历处理 [recoveryPoint, offset) 之间的 LogSegment
        for (segment <- this.logSegments(this.recoveryPoint, offset))
            segment.flush() // 执行刷盘操作
        lock synchronized {
            if (offset > this.recoveryPoint) {
                // 更新 recoveryPoint
                this.recoveryPoint = offset
                // 更新最近一次执行 flush 的时间
                lastflushedTime.set(time.milliseconds)
            }
        }
    }

    /**
     * Completely delete this log directory and all contents from the file system with no delay
     */
    private[log] def delete() {
        lock synchronized {
            logSegments.foreach(_.delete())
            segments.clear()
            Utils.delete(dir)
        }
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     */
    private[log] def truncateTo(targetOffset: Long) {
        info("Truncating log %s to offset %d.".format(name, targetOffset))
        if (targetOffset < 0)
            throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
        if (targetOffset > logEndOffset) {
            info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset - 1))
            return
        }
        lock synchronized {
            if (segments.firstEntry.getValue.baseOffset > targetOffset) {
                truncateFullyAndStartAt(targetOffset)
            } else {
                val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
                deletable.foreach(deleteSegment)
                activeSegment.truncateTo(targetOffset)
                updateLogEndOffset(targetOffset)
                this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
            }
        }
    }

    /**
     * Delete all data in the log and start at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    private[log] def truncateFullyAndStartAt(newOffset: Long) {
        debug("Truncate and start log '" + name + "' to " + newOffset)
        lock synchronized {
            val segmentsToDelete = logSegments.toList
            segmentsToDelete.foreach(deleteSegment)
            addSegment(new LogSegment(dir,
                newOffset,
                indexIntervalBytes = config.indexInterval,
                maxIndexSize = config.maxIndexSize,
                rollJitterMs = config.randomSegmentJitter,
                time = time,
                fileAlreadyExists = false,
                initFileSize = initFileSize(),
                preallocate = config.preallocate))
            updateLogEndOffset(newOffset)
            this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
        }
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     *
     * 上次执行 flush 的时间
     */
    def lastFlushTime(): Long = lastflushedTime.get

    /**
     * The active segment that is currently taking appends
     *
     * Log 中的数据是顺序写入的，也就是说只有最后一个 LogSegment 可以执行写入操作，
     * 本方法用于返回最后一个 LogSegment
     *
     */
    def activeSegment: LogSegment = segments.lastEntry.getValue

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    def logSegments: Iterable[LogSegment] = segments.values.asScala

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset)
     */
    def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
        lock synchronized {
            val floor = segments.floorKey(from)
            if (floor eq null)
                segments.headMap(to).values.asScala
            else
                segments.subMap(floor, true, to, false).values.asScala
        }
    }

    override def toString: String = "Log(" + dir + ")"

    /**
     * This method performs an asynchronous log segment delete by doing the following:
     * <ol>
     * <li>It removes the segment from the segment map so that it will no longer be used for reads.
     * <li>It renames the index and log files by appending .deleted to the respective file name
     * <li>It schedules an asynchronous delete operation to occur in the future
     * </ol>
     * This allows reads to happen concurrently without synchronization and without the possibility of physically
     * deleting a file while it is being read from.
     *
     * @param segment The log segment to schedule for deletion
     */
    private def deleteSegment(segment: LogSegment) {
        info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
        lock synchronized {
            // 从跳跃表中删除当前 LogSegment 对象
            segments.remove(segment.baseOffset)
            // 将对应的日志文件和索引文件后缀改为 .deleted，并提交一个定时任务执行删除操作
            this.asyncDeleteSegment(segment)
        }
    }

    /**
     * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
     *
     * @throws KafkaStorageException if the file can't be renamed and still exists
     */
    private def asyncDeleteSegment(segment: LogSegment) {
        // 修改文件后缀为 deleted
        segment.changeFileSuffixes("", Log.DeletedFileSuffix)

        def deleteSeg() {
            info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
            segment.delete()
        }

        // 提交到定时任务执行删除
        scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
    }

    /**
     * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
     * be asynchronously deleted.
     *
     * The sequence of operations is:
     * <ol>
     * <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
     * If broker crashes at this point, the clean-and-swap operation is aborted and
     * the .cleaned file is deleted on recovery in loadSegments().
     * <li> New segment is renamed .swap. If the broker crashes after this point before the whole
     * operation is completed, the swap operation is resumed on recovery as described in the next step.
     * <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
     * If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
     * replaceSegments() is then invoked to complete the swap with newSegment recreated from
     * the .swap file and oldSegments containing segments which were not renamed before the crash.
     * <li> Swap segment is renamed to replace the existing segment, completing this operation.
     * If the broker crashes, any .deleted files which may be left behind are deleted
     * on recovery in loadSegments().
     * </ol>
     *
     * @param newSegment          The new log segment to add to the log
     * @param oldSegments         The old log segments to delete from the log
     * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
     */
    private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false) {
        lock synchronized {
            // need to do this in two phases to be crash safe AND do the delete asynchronously
            // if we crash in the middle of this we complete the swap in loadSegments()
            if (!isRecoveredSwapFile)
                newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
            addSegment(newSegment)

            // delete the old files
            for (seg <- oldSegments) {
                // remove the index entry
                if (seg.baseOffset != newSegment.baseOffset)
                    segments.remove(seg.baseOffset)
                // delete segment
                asyncDeleteSegment(seg)
            }
            // okay we are safe now, remove the swap suffix
            newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
        }
    }

    /**
     * remove deleted log metrics
     */
    private[log] def removeLogMetrics(): Unit = {
        removeMetric("NumLogSegments", tags)
        removeMetric("LogStartOffset", tags)
        removeMetric("LogEndOffset", tags)
        removeMetric("Size", tags)
    }

    /**
     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
     *
     * @param segment The segment to add
     */
    def addSegment(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
 */
object Log {

    /** a log file */
    val LogFileSuffix = ".log"

    /** an index file */
    val IndexFileSuffix = ".index"

    /** a time index file */
    val TimeIndexFileSuffix = ".timeindex"

    /** a file that is scheduled to be deleted */
    val DeletedFileSuffix = ".deleted"

    /** A temporary file that is being used for log cleaning */
    val CleanedFileSuffix = ".cleaned"

    /** A temporary file used when swapping files into the log */
    val SwapFileSuffix = ".swap"

    /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
     * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
    val CleanShutdownFile = ".kafka_cleanshutdown"

    /** a directory that is scheduled to be deleted */
    val DeleteDirSuffix = "-delete"

    private val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    def filenamePrefixFromOffset(offset: Long): String = {
        val nf = NumberFormat.getInstance()
        nf.setMinimumIntegerDigits(20)
        nf.setMaximumFractionDigits(0)
        nf.setGroupingUsed(false)
        nf.format(offset)
    }

    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    def logFile(dir: File, offset: Long) = new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

    /**
     * Return a directory name to rename the log directory to for async deletion. The name will be in the following
     * format: topic-partition.uniqueId-delete where topic, partition and uniqueId are variables.
     */
    def logDeleteDirName(logName: String): String = {
        val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
        s"$logName.$uniqueId$DeleteDirSuffix"
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    def indexFilename(dir: File, offset: Long) = new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

    /**
     * Construct a time index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    def timeIndexFilename(dir: File, offset: Long) = new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix)

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    def parseTopicPartitionName(dir: File): TopicPartition = {
        if (dir == null)
            throw new KafkaException("dir should not be null")

        def exception(dir: File): KafkaException = {
            new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
                    "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                    "Kafka's log directories (and children) should only contain Kafka topic data.")
        }

        val dirName = dir.getName
        if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
            throw exception(dir)
        if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches)
            throw exception(dir)

        val name: String =
            if (dirName.endsWith(DeleteDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
            else dirName

        val index = name.lastIndexOf('-')
        val topic = name.substring(0, index)
        val partitionString = name.substring(index + 1)
        if (topic.isEmpty || partitionString.isEmpty)
            throw exception(dir)

        val partition =
            try partitionString.toInt
            catch {
                case _: NumberFormatException => throw exception(dir)
            }

        new TopicPartition(topic, partition)
    }

}

