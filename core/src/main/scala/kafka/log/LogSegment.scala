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
import java.util.concurrent.TimeUnit

import kafka.common._
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.LogEntryPosition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index.
 *
 * - The log is a FileMessageSet containing the actual messages.
 * - The index is an OffsetIndex that maps from logical offsets to physical file positions.
 *
 * Each segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * 每个日志文件可以切分成多个分片（topic 分区文件），每个分片对应一个 LogSegment，封装了 log、index 和 timeindex 文件
 *
 * @param log                用于操作 log 文件
 * @param index              用于操作 index 文件
 * @param timeIndex          用户操作 timeindex 文件
 * @param baseOffset         当前日志分片文件中第一条消息的 offset 值
 * @param indexIntervalBytes 索引项之间间隔的最小字节数
 * @param time               时间戳工具
 */
@nonthreadsafe
class LogSegment(val log: FileRecords, // log 文件对象
                 val index: OffsetIndex, // index 文件对象
                 val timeIndex: TimeIndex, // timeindex 文件对象
                 val baseOffset: Long, // 当前日志分片文件中第一条消息的 offset 值
                 val indexIntervalBytes: Int, // 索引项之间间隔的最小字节数，对应 index.interval.bytes 配置
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

    /** 当前 LogSegment 的创建时间 */
    private var created = time.milliseconds

    /** 从上次添加索引项后，在日志文件中累计加入的消息的字节数 */
    private var bytesSinceLastIndexEntry = 0

    /** The timestamp we used for time based log rolling */
    private var rollingBasedTimestamp: Option[Long] = None

    /** 已追加的消息对应的最大时间戳 */
    @volatile private var maxTimestampSoFar = timeIndex.lastEntry.timestamp
    /** 已追加的具备最大时间戳的消息对应的 offset */
    @volatile private var offsetOfMaxTimestamp = timeIndex.lastEntry.offset

    def this(dir: File,
             startOffset: Long,
             indexIntervalBytes: Int,
             maxIndexSize: Int,
             rollJitterMs: Long,
             time: Time,
             fileAlreadyExists: Boolean = false,
             initFileSize: Int = 0,
             preallocate: Boolean = false) =
        this(FileRecords.open(Log.logFile(dir, startOffset), fileAlreadyExists, initFileSize, preallocate),
            new OffsetIndex(Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
            new TimeIndex(Log.timeIndexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
            startOffset,
            indexIntervalBytes,
            rollJitterMs,
            time)

    /**
     * Return the size in bytes of this log segment
     *
     * @return
     */
    def size: Long = log.sizeInBytes()

    /**
     * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
     */
    def canConvertToRelativeOffset(offset: Long): Boolean = {
        (offset - baseOffset) <= Integer.MAX_VALUE
    }

    /**
     * 追加消息
     *
     * @param firstOffset                 待追加消息的第一条消息的 offset
     * @param largestOffset               待追加消息中的最大 offset
     * @param largestTimestamp            待追加消息中的最大时间戳
     * @param shallowOffsetOfMaxTimestamp 最大时间戳消息对应的 offset
     * @param records                     待追加的消息
     */
    @nonthreadsafe
    def append(firstOffset: Long, // 待追加消息的第一条消息的 offset
               largestOffset: Long, // 待追加消息中的最大 offset
               largestTimestamp: Long, // 待追加消息中的最大时间戳
               shallowOffsetOfMaxTimestamp: Long, // 最大时间戳消息对应的 offset
               records: MemoryRecords // 待追加的消息
              ) {
        if (records.sizeInBytes > 0) {
            trace("Inserting %d bytes at offset %d at position %d with largest timestamp %d at shallow offset %d"
                    .format(records.sizeInBytes, firstOffset, log.sizeInBytes(), largestTimestamp, shallowOffsetOfMaxTimestamp))
            // 获取物理位置（当前分片的大小）
            val physicalPosition = log.sizeInBytes()
            if (physicalPosition == 0)
                rollingBasedTimestamp = Some(largestTimestamp)

            require(canConvertToRelativeOffset(largestOffset), "largest offset in message set can not be safely converted to relative offset.")

            // 将消息数据追加到 log 文件
            val appendedBytes = log.append(records)
            trace(s"Appended $appendedBytes to ${log.file()} at offset $firstOffset")

            // 更新已追加的消息对应的最大时间戳，及其 offset
            if (largestTimestamp > maxTimestampSoFar) {
                maxTimestampSoFar = largestTimestamp
                offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
            }

            // 如果当前累计添加的日志字节数超过设置值（对应 index.interval.bytes 配置）
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                // 更新 index 和 timeindex 文件
                index.append(firstOffset, physicalPosition)
                timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
                bytesSinceLastIndexEntry = 0 // 重置当前累计加入的日志字节数
            }
            // 更新累计加入的日志字节数
            bytesSinceLastIndexEntry += records.sizeInBytes
        }
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     *
     * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
     * in the file higher than the greatest-lower-bound from the index.
     *
     * @param offset               The offset we want to translate
     * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
     *                             when omitted, the search will begin at the position in the offset index.
     * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
     *         message or null if no message meets this criteria.
     */
    @threadsafe
    private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogEntryPosition = {
        // 基于二分查找获取小于等于参数 offset 的最大 offset，返回 offset 和对应的物理地址
        val mapping = index.lookup(offset)
        // 查找对应的物理地址 position
        log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset.
     * The message set will include no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     *
     * 读取消息
     *
     * @param startOffset   A lower bound on the first offset to include in the message set we read
     * @param maxSize       The maximum number of bytes to include in the message set we read
     * @param maxOffset     An optional maximum offset for the message set we read
     * @param maxPosition   The maximum position in the log segment that should be exposed for read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
     * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
     *         or null if the startOffset is larger than the largest offset in this log
     */
    @threadsafe
    def read(startOffset: Long, // 读取消息的起始 offset
             maxOffset: Option[Long], // 读取消息的结束 offset
             maxSize: Int, // 读取消息的最大字节数
             maxPosition: Long = size, // 读取的最大物理地址
             minOneMessage: Boolean = false): FetchDataInfo = {

        if (maxSize < 0)
            throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

        // 获取当前 log 文件的字节大小
        val logSize = log.sizeInBytes // this may change, need to save a consistent copy
        // 获取小于等于 startOffset 的最大 offset 对应的物理地址 position
        val startOffsetAndSize = this.translateOffset(startOffset)

        // 读取的位置超出了当前文件，直接返回 null
        if (startOffsetAndSize == null)
            return null

        val startPosition = startOffsetAndSize.position
        val offsetMetadata = new LogOffsetMetadata(startOffset, baseOffset, startPosition)

        // 更新读取消息的最大字节数
        val adjustedMaxSize =
            if (minOneMessage) math.max(maxSize, startOffsetAndSize.size) else maxSize
        // 如果请求读取的消息最大字节数为 0，则返回一个空的结果对象
        if (adjustedMaxSize == 0)
            return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

        // 计算读取的字节数
        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        val length = maxOffset match {
            // 如果未指定读取消息的结束位置
            case None =>
                // 直接读取到指定的最大物理地址
                min((maxPosition - startPosition).toInt, adjustedMaxSize)
            // 如果指定了读取消息的结束位置
            case Some(offset) =>
                // 如果结束位置小于起始位置，则直接返回一个空的结果对象
                if (offset < startOffset)
                    return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)
                // 将结束位置 offset 转换成对应的物理地址
                val mapping = this.translateOffset(offset, startPosition)
                val endPosition =
                    if (mapping == null)
                        logSize // 如果结束位置 maxOffset 超出当前日志文件，则使用日志文件长度
                    else
                        mapping.position
                // 由 maxOffset、maxPosition，以及 maxSize 共同决定最终读取长度
                min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
        }

        // 读取对应的消息数据，并封装成 FetchDataInfo 对象返回
        FetchDataInfo(
            offsetMetadata,
            log.read(startPosition, length),
            firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
    }

    /**
     * Run recovery on the given segment.
     * This will rebuild the index from the log file and lop off （砍掉） any invalid bytes from the end of the log and index.
     *
     * 依据日志文件重建索引文件，同时验证日志文件中的消息的合法性
     *
     * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
     *                       is corrupt.
     * @return The number of bytes truncated from the log
     */
    @nonthreadsafe
    def recover(maxMessageSize: Int): Int = {
        // 清空 index 文件
        index.truncate()
        index.resize(index.maxIndexSize)
        // 清空 timeindex 文件
        timeIndex.truncate()
        timeIndex.resize(timeIndex.maxIndexSize)
        var validBytes = 0 // 记录通过验证的字节数
        var lastIndexEntry = 0 // 最后一个索引项对应的物理地址
        maxTimestampSoFar = Record.NO_TIMESTAMP
        try {
            // 遍历 log 文件，重建索引
            for (entry <- log.shallowEntries(maxMessageSize).asScala) {
                // 获取对应的消息 Record 对象
                val record = entry.record
                // 校验消息数据的有效性，如果存在问题则抛出异常
                record.ensureValid()

                // 更新本地记录的消息最大时间戳及其 offset
                if (record.timestamp > maxTimestampSoFar) {
                    maxTimestampSoFar = record.timestamp
                    offsetOfMaxTimestamp = entry.offset
                }

                // 如果当前字节减去上一次记录索引的字节超过设置的索引项之间间隔的最小字节数，则添加索引项
                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    val startOffset = entry.firstOffset
                    index.append(startOffset, validBytes)
                    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
                    lastIndexEntry = validBytes
                }
                validBytes += entry.sizeInBytes()
            }
        } catch {
            case e: CorruptRecordException =>
                logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
        }
        // 截断日志和索引文件中无效的字节
        val truncated = log.sizeInBytes - validBytes
        log.truncateTo(validBytes)
        index.trimToValidSize()
        // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
        timeIndex.trimToValidSize()
        truncated
    }

    def loadLargestTimestamp(readToLogEnd: Boolean = false) {
        // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
        val lastTimeIndexEntry = timeIndex.lastEntry
        maxTimestampSoFar = lastTimeIndexEntry.timestamp
        offsetOfMaxTimestamp = lastTimeIndexEntry.offset
        if (readToLogEnd) {
            val offsetPosition = index.lookup(lastTimeIndexEntry.offset)
            // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
            val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
            if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
                maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
                offsetOfMaxTimestamp = maxTimestampOffsetAfterLastEntry.offset
            }
        }
    }

    override def toString: String = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

    /**
     * Truncate off all index and log entries with offsets >= the given offset.
     * If the given offset is larger than the largest message in this segment, do nothing.
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
    @nonthreadsafe
    def truncateTo(offset: Long): Int = {
        val mapping = translateOffset(offset)
        if (mapping == null)
            return 0
        index.truncateTo(offset)
        timeIndex.truncateTo(offset)
        // after truncation, reset and allocate more space for the (new currently  active) index
        index.resize(index.maxIndexSize)
        timeIndex.resize(timeIndex.maxIndexSize)
        val bytesTruncated = log.truncateTo(mapping.position.toInt)
        if (log.sizeInBytes == 0) {
            created = time.milliseconds
            rollingBasedTimestamp = None
        }
        bytesSinceLastIndexEntry = 0
        // We may need to reload the max timestamp after truncation.
        if (maxTimestampSoFar >= 0)
            loadLargestTimestamp(readToLogEnd = true)
        bytesTruncated
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     */
    @threadsafe
    def nextOffset(): Long = {
        val ms = read(index.lastOffset, None, log.sizeInBytes)
        if (ms == null) {
            baseOffset
        } else {
            ms.records.shallowEntries.asScala.lastOption match {
                case None => baseOffset
                case Some(last) => last.nextOffset
            }
        }
    }

    /**
     * Flush this log segment to disk
     */
    @threadsafe
    def flush() {
        LogFlushStats.logFlushTimer.time {
            log.flush()
            index.flush()
            timeIndex.flush()
        }
    }

    /**
     * Change the suffix for the index and log file for this log segment
     */
    def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

        def kafkaStorageException(fileType: String, e: IOException) =
            new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

        try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
        catch {
            case e: IOException => throw kafkaStorageException("log", e)
        }
        try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
        catch {
            case e: IOException => throw kafkaStorageException("index", e)
        }
        try timeIndex.renameTo(new File(CoreUtils.replaceSuffix(timeIndex.file.getPath, oldSuffix, newSuffix)))
        catch {
            case e: IOException => throw kafkaStorageException("timeindex", e)
        }
    }

    /**
     * Append the largest time index entry to the time index when this log segment become inactive segment.
     * This entry will be used to decide when to delete the segment.
     */
    def onBecomeInactiveSegment() {
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    }

    /**
     * The time this segment has waited to be rolled.
     * If the first message has a timestamp we use the message timestamp to determine when to roll a segment. A segment
     * is rolled if the difference between the new message's timestamp and the first message's timestamp exceeds the
     * segment rolling time.
     * If the first message does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
     * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
     * segment rolling time.
     */
    def timeWaitedForRoll(now: Long, messageTimestamp: Long): Long = {
        // Load the timestamp of the first message into memory
        if (rollingBasedTimestamp.isEmpty) {
            val iter = log.shallowEntries.iterator()
            if (iter.hasNext)
                rollingBasedTimestamp = Some(iter.next().record.timestamp)
        }
        rollingBasedTimestamp match {
            case Some(t) if t >= 0 => messageTimestamp - t
            case _ => now - created
        }
    }

    /**
     * Search the message offset based on timestamp.
     * This method returns an option of TimestampOffset. The offset is the offset of the first message whose timestamp is
     * greater than or equals to the target timestamp.
     *
     * If all the message in the segment have smaller timestamps, the returned offset will be last offset + 1 and the
     * timestamp will be max timestamp in the segment.
     *
     * If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp,
     * the returned the offset will be the base offset of the segment and the timestamp will be Message.NoTimestamp.
     *
     * This methods only returns None when the log is not empty but we did not see any messages when scanning the log
     * from the indexed position. This could happen if the log is truncated after we get the indexed position but
     * before we scan the log from there. In this case we simply return None and the caller will need to check on
     * the truncated log and maybe retry or even do the search on another log segment.
     *
     * @param timestamp The timestamp to search for.
     * @return the timestamp and offset of the first message whose timestamp is larger than or equal to the
     *         target timestamp. None will be returned if there is no such message.
     */
    def findOffsetByTimestamp(timestamp: Long): Option[TimestampOffset] = {
        // Get the index entry with a timestamp less than or equal to the target timestamp
        val timestampOffset = timeIndex.lookup(timestamp)
        val position = index.lookup(timestampOffset.offset).position

        // Search the timestamp
        Option(log.searchForTimestamp(timestamp, position)).map { timestampAndOffset =>
            TimestampOffset(timestampAndOffset.timestamp, timestampAndOffset.offset)
        }
    }

    /**
     * Close this log segment
     */
    def close() {
        CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true))
        CoreUtils.swallow(index.close())
        CoreUtils.swallow(timeIndex.close())
        CoreUtils.swallow(log.close())
    }

    /**
     * Delete this log segment from the filesystem.
     *
     * @throws KafkaStorageException if the delete fails.
     */
    def delete() {
        val deletedLog = log.delete()
        val deletedIndex = index.delete()
        val deletedTimeIndex = timeIndex.delete()
        if (!deletedLog && log.file.exists)
            throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
        if (!deletedIndex && index.file.exists)
            throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
        if (!deletedTimeIndex && timeIndex.file.exists)
            throw new KafkaStorageException("Delete of time index " + timeIndex.file.getName + " failed.")
    }

    /**
     * The last modified time of this log segment as a unix time stamp
     */
    def lastModified: Long = log.file.lastModified

    /**
     * The largest timestamp this segment contains.
     */
    def largestTimestamp: Long = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

    /**
     * Change the last modified time for this log segment
     */
    def lastModified_=(ms: Long): Boolean = {
        log.file.setLastModified(ms)
        index.file.setLastModified(ms)
        timeIndex.file.setLastModified(ms)
    }
}

object LogFlushStats extends KafkaMetricsGroup {
    val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
