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

import java.io.{File, IOException, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging, Os}
import org.apache.kafka.common.utils.Utils
import sun.nio.ch.DirectBuffer

import scala.math.ceil

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * @param file         指向磁盘上的索引文件
 * @param baseOffset   对应日志文件中第一个消息的 offset
 * @param maxIndexSize The maximum index size in bytes.
 */
abstract class AbstractIndex[K, V](@volatile var file: File,
                                   val baseOffset: Long,
                                   val maxIndexSize: Int = -1) extends Logging {

    protected def entrySize: Int

    protected val lock = new ReentrantLock

    /** 用于操作索引文件的 MappedByteBuffer 对象 */
    @volatile
    protected var mmap: MappedByteBuffer = {
        // 如果索引文件不存在，则创建
        val newlyCreated = file.createNewFile()
        val raf = new RandomAccessFile(file, "rw")
        try {
            // 如果是新创建的文件，设置容量大小
            if (newlyCreated) {
                if (maxIndexSize < entrySize)
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
                raf.setLength(this.roundDownToExactMultiple(maxIndexSize, entrySize))
            }

            // 执行内存映射
            val len = raf.length()
            val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)

            // 如果是创建的文件，则设置 position 为 0，表示从头开始写文件
            if (newlyCreated)
                idx.position(0)
            else
            // 如果是已经存在的索引文件，将 position 移动到 limit 上界的位置
                idx.position(this.roundDownToExactMultiple(idx.limit, entrySize))

            idx
        } finally {
            CoreUtils.swallow(raf.close())
        }
    }

    /** 当前索引文件中能够保存的最大索引项个数 */
    @volatile
    private[this] var _maxEntries = mmap.limit / entrySize

    /** 当前索引文件中的索引项个数 */
    @volatile
    protected var _entries: Int = mmap.position / entrySize

    /**
     * True if there are no more slots available in this index
     */
    def isFull: Boolean = _entries >= _maxEntries

    def maxEntries: Int = _maxEntries

    def entries: Int = _entries

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    def resize(newSize: Int) {
        inLock(lock) {
            val raf = new RandomAccessFile(file, "rw")
            val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)
            val position = mmap.position

            /* Windows won't let us modify the file length while the file is mmapped :-( */
            if (Os.isWindows)
                forceUnmap(mmap)
            try {
                raf.setLength(roundedNewSize)
                mmap = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
                _maxEntries = mmap.limit / entrySize
                mmap.position(position)
            } finally {
                CoreUtils.swallow(raf.close())
            }
        }
    }

    /**
     * Rename the file that backs this offset index
     *
     * @throws IOException if rename fails
     */
    def renameTo(f: File) {
        try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
        finally file = f
    }

    /**
     * Flush the data in the index to disk
     */
    def flush() {
        inLock(lock) {
            mmap.force()
        }
    }

    /**
     * Delete this index file
     */
    def delete(): Boolean = {
        info(s"Deleting index ${file.getAbsolutePath}")
        inLock(lock) {
            // On JVM, a memory mapping is typically unmapped by garbage collector.
            // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
            // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
            // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
            CoreUtils.swallow(forceUnmap(mmap))
            // Accessing unmapped mmap crashes JVM by SEGV.
            // Accessing it after this method called sounds like a bug but for safety, assign null and do not allow later access.
            mmap = null
        }
        file.delete()
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from the file.
     */
    def trimToValidSize() {
        inLock(lock) {
            resize(entrySize * _entries)
        }
    }

    /**
     * The number of bytes actually used by this index
     */
    def sizeInBytes: Int = entrySize * _entries

    /** Close the index */
    def close() {
        trimToValidSize()
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throws IllegalArgumentException if any problems are found
     */
    def sanityCheck(): Unit

    /**
     * Remove all the entries from the index.
     */
    def truncate(): Unit

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    def truncateTo(offset: Long): Unit

    /**
     * Forcefully free the buffer's mmap.
     */
    protected def forceUnmap(m: MappedByteBuffer) {
        try {
            m match {
                case buffer: DirectBuffer =>
                    val bufferCleaner = buffer.cleaner()
                    /* cleaner can be null if the mapped region has size 0 */
                    if (bufferCleaner != null)
                        bufferCleaner.clean()
                case _ =>
            }
        } catch {
            case t: Throwable => error("Error when freeing index buffer", t)
        }
    }

    /**
     * 针对 Windows 系统加锁
     *
     * Execute the given function in a lock only if we are running on windows.
     * We do this because Windows won't let us resize a file while it is mmapped.
     * As a result we have to force unmap it and this requires synchronizing reads.
     */
    protected def maybeLock[T](lock: Lock)(fun: => T): T = {
        if (Os.isWindows)
            lock.lock()
        try {
            fun
        } finally {
            if (Os.isWindows)
                lock.unlock()
        }
    }

    /**
     * To parse an entry in the index.
     *
     * @param buffer the buffer of this memory mapped index.
     * @param n      the slot
     * @return the index entry stored in the given slot.
     */
    protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

    /**
     * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
     * The comparison is made using the `IndexEntry.compareTo()` method.
     *
     * @param idx    The index buffer
     * @param target The index key to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
     */
    protected def indexSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int = {
        // 当前索引文件中没有索引项
        if (_entries == 0)
            return -1

        // 如果检索的 offset 小于索引文件中最小的 offset
        if (this.compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
            return -1

        // 二分查找
        var lo = 0
        var hi = _entries - 1
        while (lo < hi) {
            val mid = ceil(hi / 2.0 + lo / 2.0).toInt
            val found = parseEntry(idx, mid)
            val compareResult = compareIndexEntry(found, target, searchEntity)
            if (compareResult > 0) hi = mid - 1
            else if (compareResult < 0) lo = mid
            else return mid
        }
        lo
    }

    private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
        searchEntity match {
            case IndexSearchType.KEY => indexEntry.indexKey.compareTo(target)
            case IndexSearchType.VALUE => indexEntry.indexValue.compareTo(target)
        }
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundDownToExactMultiple(67, 8) == 64
     */
    private def roundDownToExactMultiple(number: Int, factor: Int): Int = factor * (number / factor)

}

object IndexSearchType extends Enumeration {
    type IndexSearchEntity = Value
    val KEY, VALUE = Value
}
