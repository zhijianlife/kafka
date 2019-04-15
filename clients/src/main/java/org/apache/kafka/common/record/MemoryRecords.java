/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.record;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.ByteBufferLogInputStream.ByteBufferLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 * This is used only for reading or modifying in-place an existing buffer of log entries.
 * To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, CompressionType, TimestampType) builder} variants.
 *
 * 用于封装多个消息，使用 ByteBuffer 进行存储
 */
public class MemoryRecords extends AbstractRecords {

    private static final Logger log = LoggerFactory.getLogger(MemoryRecords.class);

    public final static MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    /** 用于保存消息 */
    private final ByteBuffer buffer;

    private final Iterable<ByteBufferLogEntry> shallowEntries = new Iterable<ByteBufferLogEntry>() {
        @Override
        public Iterator<ByteBufferLogEntry> iterator() {
            return shallowIterator();
        }
    };

    private int validBytes = -1;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        if (position > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("position should not be greater than Integer.MAX_VALUE: " + position);
        }
        if (position + length > buffer.limit()) {
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());
        }

        int pos = (int) position;
        ByteBuffer dup = buffer.duplicate();
        dup.position(pos);
        dup.limit(pos + length);
        return channel.write(dup);
    }

    /**
     * Write all records to the given channel (including partial records).
     *
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     *
     * @return The number of valid bytes
     */
    public int validBytes() {
        if (validBytes >= 0) {
            return validBytes;
        }

        int bytes = 0;
        for (LogEntry entry : shallowEntries())
            bytes += entry.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    /**
     * Filter the records into the provided ByteBuffer.
     *
     * @param filter The filter function
     * @param destinationBuffer The byte buffer to write the filtered records to
     * @return A FilterResult with a summary of the output (for metrics)
     */
    public FilterResult filterTo(TopicPartition partition, LogEntryFilter filter, ByteBuffer destinationBuffer, int maxRecordSize) {
        return filterTo(partition, shallowEntries(), filter, destinationBuffer, maxRecordSize);
    }

    private static FilterResult filterTo(TopicPartition partition,
                                         Iterable<ByteBufferLogEntry> fromShallowEntries,
                                         LogEntryFilter filter,
                                         ByteBuffer destinationBuffer,
                                         int maxRecordSize) {
        long maxTimestamp = Record.NO_TIMESTAMP;
        long maxOffset = -1L;
        long shallowOffsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(destinationBuffer);

        // 浅层迭代
        for (ByteBufferLogEntry shallowEntry : fromShallowEntries) {
            bytesRead += shallowEntry.sizeInBytes();

            // We use the absolute offset to decide whether to retain the message or not (this is handled by the
            // deep iterator). Because of KAFKA-4298, we have to allow for the possibility that a previous version
            // corrupted the log by writing a compressed message set with a wrapper magic value not matching the magic
            // of the inner messages. This will be fixed as we recopy the messages to the destination buffer.

            Record shallowRecord = shallowEntry.record();
            byte shallowMagic = shallowRecord.magic();
            boolean writeOriginalEntry = true;
            List<LogEntry> retainedEntries = new ArrayList<>();

            for (LogEntry deepEntry : shallowEntry) {
                Record deepRecord = deepEntry.record();
                messagesRead += 1;

                // 使用消息过滤器，过滤掉不应该保留的消息
                if (filter.shouldRetain(deepEntry)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted entry with correct data.
                    if (shallowMagic != deepRecord.magic()) {
                        writeOriginalEntry = false;
                    }

                    if (deepEntry.offset() > maxOffset) {
                        maxOffset = deepEntry.offset();
                    }

                    // 将当前消息记录到需要保留的集合中
                    retainedEntries.add(deepEntry);
                } else {
                    writeOriginalEntry = false;
                }
            }

            // 如果整个消息都需要被保留，直接将消息写入到 buffer 中
            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                bufferOutputStream.write(shallowEntry.buffer());
                messagesRetained += retainedEntries.size();
                bytesRetained += shallowEntry.sizeInBytes();

                if (shallowRecord.timestamp() > maxTimestamp) {
                    maxTimestamp = shallowRecord.timestamp();
                    shallowOffsetOfMaxTimestamp = shallowEntry.offset();
                }
            }
            // 如果只有部分消息需要被保留，则需要对深层消息进行重新压缩后写入 buffer
            else if (!retainedEntries.isEmpty()) {
                LogEntry firstEntry = retainedEntries.iterator().next();
                long firstOffset = firstEntry.offset();
                byte magic = firstEntry.record().magic();

                MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                        bufferOutputStream, magic,
                        shallowRecord.compressionType(), shallowRecord.timestampType(),
                        firstOffset, shallowRecord.timestamp(), bufferOutputStream.buffer().remaining());
                for (LogEntry entry : retainedEntries)
                    builder.appendWithOffset(entry.offset(), entry.record());

                MemoryRecords records = builder.build();
                int filteredSizeInBytes = records.sizeInBytes();

                messagesRetained += retainedEntries.size();
                bytesRetained += records.sizeInBytes();

                if (filteredSizeInBytes > shallowEntry.sizeInBytes() && filteredSizeInBytes > maxRecordSize) {
                    log.warn("Record batch from {} with first offset {} exceeded max record size {} after cleaning " +
                                    "(new size is {}). Consumers with version earlier than 0.10.1.0 may need to increase their fetch sizes.",
                            partition, firstOffset, maxRecordSize, filteredSizeInBytes);
                }

                MemoryRecordsBuilder.RecordsInfo info = builder.info();
                if (info.maxTimestamp > maxTimestamp) {
                    maxTimestamp = info.maxTimestamp;
                    shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp;
                }
            }

            // If we had to allocate a new buffer to fit the filtered output (see KAFKA-5316), return early to
            // avoid the need for additional allocations.
            ByteBuffer outputBuffer = bufferOutputStream.buffer();
            if (outputBuffer != destinationBuffer) {
                return new FilterResult(outputBuffer, messagesRead, bytesRead, messagesRetained, bytesRetained,
                        maxOffset, maxTimestamp, shallowOffsetOfMaxTimestamp);
            }
        } // ~ end for

        return new FilterResult(destinationBuffer, messagesRead, bytesRead,
                messagesRetained, bytesRetained, maxOffset, maxTimestamp, shallowOffsetOfMaxTimestamp);
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterable<ByteBufferLogEntry> shallowEntries() {
        return shallowEntries;
    }

    private Iterator<ByteBufferLogEntry> shallowIterator() {
        return RecordsIterator.shallowIterator(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    @Override
    public Iterable<LogEntry> deepEntries(BufferSupplier bufferSupplier) {
        return deepEntries(false, bufferSupplier);
    }

    @Override
    public Iterable<LogEntry> deepEntries() {
        return deepEntries(false, BufferSupplier.NO_CACHING);
    }

    public Iterable<LogEntry> deepEntries(final boolean ensureMatchingMagic, final BufferSupplier bufferSupplier) {
        return new Iterable<LogEntry>() {
            @Override
            public Iterator<LogEntry> iterator() {
                return deepIterator(ensureMatchingMagic, Integer.MAX_VALUE, bufferSupplier);
            }
        };
    }

    private Iterator<LogEntry> deepIterator(boolean ensureMatchingMagic, int maxMessageSize, BufferSupplier bufferSupplier) {
        return new RecordsIterator(new ByteBufferLogInputStream(buffer.duplicate(), maxMessageSize), false,
                ensureMatchingMagic, maxMessageSize, bufferSupplier);
    }

    @Override
    public String toString() {
        Iterator<LogEntry> iter = deepEntries(BufferSupplier.NO_CACHING).iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
            if (iter.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public interface LogEntryFilter {
        boolean shouldRetain(LogEntry entry);
    }

    public static class FilterResult {
        public final ByteBuffer output;
        public final int messagesRead;
        public final int bytesRead;
        public final int messagesRetained;
        public final int bytesRetained;
        public final long maxOffset;
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public FilterResult(ByteBuffer output,
                            int messagesRead,
                            int bytesRead,
                            int messagesRetained,
                            int bytesRetained,
                            long maxOffset,
                            long maxTimestamp,
                            long shallowOffsetOfMaxTimestamp) {
            this.output = output;
            this.messagesRead = messagesRead;
            this.bytesRead = bytesRead;
            this.messagesRetained = messagesRetained;
            this.bytesRetained = bytesRetained;
            this.maxOffset = maxOffset;
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               int writeLimit) {
        return new MemoryRecordsBuilder(
                buffer, Record.CURRENT_MAGIC_VALUE, compressionType,
                timestampType, 0L, System.currentTimeMillis(), writeLimit);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime) {
        return new MemoryRecordsBuilder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime, buffer.capacity());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType) {
        // use the buffer capacity as the default write limit
        return builder(buffer, compressionType, timestampType, buffer.capacity());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType) {
        return builder(buffer, magic, compressionType, timestampType, 0L);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer);
    }

    public static MemoryRecords withLogEntries(CompressionType compressionType, List<LogEntry> entries) {
        return withLogEntries(TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), entries);
    }

    public static MemoryRecords withLogEntries(LogEntry... entries) {
        return withLogEntries(CompressionType.NONE, Arrays.asList(entries));
    }

    public static MemoryRecords withRecords(CompressionType compressionType, long initialOffset, List<Record> records) {
        return withRecords(initialOffset, TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), records);
    }

    public static MemoryRecords withRecords(Record... records) {
        return withRecords(CompressionType.NONE, 0L, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(long initialOffset, Record... records) {
        return withRecords(CompressionType.NONE, initialOffset, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(CompressionType compressionType, Record... records) {
        return withRecords(compressionType, 0L, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(TimestampType timestampType, CompressionType compressionType, Record... records) {
        return withRecords(0L, timestampType, compressionType, System.currentTimeMillis(), Arrays.asList(records));
    }

    public static MemoryRecords withRecords(long initialOffset,
                                            TimestampType timestampType,
                                            CompressionType compressionType,
                                            long logAppendTime,
                                            List<Record> records) {
        return withLogEntries(timestampType, compressionType, logAppendTime, buildLogEntries(initialOffset, records));
    }

    private static MemoryRecords withLogEntries(TimestampType timestampType,
                                                CompressionType compressionType,
                                                long logAppendTime,
                                                List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return MemoryRecords.EMPTY;
        }
        return builderWithEntries(timestampType, compressionType, logAppendTime, entries).build();
    }

    private static List<LogEntry> buildLogEntries(long initialOffset, List<Record> records) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : records)
            entries.add(LogEntry.create(initialOffset++, record));
        return entries;
    }

    public static MemoryRecordsBuilder builderWithEntries(TimestampType timestampType,
                                                          CompressionType compressionType,
                                                          long logAppendTime,
                                                          List<LogEntry> entries) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize(compressionType, entries));
        return builderWithEntries(buffer, timestampType, compressionType, logAppendTime, entries);
    }

    private static MemoryRecordsBuilder builderWithEntries(ByteBuffer buffer,
                                                           TimestampType timestampType,
                                                           CompressionType compressionType,
                                                           long logAppendTime,
                                                           List<LogEntry> entries) {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }

        LogEntry firstEntry = entries.iterator().next();
        long firstOffset = firstEntry.offset();
        byte magic = firstEntry.record().magic();

        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer, magic, compressionType, timestampType, firstOffset, logAppendTime);
        for (LogEntry entry : entries)
            builder.appendWithOffset(entry.offset(), entry.record());

        return builder;
    }

}
