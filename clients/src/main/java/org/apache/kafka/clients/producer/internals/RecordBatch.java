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

package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    /** 当前 RecordBatch 创建的时间戳 */
    final long createdMs;

    /** 当前缓存的消息的目标 TopicPartition 对象 */
    final TopicPartition topicPartition;

    /** 标识当前 RecordBatch 发送之后的状态 */
    final ProduceRequestResult produceFuture;

    /** 消息的 Callback 队列，每个消息都对应一个 Callback 对象 */
    private final List<Thunk> thunks = new ArrayList<>();

    /** 用来存储数据的 {@link MemoryRecords} 对应的 builder 对象 */
    private final MemoryRecordsBuilder recordsBuilder;

    /** 尝试发送当前 RecordBatch 的次数 */
    volatile int attempts;
    /** 最后一次尝试发送的时间戳` */
    long lastAttemptMs;

    /** 记录保存的 record 个数 */
    int recordCount;

    /** 记录最大的 record 字节数 */
    int maxRecordSize;

    /** 记录上次投递当前 BatchRecord 的时间戳 */
    long drainedMs;

    /** 追后一次向当前 RecordBatch 追加消息的时间戳 */
    long lastAppendTime;
    private String expiryErrorMessage;
    private AtomicBoolean completed;

    /** 标记是否正在重试 */
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.completed = new AtomicBoolean();
    }

    /**
     * Append the record to the current record set and return the relative（相对的） offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        // 检测是否还有多余的空间容纳该消息
        if (!recordsBuilder.hasRoomFor(key, value)) {
            // 没有多余的空间则直接返回，后面会尝试申请新的空间
            return null;
        }
        // 添加当前消息到 MemoryRecords，并返回消息对应的 CRC32 校验码
        long checksum = this.recordsBuilder.append(timestamp, key, value);
        // 更新最大 record 字节数
        this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
        // 更新最后一次追加记录时间戳
        this.lastAppendTime = now;
        FutureRecordMetadata future = new FutureRecordMetadata(
                produceFuture, recordCount,
                timestamp, checksum,
                key == null ? -1 : key.length,
                value == null ? -1 : value.length);
        if (callback != null) {
            // 如果指定了 Callback，将 Callback 和 FutureRecordMetadata 封装到 Trunk 中
            thunks.add(new Thunk(callback, future));
        }
        this.recordCount++;
        return future;
    }

    /**
     * Complete the request.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long logAppendTime, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.", topicPartition, baseOffset, exception);

        if (completed.getAndSet(true)) {
            throw new IllegalStateException("Batch has already been completed");
        }

        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        // 循环执行每个消息的 Callback
        for (Thunk thunk : thunks) {
            try {
                // 消息处理正常
                if (exception == null) {
                    // RecordMetadata 是服务端返回的
                    RecordMetadata metadata = thunk.future.value();
                    thunk.callback.onCompletion(metadata, null);
                }
                // 消息处理异常
                else {
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 整个 RecordBatch 都处理完成
        produceFuture.done();
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     * <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     * <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     * This methods closes this batch and sets {@code expiryErrorMessage} if the batch has timed out.
     * {@link #expirationDone()} must be invoked to complete the produce future and invoke callbacks.
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime)) {
            expiryErrorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
        } else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs))) {
            expiryErrorMessage = (now - (this.createdMs + lingerMs)) + " ms has passed since batch creation plus linger time";
        } else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs))) {
            expiryErrorMessage = (now - (this.lastAttemptMs + retryBackoffMs)) + " ms has passed since last attempt plus backoff time";
        }

        boolean expired = expiryErrorMessage != null;
        if (expired) {
            this.close();
        }
        return expired;
    }

    /**
     * Completes the produce future with timeout exception and invokes callbacks.
     * This method should be invoked only if {@link #maybeExpire(int, long, long, long, boolean)} returned true.
     */
    void expirationDone() {
        if (expiryErrorMessage == null) {
            throw new IllegalStateException("Batch has not expired");
        }
        this.done(-1L, Record.NO_TIMESTAMP,
                new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + ": " + expiryErrorMessage));
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    private boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int sizeInBytes() {
        return recordsBuilder.sizeInBytes();
    }

    public double compressionRate() {
        return recordsBuilder.compressionRate();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void close() {
        recordsBuilder.close();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

}
