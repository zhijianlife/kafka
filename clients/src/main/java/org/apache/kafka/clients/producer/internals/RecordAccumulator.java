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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息收集器，可以看做是一个本地的消息队列，本质上使用 {@link MemoryRecords} 存储消息
 *
 * This class acts as a queue that accumulates records into {@link MemoryRecords} instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted,
 * unless this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    /** 标识当前收集器是否被关闭，对应 producer 被关闭 */
    private volatile boolean closed;

    /** 记录正在执行 flush 操作的线程数 */
    private final AtomicInteger flushesInProgress;

    /** 记录正在执行 append 操作的线程数 */
    private final AtomicInteger appendsInProgress;

    /** 指定每个 RecordBatch 中 ByteBuffer 的大小 */
    private final int batchSize;

    /** 消息压缩类型 */
    private final CompressionType compression;

    /** 通过参数 linger.ms 指定，当本地消息缓存时间超过该值时，即使消息量未达到阈值也会进行投递 */
    private final long lingerMs;

    /** 生产者重试时间间隔 */
    private final long retryBackoffMs;

    /** 缓存（ByteBuffer）管理工具 */
    private final BufferPool free;

    /** 时间戳工具 */
    private final Time time;

    /** 记录 topic 分区与 RecordBatch 的映射关系，对应的消息都是发往对应的 topic 分区 */
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;

    /** 记录未发送完成（即未收到服务端响应）的消息集合 */
    private final IncompleteRecordBatches incomplete;

    /* The following variables are only accessed by the sender thread, so we don't need to protect them. */

    /**
     * 消息顺序性保证，
     * 缓存当前待发送消息的目标 topic 分区，防止对于同一个 topic 分区同时存在多个未完成的消息，可能导致消息顺序性错乱
     */
    private final Set<TopicPartition> muted;

    /** 记录 drain 方法批量导出消息时上次的偏移量 */
    private int drainIndex;

    /**
     * Create a new record accumulator
     *
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     * sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     * latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     * exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // 记录正在向收集器中追加消息的线程数
        appendsInProgress.incrementAndGet();
        try {
            // 获取当前 topic 分区对应的 Deque，如果不存在则创建一个
            Deque<RecordBatch> dq = this.getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed) {
                    // producer 已经被关闭了，抛出异常
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }
                // 向 Deque 中最后一个 RecordBatch 追加 Record，并返回对应的 RecordAppendResult 对象
                RecordAppendResult appendResult = this.tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // 追加成功，直接返回
                    return appendResult;
                }
            }

            /* 追加 Record 失败，尝试申请新的 buffer */

            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            // 申请新的 buffer
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                if (closed) {
                    // 再次校验 producer 状态，如果已经被关闭了，抛出异常
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                }

                // 再次尝试向 Deque 中最后一个 RecordBatch 追加 Record
                RecordAppendResult appendResult = this.tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // 追加成功则返回，同时归还之前申请的 buffer
                    free.deallocate(buffer);
                    return appendResult;
                }

                /* 仍然追加失败，创建一个新的 RecordBatch 进行追加 */

                MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(buffer, compression, TimestampType.CREATE_TIME, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, recordsBuilder, time.milliseconds());
                // 在新创建的 RecordBatch 中追加 Record
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                dq.addLast(batch);
                // 追加到未完成的集合中
                incomplete.add(batch);
                // 封装成 RecordAppendResult 对象返回
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full),
     * close its memory records to release temporary resources (like compression streams buffers).
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        // 获取 deque 的最后一个 RecordBatch
        RecordBatch last = deque.peekLast();
        if (last != null) {
            // 尝试往该 RecordBatch 末尾追加消息
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null) {
                // 追加失败
                last.close();
            } else {
                // 追加成功，将结果封装成 RecordAppendResult 对象返回
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
            }
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator
     * for more than the configured requestTimeout due to metadata being unavailable
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        // 遍历各个 TopicPartition 对应的 RecordBatch 集合
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.isFull();
                        /*
                         * Check if the batch has expired.
                         * Expired batches are closed by maybeExpire, but callbacks are invoked after completing the iterations,
                         * since sends invoked from callbacks may append more batches to the deque being iterated.
                         * The batch is deallocated after callbacks are invoked.
                         */
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, lingerMs, isFull)) {
                            // 当前 RecordBatch 已经过期
                            expiredBatches.add(batch);
                            count++;
                            batchIterator.remove();
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty()) {
            log.trace("Expired {} batches in accumulator", count);
            for (RecordBatch batch : expiredBatches) {
                // 结束当前的请求
                batch.expirationDone();
                this.deallocate(batch);
            }
        }

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        batch.attempts++;
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            deque.addFirst(batch);
        }
    }

    /**
     * 获取集群中符合发送条件的节点集合
     *
     * Get a list of nodes whose partitions are ready to be sent,
     * and the earliest time at which any non-sendable partition will be ready;
     * Also return the flag for whether there are any unknown leaders for the accumulated partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     * {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     * is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     * <li>The record set is full</li>
     * <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     * <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions are immediately considered ready).</li>
     * <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        // 用于记录接收请求的节点
        Set<Node> readyNodes = new HashSet<>();
        // 记录下次执行 ready 判断的时间间隔
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // 记录找不到 leader 副本的分区对应的 topic 集合
        Set<String> unknownLeaderTopics = new HashSet<>();

        // 是否有线程在等待 BufferPool 分配空间
        boolean exhausted = this.free.queued() > 0;
        // 遍历每个 topic 分区及其 RecordBatch 队列，对每个分区的 leader 副本所在的节点执行判定
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();

            // 获取当前 topic 分区 leader 副本所在的节点
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                // 当前分区 leader 副本未知，但存在发往该分区的消息
                if (leader == null && !deque.isEmpty()) {
                    unknownLeaderTopics.add(part.topic());
                }
                // 如果需要保证消息顺序性，则不应该存在多个发往该 leader 副本节点且未完成的消息
                else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        // 当前为重试操作，且重试时间间隔未达到阈值时间
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        long waitedTimeMs = nowMs - batch.lastAttemptMs; // 重试等待的时间
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        boolean full = deque.size() > 1 || batch.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;

                        // 标记当前节点是否可以接收请求
                        boolean sendable = full // 1. 队列中有多个 RecordBatch，或第一个 RecordBatch 已满
                                || expired // 2. 当前等待重试的时间过长
                                || exhausted // 3. 有其他线程在等待 BufferPoll 分配空间，即本地消息缓存已满
                                || closed // 4. producer 已经关闭
                                || flushInProgress(); // 5. 有线程正在等待 flush 操作完成
                        if (sendable && !backingOff) {
                            // 允许发送消息，且当前为首次发送，或者重试等待时间已经较长，则记录目标 leader 副本所在节点
                            readyNodes.add(leader);
                        } else {
                            // 更新下次执行 ready 判定的时间间隔
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        // 封装结果返回
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * 当前 accumulator 是否存在未发送的消息
     *
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 将 (TopicPartition -> RecordBatch 集合)转换成（NodeId -> RecordBatch 集合）
     *
     * 返回结果：key 是节点 ID，value 是待发送到该节点的 RecordBatch 集合
     *
     * Drain all the data for the given nodes and collate them into a list of batches
     * that will fit within the specified size on a per-node basis.
     * This method attempts to avoid choosing the same topic-node over and over.
     *
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }

        // 记录转换后的结果，key 是目标节点 ID
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            // 获取当前节点上的分区信息
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            // 记录待发往当前节点的 RecordBatch 集合
            List<RecordBatch> ready = new ArrayList<>();
            /*
             * drainIndex 用于记录上次发送停止的位置，本次继续从当前位置开始发送，
             * 如果每次都是从 0 位置开始，可能会导致排在后面的分区饿死，可以看做是一个简单的负载均衡策略
             */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // 如果需要保证消息强顺序性，则不应该同时存在多个发往目标分区的消息
                if (!muted.contains(tp)) {
                    // 获取当前分区对应的 RecordBatch 集合
                    Deque<RecordBatch> deque = this.getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                // 重试 && 重试时间间隔未达到阈值时间
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // 仅发送第一次发送，或重试等待时间较长的消息
                                if (!backoff) {
                                    if (size + first.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // 单次消息数据量已达到上限，结束循环，一般对应一个请求的大小，防止请求消息过大
                                        break;
                                    } else {
                                        // 每次仅获取第一个 RecordBatch，并放入 read 列表中，这样给每个分区一个机会，保证公平，防止饥饿
                                        RecordBatch batch = deque.pollFirst();
                                        // 将当前 RecordBatch 设置为只读
                                        batch.close();
                                        size += batch.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                // 更新 drainIndex
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     *
     * 获取指定 TopicPartition 对应的发送队列，如果不存在则创建一个
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null) {
            return d;
        }
        d = new ArrayDeque<>();
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null) {
            return d;
        } else {
            return previous;
        }
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        incomplete.remove(batch);
        free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully.
     * It will fail all the incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            // 丢弃所有未发送完成的 RecordBatch
            this.abortBatches();
        } while (this.appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        this.abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = this.getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            this.deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /**
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        /** 标记追加过程中某个 RecordBatch 是否已满 */
        public final boolean batchIsFull;
        /** 标记追加过程中是否创建了新的 RecordBatch */
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /**
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }

    /**
     * A threadsafe helper class to hold RecordBatches that haven't been acked yet
     */
    private final static class IncompleteRecordBatches {

        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<>();
        }

        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }

        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed) {
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
                }
            }
        }

        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
