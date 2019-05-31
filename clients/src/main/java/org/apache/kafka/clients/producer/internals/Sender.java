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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster.
 * This thread makes metadata requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /** the state of each nodes connection */
    private final KafkaClient client;

    /** the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /** the metadata for the client */
    private final Metadata metadata;

    /**
     * the flag indicating whether the producer should guarantee the message order on the broker or not.
     *
     * 该参数使用 max.in.flight.requests.per.connection 进行配置，当 max.in.flight.requests.per.connection == 1 时为 true
     *
     * 用于指定生产者在收到服务器响应之前可以发送多少个消息，设置为 1 时可以保证消息按照发送的顺序写入服务器，即使发生重试。
     *
     * 消息的顺序性保证：
     * Kafka 可以保证同一个分区中消息的顺序性，不过如果参数配置不当也会违背顺序性保证。
     * 例如当允许生产者重试时将 max.in.flight.requests.per.connection 设置为大于 1 的值，如果生产者发送了两条位于同一个分区的消息 A 和 B，
     * 但是 A 失败 B 成功，此时生产者会重发消息 A，结果就变成了 B 排在了 A 的前面。
     * 要防止这种情况，可以将 max.in.flight.requests.per.connection 参数设置为 1，从而禁止生产者一次发送多条消息，
     * 不过这样会严重降低吞吐量，只有在对消息有严格要求时才这样做。
     */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /** the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /** true while the sender thread is still running */
    private volatile boolean running;

    /** true when the caller wants to ignore all unsent/inflight messages and force close. */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /** the max time to wait for the server to respond to the request */
    private final int requestTimeout;

    public Sender(KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  Metrics metrics,
                  Time time,
                  int requestTimeout) {
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metrics);
        this.requestTimeout = requestTimeout;
    }

    /**
     * The main run loop for the sender thread
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // 主循环，一直运行直到 KafkaProducer 被关闭
        while (running) {
            try {
                this.run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        /* 如果 KafkaProducer 被关闭，尝试发送剩余的消息 */

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        while (!forceClose // 不是强制关闭
                // 存在未发送或已发送待响应的请求
                && (this.accumulator.hasUnsent() || this.client.inFlightRequestCount() > 0)) {
            try {
                this.run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // 如果是强制关闭，忽略所有未发送和已发送待响应的请求
        if (forceClose) {
            // 丢弃所有未发送完成的消息
            this.accumulator.abortIncompleteBatches();
        }
        try {
            // 关闭网络连接
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     *
     * @param now The current POSIX time in milliseconds
     */
    void run(long now) {

        // 1. 计算需要以及可以向哪些节点发送请求
        Cluster cluster = metadata.fetch(); // 获取 kafka 集群信息
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now); // 计算需要向哪些节点发送请求

        // 2. 如果存在未知的 leader 副本对应的节点（对应的 topic 分区正在执行 leader 选举，或者对应的 topic 已经失效），标记需要更新缓存的集群元数据信息
        if (!result.unknownLeaderTopics.isEmpty()) {
            for (String topic : result.unknownLeaderTopics) this.metadata.add(topic);
            this.metadata.requestUpdate();
        }

        // 3. 遍历处理待发送请求的目标节点，基于网络 IO 检查对应节点是否可用，对于不可用的节点则剔除
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            // 检查目标节点是否准备好接收请求，如果未准备好但目标节点允许创建连接，则创建到目标节点的连接
            if (!this.client.ready(node, now)) {
                // 对于未准备好的节点，则从 ready 集合中删除
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        // 4. 获取每个节点待发送消息集合，其中 key 是目标 leader 副本所在节点 ID
        Map<Integer, List<RecordBatch>> batches =
                this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);

        // 5. 如果需要保证消息的强顺序性，则缓存对应 topic 分区对象，防止同一时间往同一个 topic 分区发送多条处于未完成状态的消息
        if (guaranteeMessageOrder) {
            // 将所有 RecordBatch 的 topic 分区对象加入到 muted 集合中
            // 防止同一时间往同一个 topic 分区发送多条处于未完成状态的消息
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        // 6. 处理本地过期的消息，返回 TimeoutException，并释放空间
        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        for (RecordBatch expiredBatch : expiredBatches)
            // update sensors
            sensors.recordErrors(expiredBatch.topicPartition.topic(), expiredBatch.recordCount);

        sensors.updateProduceRequestMetrics(batches);

        // 如果存在待发送的消息，则设置 pollTimeout 等于 0，这样可以立即发送请求，从而能够缩短剩余消息的缓存时间，避免堆积
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            pollTimeout = 0;
        }

        // 7. 发送请求到服务端，并处理服务端响应
        this.sendProduceRequests(batches, now);
        this.client.poll(pollTimeout, now);
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        this.initiateClose();
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, RecordBatch> batches, long now) {
        int correlationId = response.requestHeader().correlationId();
        // 如果是 disconnected 类型的响应
        if (response.wasDisconnected()) {
            log.trace("Cancelled request {} due to node {} being disconnected", response, response.destination());
            for (RecordBatch batch : batches.values())
                this.completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId, now);
        }
        // 如果是 API 版本不匹配的响应
        else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}", response, response.destination(), response.versionMismatch());
            for (RecordBatch batch : batches.values())
                this.completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.INVALID_REQUEST), correlationId, now);
        }
        // 其它类型的响应
        else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    RecordBatch batch = batches.get(tp);
                    this.completeBatch(batch, partResp, correlationId, now);
                }
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
                this.sensors.recordThrottleTime(produceResponse.getThrottleTime());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (RecordBatch batch : batches.values()) {
                    this.completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     */
    private void completeBatch(RecordBatch batch, ProduceResponse.PartitionResponse response, long correlationId, long now) {
        Errors error = response.error;
        // 异常响应，但是允许重试
        if (error != Errors.NONE && this.canRetry(batch, error)) {
            log.warn("Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}", correlationId, batch.topicPartition, retries - batch.attempts - 1, error);
            // 将消息重新添加到收集器中，等待再次发送
            this.accumulator.reenqueue(batch, now);
            this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
        }
        // 正常响应，或不允许重试的异常
        else {
            RuntimeException exception;
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                // 权限认证失败
                exception = new TopicAuthorizationException(batch.topicPartition.topic());
            } else {
                // 其他异常，如果是正常响应，则为 null
                exception = error.exception();
            }
            // 将响应信息传递给用户，并释放 RecordBatch 占用的空间
            batch.done(response.baseOffset, response.logAppendTime, exception);
            this.accumulator.deallocate(batch);
            if (error != Errors.NONE) {
                this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
            }
        }

        // 如果是集群元数据异常，则标记需要更新集群元数据信息
        if (error.exception() instanceof InvalidMetadataException) {
            if (error.exception() instanceof UnknownTopicOrPartitionException) {
                log.warn("Received unknown topic or partition error in produce request on partition {}. The topic/partition may not exist or the user may not have Describe access to it", batch.topicPartition);
            }
            metadata.requestUpdate();
        }

        // 释放已经处理完成的 topic 分区，对于需要保证消息强顺序性，以允许接收下一条消息
        if (guaranteeMessageOrder) {
            this.accumulator.unmutePartition(batch.topicPartition);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(RecordBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        // 遍历处理待发送消息集合，key 是目标节点 ID
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
            this.sendProduceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     *
     * @param now 当前时间戳
     * @param destination 目标节点 ID
     * @param acks 指定服务端响应此请求之前，需要多少副本成功复制了请求的消息
     * @param timeout 响应超时时间
     * @param batches 发送的 RecordBatch 集合
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        // 遍历 RecordBatch 集合，整理成 produceRecordsByPartition 和 recordsByPartition
        Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<>(batches.size());
        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records());
            recordsByPartition.put(tp, batch);
        }

        // 创建 ProduceRequest 请求构造器
        ProduceRequest.Builder requestBuilder = new ProduceRequest.Builder(acks, timeout, produceRecordsByPartition);

        // 创建回调对象，用于处理响应
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            @Override
            public void onComplete(ClientResponse response) {
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };

        String nodeId = Integer.toString(destination);

        // 创建 ClientRequest 请求对象，如果 acks 不等于 0 则表示期望获取服务端响应
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0, callback);
        // 缓存 ClientRequest 请求对象到 InFlightRequests 中
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * A collection of sensors for the sender
     */
    private class SenderMetrics {

        private final Metrics metrics;
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor produceThrottleTimeSensor;

        public SenderMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = "producer-metrics";

            this.batchSizeSensor = metrics.sensor("batch-size");
            MetricName m = metrics.metricName("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Avg());
            m = metrics.metricName("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            m = metrics.metricName("compression-rate-avg", metricGrpName, "The average compression rate of record batches.");
            this.compressionRateSensor.add(m, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            m = metrics.metricName("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Avg());
            m = metrics.metricName("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            m = metrics.metricName("request-latency-avg", metricGrpName, "The average request latency in ms");
            this.requestTimeSensor.add(m, new Avg());
            m = metrics.metricName("request-latency-max", metricGrpName, "The maximum request latency in ms");
            this.requestTimeSensor.add(m, new Max());

            this.produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
            m = metrics.metricName("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Avg());
            m = metrics.metricName("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            m = metrics.metricName("record-send-rate", metricGrpName, "The average number of records sent per second.");
            this.recordsPerRequestSensor.add(m, new Rate());
            m = metrics.metricName("records-per-request-avg", metricGrpName, "The average number of records per request.");
            this.recordsPerRequestSensor.add(m, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            m = metrics.metricName("record-retry-rate", metricGrpName, "The average per-second number of retried record sends");
            this.retrySensor.add(m, new Rate());

            this.errorSensor = metrics.sensor("errors");
            m = metrics.metricName("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors");
            this.errorSensor.add(m, new Rate());

            this.maxRecordSizeSensor = metrics.sensor("record-size-max");
            m = metrics.metricName("record-size-max", metricGrpName, "The maximum record size");
            this.maxRecordSizeSensor.add(m, new Max());
            m = metrics.metricName("record-size-avg", metricGrpName, "The average record size");
            this.maxRecordSizeSensor.add(m, new Avg());

            m = metrics.metricName("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.");
            this.metrics.addMetric(m, new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return client.inFlightRequestCount();
                }
            });
            m = metrics.metricName("metadata-age", metricGrpName, "The age in seconds of the current producer metadata being used.");
            metrics.addMetric(m, new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return (now - metadata.lastSuccessfulUpdate()) / 1000.0;
                }
            });
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);
                String metricGrpName = "producer-topic-metrics";

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName m = this.metrics.metricName("record-send-rate", metricGrpName, metricTags);
                topicRecordCount.add(m, new Rate());

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                m = this.metrics.metricName("byte-rate", metricGrpName, metricTags);
                topicByteRate.add(m, new Rate());

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                m = this.metrics.metricName("compression-rate", metricGrpName, metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                m = this.metrics.metricName("record-retry-rate", metricGrpName, metricTags);
                topicRetrySensor.add(m, new Rate());

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                m = this.metrics.metricName("record-error-rate", metricGrpName, metricTags);
                topicErrorSensor.add(m, new Rate());
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<RecordBatch>> batches) {
            long now = time.milliseconds();
            for (List<RecordBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (RecordBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    this.maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.sizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRate());

                    // global metrics
                    this.batchSizeSensor.record(batch.sizeInBytes(), now);
                    this.queueTimeSensor.record(batch.drainedMs - batch.createdMs, now);
                    this.compressionRateSensor.record(batch.compressionRate());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null) {
                topicRetrySensor.record(count, now);
            }
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null) {
                topicErrorSensor.record(count, now);
            }
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null) {
                    nodeRequestTime.record(latency, now);
                }
            }
        }

        public void recordThrottleTime(long throttleTimeMs) {
            this.produceThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
        }

    }

}
