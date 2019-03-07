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

package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * 订阅指定 topic，并为消费者自动分配分区
     *
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * 手动订阅 topic 并指定分区
     *
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * 从服务端获取消息
     *
     * @see KafkaConsumer#poll(long)
     */
    ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * 指定起始消费位置
     *
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);

    /**
     * @see KafkaConsumer#committed(TopicPartition)
     */
    OffsetAndMetadata committed(TopicPartition partition);

    /**
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * 暂停
     *
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * 继续
     *
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#offsetsForTimes(java.util.Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * @see KafkaConsumer#beginningOffsets(java.util.Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#endOffsets(java.util.Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#close()
     */
    @Override
    void close();

    /**
     * @see KafkaConsumer#close(long, TimeUnit)
     */
    void close(long timeout, TimeUnit unit);

    /**
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();

}
