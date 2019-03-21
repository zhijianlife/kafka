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

package kafka.cluster

import kafka.common.KafkaException
import kafka.log.Log
import kafka.server.{LogOffsetMetadata, LogReadResult}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

/**
 * 表示一个副本
 *
 * @param brokerId
 * @param partition
 * @param time
 * @param initialHighWatermarkValue
 * @param log
 */
class Replica(val brokerId: Int, // 当前副本所在的 broker 的 ID，可以将该 ID 与当前所在 broker 的 ID 比对来区分当前副本是本地副本还是远程副本
              val partition: Partition, // 当前副本对应的分区
              time: Time = Time.SYSTEM,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None // 本地副本对应的 Log 对象，对于远程副本来说，该字段为空，通过该字段可以区分是本地副本还是远程副本
             ) extends Logging {

    /**
     * the high watermark offset value, in non-leader replicas only its message offsets are kept
     *
     * 记录 HW 值，消费者只能读取 HW 之前的消息，其后的消息对消费者是不可见的，
     * 此字段由 leader 副本负责更新和维护，更新时机是消息被 ISR 集合中所有副本成功同步，即消息被成功提交
     */
    @volatile private[this] var highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue)

    /**
     * the log end offset value, kept in all replicas;
     * for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
     *
     * - 对于本地副本，此字段记录的是追加到 Log 中的最新消息的 offset，可以直接从 Log#nextOffsetMetadata 字段中获取；
     * - 对于远程副本，字段含义相同，但是由其它 broker 发送请求来更新该值，不能从本地获取到
     */
    @volatile private[this] var logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

    // The log end offset value at the time the leader received the last FetchRequest from this follower
    // This is used to determine the lastCaughtUpTimeMs of the follower
    @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

    // The time when the leader received the last FetchRequest from this follower
    // This is used to determine the lastCaughtUpTimeMs of the follower
    @volatile private[this] var lastFetchTimeMs = 0L

    // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
    // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
    @volatile private[this] var _lastCaughtUpTimeMs = 0L

    val topicPartition: TopicPartition = partition.topicPartition

    def isLocal: Boolean = log.isDefined

    def lastCaughtUpTimeMs: Long = _lastCaughtUpTimeMs

    /**
     * If the FetchRequest reads up to the log end offset of the leader when the current fetch request is received,
     * set `lastCaughtUpTimeMs` to the time when the current fetch request was received.
     *
     * Else if the FetchRequest reads up to the log end offset of the leader when the previous fetch request was received,
     * set `lastCaughtUpTimeMs` to the time when the previous fetch request was received.
     *
     * This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it lags behind leader's LEO
     * by at most `replicaLagTimeMaxMs`. These semantics allow a follower to be added to the ISR even if the offset of its
     * fetch request is always smaller than the leader's LEO, which can happen if small produce requests are received at
     * high frequency.
     */
    def updateLogReadResult(logReadResult: LogReadResult) {
        if (logReadResult.info.fetchOffsetMetadata.messageOffset >= logReadResult.leaderLogEndOffset)
            _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, logReadResult.fetchTimeMs)
        else if (logReadResult.info.fetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
                 _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

        logEndOffset = logReadResult.info.fetchOffsetMetadata
        lastFetchLeaderLogEndOffset = logReadResult.leaderLogEndOffset
        lastFetchTimeMs = logReadResult.fetchTimeMs
    }

    def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long) {
        lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
        lastFetchTimeMs = curTimeMs
        _lastCaughtUpTimeMs = lastCaughtUpTimeMs
    }

    private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
        if (isLocal) {
            throw new KafkaException(s"Should not set log end offset on partition $topicPartition's local replica $brokerId")
        } else {
            logEndOffsetMetadata = newLogEndOffset
            trace(s"Setting log end offset for replica $brokerId for partition $topicPartition to [$logEndOffsetMetadata]")
        }
    }

    def logEndOffset: LogOffsetMetadata =
        if (isLocal)
            log.get.logEndOffsetMetadata
        else
            logEndOffsetMetadata

    def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
        if (isLocal) {
            highWatermarkMetadata = newHighWatermark
            trace(s"Setting high watermark for replica $brokerId partition $topicPartition to [$newHighWatermark]")
        } else {
            throw new KafkaException(s"Should not set high watermark on partition $topicPartition's non-local replica $brokerId")
        }
    }

    def highWatermark: LogOffsetMetadata = highWatermarkMetadata

    def convertHWToLocalOffsetMetadata(): Unit = {
        if (isLocal) {
            highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
        } else {
            throw new KafkaException(s"Should not construct complete high watermark on partition $topicPartition's non-local replica $brokerId")
        }
    }

    override def equals(that: Any): Boolean = that match {
        case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
        case _ => false
    }

    override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId

    override def toString: String = {
        val replicaString = new StringBuilder
        replicaString.append("ReplicaId: " + brokerId)
        replicaString.append("; Topic: " + partition.topic)
        replicaString.append("; Partition: " + partition.partitionId)
        replicaString.append("; isLocal: " + isLocal)
        replicaString.append("; lastCaughtUpTimeMs: " + lastCaughtUpTimeMs)
        if (isLocal) replicaString.append("; Highwatermark: " + highWatermark)
        replicaString.toString
    }
}
