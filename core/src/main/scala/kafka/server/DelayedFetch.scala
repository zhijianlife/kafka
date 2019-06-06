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

package kafka.server

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

    override def toString: String = "[startOffsetMetadata: " + startOffsetMetadata + ", fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int, // 读取的最小字节数
                         fetchMaxBytes: Int, // 读取的最大字节数
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean, // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
                         fetchOnlyCommitted: Boolean, // 是否只读已完成提交的消息（即 HW 之前的消息），如果是来自消费者的请求则该参数是 true，如果是 follower 则该参数是 false
                         isFromFollower: Boolean, // fetch 请求是否来自 follower
                         replicaId: Int, // fetch 的副本 ID
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) { // 记录每个 topic 分区的 fetch 状态

    override def toString: String = s"[minBytes: $fetchMinBytes, onlyLeader: $fetchOnlyLeader, " +
            s"onlyCommitted: $fetchOnlyCommitted, partitionStatus: $fetchPartitionStatus]"
}

/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long, // 延时任务延迟时长
                   fetchMetadata: FetchMetadata, // 记录对应 topic 分区的状态信息，用于判定当前延时任务是否满足执行条件
                   replicaManager: ReplicaManager, // 副本管理器
                   quota: ReplicaQuota,
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit // 响应回调函数
                  ) extends DelayedOperation(delayMs) {

    /**
     * The operation can be completed if:
     *
     * Case A: This broker is no longer the leader for some partitions it tries to fetch
     * Case B: This broker does not know of some partitions it tries to fetch
     * Case C: The fetch offset locates not on the last segment of the log
     * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
     *
     * 1. 对应 topic 分区的 leader 副本不再位于当前 broker 节点上
     * 2. 请求 fetch 的 topic 分区在当前 broker 节点上找不到
     * 3. 请求 fetch 的 offset 不位于 activeSegment 上，可能已经创建了新的 activeSegment，或者 Log 被截断
     * 4. 累计读取的字节数已经达到最小字节限制
     *
     * Upon completion, should return whatever data is available for each valid partition
     */
    override def tryComplete(): Boolean = {
        var accumulatedSize = 0
        var accumulatedThrottledSize = 0
        // 遍历处理当前延时任务关注的所有 topic 分区的状态信息
        fetchMetadata.fetchPartitionStatus.foreach { case (topicPartition, fetchStatus) =>
            // 获取上次拉取消息的结束 offset
            val fetchOffset = fetchStatus.startOffsetMetadata
            try {
                if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
                    // 获取 topic 分区的 leader 副本
                    val replica = replicaManager.getLeaderReplicaIfLocal(topicPartition)
                    // 依据发起请求是消费者还是 follower 来确定拉取消息的结束 offset
                    val endOffset = if (fetchMetadata.fetchOnlyCommitted) replica.highWatermark else replica.logEndOffset

                    // 校验上次拉取消息完成之后，endOffset 是否发生变化，如果未发送变化则说明数据不够，没有继续的必要，否则继续执行
                    if (endOffset.messageOffset != fetchOffset.messageOffset) {
                        if (endOffset.onOlderSegment(fetchOffset)) {
                            // 条件 3，endOffset 相对于 fetchOffset 较小，说明请求不位于当前 activeSegment 上
                            debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicPartition))
                            return forceComplete()
                        } else if (fetchOffset.onOlderSegment(endOffset)) {
                            // 条件 3，fetchOffset 位于 endOffset 之前，但是 fetchOffset 落在老的 LogSegment 上，而非 activeSegment 上
                            debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                            if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId)) return forceComplete()
                        } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                            // fetchOffset 和 endOffset 位于同一个 LogSegment 上，计算累计读取的字节数
                            val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                            if (quota.isThrottled(topicPartition)) accumulatedThrottledSize += bytesAvailable
                            else accumulatedSize += bytesAvailable
                        }
                    }
                }
            } catch {
                // 条件 2，请求 fetch 的 topic 分区在当前 broker 节点上找不到
                case _: UnknownTopicOrPartitionException => // Case B
                    debug("Broker no longer know of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
                    return forceComplete()
                // 条件 1，对应 topic 分区的 leader 副本不再位于当前 broker 节点上
                case _: NotLeaderForPartitionException => // Case A
                    debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
                    return forceComplete()
            }
        }

        // 条件 4，累计读取的字节数已经达到最小字节限制
        if (accumulatedSize >= fetchMetadata.fetchMinBytes
                || ((accumulatedSize + accumulatedThrottledSize) >= fetchMetadata.fetchMinBytes && !quota.isQuotaExceeded()))
            forceComplete()
        else
            false
    }

    override def onExpiration() {
        if (fetchMetadata.isFromFollower)
            DelayedFetchMetrics.followerExpiredRequestMeter.mark()
        else
            DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
    }

    /**
     * Upon completion, read whatever data is available and pass to the complete callback
     */
    override def onComplete() {
        val logReadResults = replicaManager.readFromLocalLog(
            replicaId = fetchMetadata.replicaId,
            fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
            readOnlyCommitted = fetchMetadata.fetchOnlyCommitted,
            fetchMaxBytes = fetchMetadata.fetchMaxBytes,
            hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
            readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
            quota = quota
        )

        val fetchPartitionData = logReadResults.map { case (tp, result) =>
            tp -> FetchPartitionData(result.error, result.hw, result.info.records)
        }

        responseCallback(fetchPartitionData)
    }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
    private val FetcherTypeKey = "fetcherType"
    val followerExpiredRequestMeter: Meter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
    val consumerExpiredRequestMeter: Meter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

