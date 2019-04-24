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
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, // 对应 topic 分区最后一条消息的 offset
                                  responseStatus: PartitionResponse // 记录 ProducerResponse 中的错误码
                                 ) {

    /** 标识是否正在等待 ISR 集合中的其它副本从 leader 副本同步 requiredOffset 之前的消息 */
    @volatile var acksPending = false

    override def toString: String = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
            .format(acksPending, responseStatus.error.code, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short, // 对应 acks 值
                           produceStatus: Map[TopicPartition, ProducePartitionStatus] // 记录每个 topic 分区对应的 ProducePartitionStatus 对象
                          ) {

    override def toString: String = "[requiredAcks: %d, partitionStatus: %s]".format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long, // 延迟时长
                     produceMetadata: ProduceMetadata, // 用于判断 DelayedProduce 是否满足执行条件
                     replicaManager: ReplicaManager, // 副本管理器
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit // 回调函数，在任务满足条件或到期时执行
                    ) extends DelayedOperation(delayMs) {

    // 依据消息写入 leader 分区操作的错误码对 produceMetadata 的 produceStatus 进行初始化
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
        if (status.responseStatus.error == Errors.NONE) {
            // 对应 topic 分区消息写入 leader 副本成功，等待其它副本同步
            status.acksPending = true
            status.responseStatus.error = Errors.REQUEST_TIMED_OUT // 默认错误码
        } else {
            // 对应 topic 分区消息写入 leader 副本失败，无需等待
            status.acksPending = false
        }

        trace("Initial partition status for %s is %s".format(topicPartition, status))
    }

    /**
     * The delayed produce operation can be completed if every partition
     * it produces to is satisfied by one of the following:
     *
     * Case A: This broker is no longer the leader: set an error in response
     * Case B: This broker is the leader:
     *   B.1 - If there was a local error thrown while checking if at least requiredAcks
     * replicas have caught up to this operation: set an error in response
     *   B.2 - Otherwise, set the response with no error.
     *
     * 检测当前 DelayedProduce 是否满足执行条件（如下），如果满足则调用 forceComplete 方法：
     *
     * 1. 对应分区的 leader 副本不再位于当前 broker 上；
     * 2. ISR 集合中的所有副本均完成了同步
     * 3. 出现异常
     */
    override def tryComplete(): Boolean = {
        // 遍历处理所有的 topic 分区
        produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
            trace(s"Checking produce satisfaction for $topicPartition, current status $status")
            // 仅处理正在等待 follower 副本复制的分区
            if (status.acksPending) {
                val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
                    case Some(partition) =>
                        // 检测对应分区本次追加的最后一条消息是否已经被 ISR 集合中所有的 follower 副本同步
                        partition.checkEnoughReplicasReachOffset(status.requiredOffset)
                    case None =>
                        // 找不到对应的分区对象，说明对应分区的 leader 副本已经不再当前 broker 节点上
                        (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
                // 出现异常 || 所有的 ISR 副本已经同步完成
                if (error != Errors.NONE || hasEnough) {
                    status.acksPending = false
                    status.responseStatus.error = error
                }
            }
        }

        // 如果所有的 topic 分区都已经满足了 DelayedProduce 的执行条件
        if (!produceMetadata.produceStatus.values.exists(_.acksPending))
            forceComplete()
        else
            false
    }

    override def onExpiration() {
        produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
            if (status.acksPending) {
                DelayedProduceMetrics.recordExpiration(topicPartition)
            }
        }
    }

    /**
     * Upon completion, return the current response status along with the error code per partition
     */
    override def onComplete() {
        // 为每个 topic 分区生成对应的响应状态
        val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
        // 执行回调函数
        responseCallback(responseStatus)
    }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

    private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

    private val partitionExpirationMeterFactory = (key: TopicPartition) =>
        newMeter("ExpiresPerSec",
            "requests",
            TimeUnit.SECONDS,
            tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
    private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

    def recordExpiration(partition: TopicPartition) {
        aggregateExpirationMeter.mark()
        partitionExpirationMeters.getAndMaybePut(partition).mark()
    }
}

