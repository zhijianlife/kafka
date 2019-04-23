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
import kafka.network._
import kafka.utils._
import org.apache.kafka.common.utils.{Time, Utils}

/**
 * A thread that answers kafka requests.
 *
 * 负责从请求队列中获取请求，并调用 KafkaApis 的 handle 方法进行处理
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: Int,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
    this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

    override def run() {
        while (true) {
            try {
                var req: RequestChannel.Request = null
                while (req == null) {
                    val startSelectTime = time.nanoseconds
                    // 从请求队列中获取请求 Processor 分配的请求
                    req = requestChannel.receiveRequest(300)
                    val idleTime = time.nanoseconds - startSelectTime
                    aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
                }

                // 如果是 AllDone 请求，则退出当前线程
                if (req eq RequestChannel.AllDone) {
                    debug("Kafka request handler %d on broker %d received shut down command".format(id, brokerId))
                    return
                }
                req.requestDequeueTimeMs = time.milliseconds
                trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
                // 处理请求，将响应写回到对应 Processor 的响应队列中，并唤醒 Processor 线程
                apis.handle(req)
            } catch {
                case e: Throwable => error("Exception when handling request", e)
            }
        }
    }

    def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
}

/**
 * 简单的线程池实现，用来管理所有 KafkaRequestHandler 的线程
 *
 * @param brokerId
 * @param requestChannel
 * @param apis
 * @param time
 * @param numThreads
 */
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {

    /* a meter to track the average free capacity of the request handlers */
    private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

    this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "

    /** 记录执行 KafkaRequestHandler 的线程对象 */
    val threads = new Array[Thread](numThreads)
    /** KafkaRequestHandler 集合 */
    val runnables = new Array[KafkaRequestHandler](numThreads)

    // 创建并启动运行 KafkaRequestHandler 的线程
    for (i <- 0 until numThreads) {
        runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time)
        threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
        threads(i).start()
    }

    def shutdown() {
        info("shutting down")
        for (handler <- runnables)
            handler.shutdown()
        for (thread <- threads)
            thread.join()
        info("shut down completely")
    }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
    val tags: scala.collection.Map[String, String] = name match {
        case None => scala.collection.Map.empty
        case Some(topic) => Map("topic" -> topic)
    }

    val messagesInRate: Meter = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)
    val bytesInRate: Meter = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags)
    val bytesOutRate: Meter = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)
    val bytesRejectedRate: Meter = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags)
    val failedProduceRequestRate: Meter = newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
    val failedFetchRequestRate: Meter = newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
    val totalProduceRequestRate: Meter = newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
    val totalFetchRequestRate: Meter = newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)

    def close() {
        removeMetric(BrokerTopicStats.MessagesInPerSec, tags)
        removeMetric(BrokerTopicStats.BytesInPerSec, tags)
        removeMetric(BrokerTopicStats.BytesOutPerSec, tags)
        removeMetric(BrokerTopicStats.BytesRejectedPerSec, tags)
        removeMetric(BrokerTopicStats.FailedProduceRequestsPerSec, tags)
        removeMetric(BrokerTopicStats.FailedFetchRequestsPerSec, tags)
        removeMetric(BrokerTopicStats.TotalProduceRequestsPerSec, tags)
        removeMetric(BrokerTopicStats.TotalFetchRequestsPerSec, tags)
    }
}

object BrokerTopicStats extends Logging {
    val MessagesInPerSec = "MessagesInPerSec"
    val BytesInPerSec = "BytesInPerSec"
    val BytesOutPerSec = "BytesOutPerSec"
    val BytesRejectedPerSec = "BytesRejectedPerSec"
    val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
    val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
    val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
    val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"

    private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
    private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
    private val allTopicsStats = new BrokerTopicMetrics(None)

    def getBrokerAllTopicsStats: BrokerTopicMetrics = allTopicsStats

    def getBrokerTopicStats(topic: String): BrokerTopicMetrics = {
        stats.getAndMaybePut(topic)
    }

    def removeMetrics(topic: String) {
        val metrics = stats.remove(topic)
        if (metrics != null)
            metrics.close()
    }
}
