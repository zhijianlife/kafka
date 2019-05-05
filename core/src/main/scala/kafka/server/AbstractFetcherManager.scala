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

import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.{Map, Set, mutable}

abstract class AbstractFetcherManager(protected val name: String,
                                      clientId: String,
                                      numFetchers: Int = 1
                                     ) extends Logging with KafkaMetricsGroup {

    /**
     * 管理 AbstractFetcherThread
     *
     * map of (source broker_id, fetcher_id per source broker) => fetcher
     */
    private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, AbstractFetcherThread]
    private val mapLock = new Object
    this.logIdent = "[" + name + "] "

    newGauge(
        "MaxLag",
        new Gauge[Long] {
            // current max lag across all fetchers/topics/partitions
            def value: Long = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
                fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
                    curMaxThread.max(fetcherLagStatsEntry._2.lag)
                }).max(curMaxAll)
            })
        },
        Map("clientId" -> clientId)
    )

    newGauge(
        "MinFetchRate", {
            new Gauge[Double] {
                // current min fetch rate across all fetchers/topics/partitions
                def value: Double = {
                    val headRate: Double =
                        fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

                    fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
                        fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
                    })
                }
            }
        },
        Map("clientId" -> clientId)
    )

    private def getFetcherId(topic: String, partitionId: Int): Int = {
        Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
    }

    // to be defined in subclass to create a specific fetcher
    def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread

    /**
     * 让 follower 副本从指定的 offset 开始与 leader 副本进行同步
     *
     * @param partitionAndOffsets
     */
    def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, BrokerAndInitialOffset]) {
        mapLock synchronized {
            val partitionsPerFetcher = partitionAndOffsets.groupBy {
                case (topicPartition, brokerAndInitialOffset) =>
                    /*
                     * 通过分区所属的 topic 和分区编号计算得到对应的 fetcher 线程 ID，然后与 broker 的网络位置信息组成 key，并进行分组。
                     * 每组对应相同的 fetcher 线程。每个线程只连接一个 broker，但可以为多个分区的 follower 副本执行同步操作
                     */
                    BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicPartition.topic, topicPartition.partition))
            }

            // 按照 key 查找对应的 fetcher 线程，查找不到就创建新的 fetcher 线程并启动
            for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
                var fetcherThread: AbstractFetcherThread = null
                fetcherThreadMap.get(brokerAndFetcherId) match {
                    // 找到对应的 fether 线程
                    case Some(f) => fetcherThread = f
                    // 没有找到，创建并启动新的 fetcher 线程
                    case None =>
                        fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
                        // 添加到 fetcherThreadMap 中进行管理
                        fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
                        fetcherThread.start()
                }

                // 将分区信息以及同步起始位置传递给 fetcher 线程，并唤醒 fetcher 线程，开始同步
                fetcherThreadMap(brokerAndFetcherId).addPartitions(
                    partitionAndOffsets.map {
                        case (tp, brokerAndInitOffset) => tp -> brokerAndInitOffset.initOffset
                    })
            }
        }

        info("Added fetcher for partitions %s".format(partitionAndOffsets.map { case (topicPartition, brokerAndInitialOffset) =>
            "[" + topicPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "
        }))
    }

    /**
     * 停止指定 follower 副本的同步操作
     *
     * @param partitions
     */
    def removeFetcherForPartitions(partitions: Set[TopicPartition]) {
        mapLock synchronized {
            // 将 TP 信息从 fetcher 线程中移除
            for (fetcher <- fetcherThreadMap.values)
                fetcher.removePartitions(partitions)
        }
        info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
    }

    /**
     * 如果 fetcher 线程不再为任何分区的 follower 的副本执行同步操作，可以通过调用本方法将其停止
     */
    def shutdownIdleFetcherThreads() {
        mapLock synchronized {
            val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
            for ((key, fetcher) <- fetcherThreadMap) {
                if (fetcher.partitionCount <= 0) {
                    fetcher.shutdown()
                    keysToBeRemoved += key
                }
            }
            fetcherThreadMap --= keysToBeRemoved
        }
    }

    def closeAllFetchers() {
        mapLock synchronized {
            for ((_, fetcher) <- fetcherThreadMap) {
                fetcher.initiateShutdown()
            }

            for ((_, fetcher) <- fetcherThreadMap) {
                fetcher.shutdown()
            }
            fetcherThreadMap.clear()
        }
    }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, // broker 的网络位置信息
                              fetcherId: Int // 对应的 fetcher 线程的 ID
                             )

case class BrokerAndInitialOffset(broker: BrokerEndPoint, // broker 的网络位置信息
                                  initOffset: Long // 同步的起始 offset
                                 )
