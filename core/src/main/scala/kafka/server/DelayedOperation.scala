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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true.
 * onComplete() can be triggered by either forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * 表示延迟的、异步的操作
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

    /** 标识当前异步操作是否执行完成 */
    private val completed = new AtomicBoolean(false)

    /**
     * 如果延迟操作没有完成，则先将任务从时间轮中删除，然后调用 onComplete() 方法执行具体的逻辑
     *
     * Force completing the delayed operation, if not already completed.
     * This function can be triggered when
     *
     * 1. The operation has been verified to be completable inside tryComplete()
     * 2. The operation has expired and hence needs to be completed right now
     *
     * Return true iff the operation is completed by the caller: note that
     * concurrent threads can try to complete the same operation, but only
     * the first thread will succeed in completing the operation and return
     * true, others will still return false
     */
    def forceComplete(): Boolean = {
        if (completed.compareAndSet(false, true)) { // CAS 操作修改 completed 字段
            // 将当前延时任务从时间轮中移除
            this.cancel()
            // 立即触发执行延时任务
            this.onComplete()
            true
        } else {
            false
        }
    }

    /**
     * 检测延时任务是否已经完成
     */
    def isCompleted: Boolean = completed.get()

    /**
     * Call-back to execute when a delayed operation gets expired and hence forced to complete.
     *
     * 延时任务到期时的回调方法
     */
    def onExpiration(): Unit

    /**
     * Process for completing an operation; This function needs to be defined
     * in subclasses and will be called exactly once in forceComplete()
     *
     * 延时任务的具体逻辑，该方法只能被调用一次
     */
    def onComplete(): Unit

    /**
     * 检测执行条件是否满足，如果满足则会调用 forceComplete 方法
     */
    def tryComplete(): Boolean

    /**
     * 方法 tryComplete 的线程安全版本
     */
    def safeTryComplete(): Boolean = {
        synchronized {
            tryComplete()
        }
    }

    /**
     * run() method defines a task that is executed on timeout
     *
     * DelayedOperation 到期时会提交给线程池执行
     */
    override def run(): Unit = {
        if (forceComplete())
            onExpiration()
    }
}

object DelayedOperationPurgatory {

    def apply[T <: DelayedOperation](purgatoryName: String,
                                     brokerId: Int = 0,
                                     purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
        val timer = new SystemTimer(purgatoryName)
        new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
    }

}

/**
 * A helper purgatory（炼狱） class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 *
 * 辅助类，提供了管理 DelayedOperation，以及处理到期 DelayedOperation 的功能
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer, // 定时器
                                                       brokerId: Int = 0, // 所在 broker 节点 ID
                                                       purgeInterval: Int = 1000, // 执行清理操作的阈值
                                                       reaperEnabled: Boolean = true // 是否启用后台指针推进器
                                                      ) extends Logging with KafkaMetricsGroup {

    /** 用于管理 DelayedOperation，其中 key 是 Watcher 中的 DelayedOperation 集合所关心的对象 */
    private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    /** watchersForKey 读写锁 */
    private val removeWatchersLock = new ReentrantReadWriteLock()

    /** 记录当前 DelayedOperationPurgatory 中延时任务的个数 */
    private[this] val estimatedTotalOperations = new AtomicInteger(0)

    /**
     * 主要具备 2 个作用：
     *  1. 推进时间指针
     *  2. 定期清理 watchersForKey 中已经完成的延时任务
     */
    private val expirationReaper = new ExpiredOperationReaper()

    private val metricsTags = Map("delayedOperation" -> purgatoryName)

    newGauge(
        "PurgatorySize",
        new Gauge[Int] {
            def value: Int = watched()
        },
        metricsTags
    )

    newGauge(
        "NumDelayedOperations",
        new Gauge[Int] {
            def value: Int = delayed()
        },
        metricsTags
    )

    if (reaperEnabled)
        expirationReaper.start()

    /**
     * 检测延时任务是否已经执行完成，如果未完成则添加到对应 key 的 Watcher 集合和 SystemTimer 中
     *
     * Check if the operation can be completed, if not watch it based on the given watch keys
     *
     * Note that a delayed operation can be watched on multiple keys. It is possible that
     * an operation is completed after it has been added to the watch list for some, but
     * not all of the keys. In this case, the operation is considered completed and won't
     * be added to the watch list of the remaining keys. The expiration reaper thread will
     * remove this operation from any watcher list in which the operation exists.
     *
     * @param operation the delayed operation to be checked
     * @param watchKeys keys for bookkeeping the operation
     * @return true iff the delayed operations can be completed by the caller
     */
    def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
        assert(watchKeys.nonEmpty, "The watch key list can't be empty")

        // 1. 调用延时任务的 tryComplete 方法，尝试完成延迟操作
        var isCompletedByMe = operation.safeTryComplete()
        if (isCompletedByMe)
            return true // 已经执行完成，则直接返回

        // 2. 遍历处理 watchKeys，将 key 对应的 DelayedOperation 添加到 key 对应的 Watcher 中
        var watchCreated = false
        for (key <- watchKeys) {
            // 如果待添加的延时任务已经执行完成，则放弃添加
            if (operation.isCompleted)
                return false

            // 添加延时任务添加到对应 key 的 Watcher 集合中，用于从时间维度以外的维度触发延时任务执行
            this.watchForOperation(key, operation)

            if (!watchCreated) {
                watchCreated = true
                // 延时任务计数加 1，一个延时任务可能会被添加到多个 key 对应的 Watcher 集合中，但是任务计数只会增加 1 次
                estimatedTotalOperations.incrementAndGet()
            }
        }

        // 3. 再次调用延时任务的 tryComplete 方法，尝试完成延迟操作
        isCompletedByMe = operation.safeTryComplete()
        if (isCompletedByMe)
            return true

        // 4. 对于未执行的延时任务，尝试添加到 SystemTimer 中，用于从时间维度触发延时任务执行
        if (!operation.isCompleted) {
            timeoutTimer.add(operation)
            // 再次检测延时任务的执行情况，如果已经完成则从 SystemTimer 中移除
            if (operation.isCompleted) {
                operation.cancel()
            }
        }

        false
    }

    /**
     * 依据传入的 key 尝试执行对应 Watcher 中的延时任务集合，并从集合中移除已经执行完成的任务
     *
     * @return the number of completed operations during this process
     */
    def checkAndComplete(key: Any): Int = {
        // 获取 key 对应的 Watcher 集合
        val watchers = inReadLock(removeWatchersLock) {
            watchersForKey.get(key)
        }
        if (watchers == null) 0
        // 如果存在对应的 Watcher 集合，则对该集合中未完成的任务尝试触发执行，并移除已经执行完成的任务
        else watchers.tryCompleteWatched()
    }

    /**
     * Return the total size of watch lists the purgatory. Since an operation may be watched
     * on multiple lists, and some of its watched entries may still be in the watch lists
     * even when it has been completed, this number may be larger than the number of real operations watched
     *
     * 获取记录的延时任务总个数，计数不一定准确
     */
    def watched(): Int = allWatchers.map(_.countWatched).sum

    /**
     * Return the number of delayed operations in the expiry queue
     *
     * 获取等待执行的任务总数
     */
    def delayed(): Int = timeoutTimer.size

    /**
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     *
     * 返回所有的 Watcher 集合
     */
    private def allWatchers: Iterable[Watchers] = inReadLock(removeWatchersLock) {
        watchersForKey.values
    }

    /**
     * 获取 key 对应的 Watchers 集合，并将 operation 添加到该集合中
     */
    private def watchForOperation(key: Any, operation: T) {
        inReadLock(removeWatchersLock) {
            val watcher = watchersForKey.getAndMaybePut(key)
            watcher.watch(operation)
        }
    }

    /**
     * 对于 Watcher 已空的 key，尝试从 watchersForKey 中移除
     */
    private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
        inWriteLock(removeWatchersLock) {
            // if the current key is no longer correlated to the watchers to remove, skip
            if (watchersForKey.get(key) != watchers)
                return

            if (watchers != null && watchers.isEmpty) {
                watchersForKey.remove(key)
            }
        }
    }

    /**
     * Shutdown the expire reaper thread
     */
    def shutdown() {
        if (reaperEnabled)
            expirationReaper.shutdown()
        timeoutTimer.shutdown()
    }

    def advanceClock(timeoutMs: Long) {
        // 尝试推进时间轮指针
        timeoutTimer.advanceClock(timeoutMs)

        // 如果当前炼狱中的已完成任务数超过给定阈值 purgeInterval，则尝试清理
        if (estimatedTotalOperations.get - delayed > purgeInterval) {
            estimatedTotalOperations.getAndSet(delayed())
            debug("Begin purging watch lists")
            // 遍历各个 Watcher 集合，执行清理操作
            val purged = allWatchers.map(_.purgeCompleted()).sum
            debug("Purged %d elements from watch lists.".format(purged))
        }
    }

    /**
     * 封装监听指定 key 的延时任务集合
     */
    private class Watchers(val key: Any) {

        /** 监听指定 key 的延时任务同步队列 */
        private[this] val operations = new ConcurrentLinkedQueue[T]()

        /** 延时任务个数 */
        def countWatched: Int = operations.size

        /** 标识延时任务个数是否为 0 */
        def isEmpty: Boolean = operations.isEmpty

        /**
         * 将延时任务添加到队列
         *
         * @param t
         */
        def watch(t: T) {
            operations.add(t)
        }

        /**
         * 遍历队列，移除已执行完成的延时任务，对于未完成的延时任务，尝试触发执行，
         * 如果 key 对应的 Watcher 为空，则将 key 从 watchersForKey 中移除
         *
         * @return 返回执行完成的任务数
         */
        def tryCompleteWatched(): Int = {
            var completed = 0

            // 遍历处理当前 Watcher 集合中的延时任务
            val iter = operations.iterator()
            while (iter.hasNext) {
                val curr = iter.next()
                // 如果对应的延时任务已经执行完成，则从 Watcher 集合中移除
                if (curr.isCompleted) {
                    iter.remove()
                }
                // 尝试执行延时任务
                else if (curr.safeTryComplete()) {
                    iter.remove()
                    completed += 1
                }
            }

            // 如果 key 对应的 Watcher 集合已空，则将 key 从 watchersForKey 中移除
            if (operations.isEmpty) removeKeyIfEmpty(key, this)

            completed
        }

        /**
         * 负责清理 Watcher 集合，移除已完成的延时任务
         *
         * @return
         */
        def purgeCompleted(): Int = {
            var purged = 0

            val iter = operations.iterator()
            while (iter.hasNext) {
                val curr = iter.next()
                if (curr.isCompleted) {
                    iter.remove()
                    purged += 1
                }
            }

            if (operations.isEmpty) removeKeyIfEmpty(key, this)

            purged
        }
    } // ~ end of Watchers

    /**
     * 用于推进时间轮指针，并定期清理所有 Watcher 集合中的任务队列，将已完成的延时任务从队列中移除
     */
    private class ExpiredOperationReaper extends ShutdownableThread("ExpirationReaper-%d".format(brokerId), false) {

        override def doWork() {
            advanceClock(200L)
        }
    }

}
