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
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * 表示延迟的、异步的操作
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

    /** 标识当前异步操作是否执行完成 */
    private val completed = new AtomicBoolean(false)

    /**
     * 如果延迟操作没有完成，则现将任务从时间轮中删除，然后调用 onComplete() 方法执行具体的逻辑
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
            // 将当前任务从 TimerTaskList 中删除
            this.cancel()
            // 执行延迟操作的具体业务
            this.onComplete()
            true
        } else {
            false
        }
    }

    /**
     * Check if the delayed operation is already completed
     *
     * 检测任务是否已经完成
     */
    def isCompleted: Boolean = completed.get()

    /**
     * Call-back to execute when a delayed operation gets expired and hence forced to complete.
     *
     * 到期时执行的业务逻辑
     */
    def onExpiration(): Unit

    /**
     * Process for completing an operation; This function needs to be defined
     * in subclasses and will be called exactly once in forceComplete()
     *
     * 延迟操作的具体业务逻辑，该方法只能被调用一次
     */
    def onComplete(): Unit

    /**
     * Try to complete the delayed operation by first checking if the operation
     * can be completed by now. If yes execute the completion logic by calling
     * forceComplete() and return true iff forceComplete returns true; otherwise return false
     *
     * This function needs to be defined in subclasses
     *
     * 检测执行条件是否满足，如果满足则会调用 forceComplete
     */
    def tryComplete(): Boolean

    /**
     * Thread-safe variant of tryComplete(). This can be overridden if the operation provides its
     * own synchronization.
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
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

    /**
     * a list of operation watching keys,
     * 用于管理 DelayedOperation，其中 key 是 Watchers 中的 DelayedOperation 所关心的对象
     */
    private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    /** watchersForKey 读写锁 */
    private val removeWatchersLock = new ReentrantReadWriteLock()

    /**
     * the number of estimated total operations in the purgatory
     *
     * 当前 DelayedOperationPurgatory 中 DelayedOperation 的个数
     */
    private[this] val estimatedTotalOperations = new AtomicInteger(0)

    /**
     * background thread expiring operations that have timed out
     *
     * 主要具备 2 个作用：
     *  1. 推进时间表针
     *  2. 定期清理 watchersForKey 中已经完成的 DelayedOperation
     *
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
     * 检测 DelayedOperation 是否已经执行完成，如果未完成则添加到 watchersForKey 和 SystemTimer 中
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

        // The cost of tryComplete() is typically proportional to the number of keys. Calling
        // tryComplete() for each key is going to be expensive if there are many keys. Instead,
        // we do the check in the following way. Call tryComplete(). If the operation is not completed,
        // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
        // the operation is still not completed, we are guaranteed that it won't miss any future triggering
        // event since the operation is already on the watcher list for all keys. This does mean that
        // if the operation is completed (by another thread) between the two tryComplete() calls, the
        // operation is unnecessarily added for watch. However, this is a less severe issue since the
        // expire reaper will clean it up periodically.

        // 1. 调用 DelayedOperation.tryComplete 方法，尝试完成延迟操作
        var isCompletedByMe = operation.safeTryComplete()
        if (isCompletedByMe)
        // 已经完成，则直接返回
            return true

        var watchCreated = false

        // 2. 遍历处理 watchKeys，将 key 对应的 DelayedOperation 添加到 key 对应的 Watchers 中
        for (key <- watchKeys) {
            // 如果对应的 DelayedOperation 已经执行完成，则放弃添加
            if (operation.isCompleted)
                return false

            // 添加 DelayedOperation 到对应 key 的 Watchers 中
            watchForOperation(key, operation)

            if (!watchCreated) {
                watchCreated = true
                // 增加 estimatedTotalOperations 的值
                estimatedTotalOperations.incrementAndGet()
            }
        }

        // 3. 再次调用 DelayedOperation.tryComplete 方法，尝试完成延迟操作
        isCompletedByMe = operation.safeTryComplete()
        if (isCompletedByMe)
            return true

        // if it cannot be completed by now and hence is watched, add to the expire queue also
        /* 执行到这里，可以保证此 DelayedOperation 不会错过任何 key 上触发的 checkAndComplete 操作 */

        // 4. 将 DelayedOperation 添加到 SystemTimer 中
        if (!operation.isCompleted) {
            timeoutTimer.add(operation)
            // 再次检测 DelayedOperation 的执行情况，如果已经完成则从 SystemTimer 中移除
            if (operation.isCompleted) {
                operation.cancel()
            }
        }

        false
    }

    /**
     * Check if some some delayed operations can be completed with the given watch key,
     * and if yes complete them.
     *
     * 依据传入的 key 尝试指定对应 Watchers 中的 DelayedOperation
     *
     * @return the number of completed operations during this process
     */
    def checkAndComplete(key: Any): Int = {
        val watchers = inReadLock(removeWatchersLock) {
            watchersForKey.get(key)
        }
        if (watchers == null)
            0
        else
            watchers.tryCompleteWatched()
    }

    /**
     * Return the total size of watch lists the purgatory. Since an operation may be watched
     * on multiple lists, and some of its watched entries may still be in the watch lists
     * even when it has been completed, this number may be larger than the number of real operations watched
     */
    def watched(): Int = allWatchers.map(_.countWatched).sum

    /**
     * Return the number of delayed operations in the expiry queue
     */
    def delayed(): Int = timeoutTimer.size

    /**
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    private def allWatchers: Iterable[Watchers] = inReadLock(removeWatchersLock) {
        watchersForKey.values
    }

    /**
     * Return the watch list of the given key, note that we need to
     * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
     */
    private def watchForOperation(key: Any, operation: T) {
        inReadLock(removeWatchersLock) {
            val watcher = watchersForKey.getAndMaybePut(key)
            watcher.watch(operation)
        }
    }

    /**
     * Remove the key from watcher lists if its list is empty
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

    /**
     * A linked list of watched delayed operations based on some key
     */
    private class Watchers(val key: Any) {

        /** 管理 DelayedOperation 的同步队列 */
        private[this] val operations = new ConcurrentLinkedQueue[T]()

        // count the current number of watched operations. This is O(n), so use isEmpty() if possible
        def countWatched: Int = operations.size

        def isEmpty: Boolean = operations.isEmpty

        /**
         * 将 DelayedOperation 添加到队列
         *
         * @param t
         */
        def watch(t: T) {
            operations.add(t)
        }

        /**
         * traverse the list and try to complete some watched elements
         *
         * 遍历队列，对于未完成的 DelayedOperation，
         * 执行 tryComplete 操作，将以及完成的 DelayedOperation 从队列中移除，
         *
         * 如果 operations 为空，则将 Watchers 从 watchersForKey 中移除
         *
         * @return
         */
        def tryCompleteWatched(): Int = {
            var completed = 0

            val iter = operations.iterator()
            while (iter.hasNext) {
                val curr = iter.next()
                if (curr.isCompleted) {
                    // another thread has completed this operation, just remove it
                    iter.remove()
                } else if (curr.safeTryComplete()) {
                    iter.remove()
                    completed += 1
                }
            }

            if (operations.isEmpty)
                removeKeyIfEmpty(key, this)

            completed
        }

        /**
         * traverse the list and purge elements that are already completed by others
         *
         * 负责清理 operations 队列，将以及完成的 DelayedOperation 从队列中移除
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

            if (operations.isEmpty)
                removeKeyIfEmpty(key, this)

            purged
        }
    }

    def advanceClock(timeoutMs: Long) {
        // 尝试推进时间轮表针
        timeoutTimer.advanceClock(timeoutMs)

        /**
         * DelayedOperation 到期被 System.taskExecutor 完成后，并不会通知 DelayedOperationPurgatory 删除 DelayedOperation，
         * 当 DelayedOperationPurgatory 与 SystemTimer 中的 DelayedOperation 数量达到一个阈值时，会调用 purgeCompleted() 方法执行清理工作
         */

        // Trigger a purge if the number of completed but still being watched operations is larger than
        // the purge threshold. That number is computed by the difference btw the estimated total number of
        // operations and the number of pending delayed operations.
        if (estimatedTotalOperations.get - delayed > purgeInterval) {
            // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
            // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
            // a little overestimated total number of operations.
            estimatedTotalOperations.getAndSet(delayed())
            debug("Begin purging watch lists")
            val purged = allWatchers.map(_.purgeCompleted()).sum
            debug("Purged %d elements from watch lists.".format(purged))
        }
    }

    /**
     * A background reaper to expire delayed operations that have timed out
     */
    private class ExpiredOperationReaper extends ShutdownableThread("ExpirationReaper-%d".format(brokerId), false) {

        override def doWork() {
            advanceClock(200L)
        }
    }

}
