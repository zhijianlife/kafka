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

package kafka.utils.timer

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

import kafka.utils.nonthreadsafe

/**
 * 分层时间轮算法，相对于时间复杂度为 O(log(n)) 的 DelayQueue 和 Timer，
 * 具备 O(1) 获取执行任务的时间复杂度，插入任务的时间复杂度是 O(m)，删除任务的时间复杂度是 O(1)，
 * 普通时间轮算法仅支持在一个时间轮区间的延时任务调度，而分层时间轮算法能够解决这一缺点。
 *
 *
 *
 * Hierarchical（分级） Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 *
 * 时间轮
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, // 当前时间轮中一格的时间跨度
                                 wheelSize: Int, // 时间轮的格数
                                 startMs: Long, // 当前时间轮的创建时间
                                 taskCounter: AtomicInteger, // 各层级时间轮共用的任务计数器，用于记录时间轮中总的任务数
                                 queue: DelayQueue[TimerTaskList] // 整个层级时间轮共用一个任务队列
                                ) {

    /** 时间轮指针，将时间轮划分为到期部分和未到期部分 */
    private[this] var currentTime = startMs - (startMs % tickMs) // 修剪成 tickMs 的倍数，近似等于创建时间

    /**
     * 当前时间轮的时间跨度，
     * 只能处理时间范围在 [currentTime, currentTime + interval] 之间的延时任务，超过该范围则需要将任务添加到上层时间轮中
     */
    private[this] val interval = tickMs * wheelSize

    /** 每一项都对应时间轮中的一格 */
    private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

    /** 对于上层时间轮的引用 */
    @volatile private[this] var overflowWheel: TimingWheel = _

    /**
     * 创建上层时间轮，默认情况下上层时间轮的 tickMs 是当前时间轮时间跨度 interval
     */
    private[this] def addOverflowWheel(): Unit = {
        synchronized {
            if (overflowWheel == null) {
                // 创建上层时间轮
                overflowWheel = new TimingWheel(
                    tickMs = interval, // tickMs 是当前时间轮的时间跨度 interval
                    wheelSize = wheelSize, // 时间轮的格数不变
                    startMs = currentTime, // 创建时间即当前时间
                    taskCounter = taskCounter, // 全局唯一的任务计数器
                    queue // 全局唯一的任务队列
                )
            }
        }
    }

    /**
     * 往时间轮中添加定时任务，同时检测添加的任务是否已经过期
     *
     * @param timerTaskEntry
     * @return
     */
    def add(timerTaskEntry: TimerTaskEntry): Boolean = {
        // 获取任务的到期时间戳
        val expiration = timerTaskEntry.expirationMs
        if (timerTaskEntry.cancelled) {
            // 任务已经被取消，则不应该被添加
            false
        } else if (expiration < currentTime + tickMs) {
            // 任务已经到期，则不应该被添加
            false
        } else if (expiration < currentTime + interval) {
            /*
             * 任务正好位于当前时间轮的时间跨度范围内，
             * 依据任务的到期时间查找此任务隶属的时间格，并将任务添加到对应的时间格中
             */
            val virtualId = expiration / tickMs
            val bucket = buckets((virtualId % wheelSize.toLong).toInt)
            bucket.add(timerTaskEntry)

            // 更新对应时间格的时间区间上界，如果是第一次往对应时间格中添加延时任务，则需要将时间格记录到全局任务队列中
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket)
            }
            true
        } else {
            // 已经超出了当前时间轮的时间跨度范围，将任务添加到上层时间轮中
            if (overflowWheel == null)
                this.addOverflowWheel()
            overflowWheel.add(timerTaskEntry)
        }
    }

    /**
     * 尝试推进当前时间轮的指针，同时也会尝试推进上层时间轮的表针
     *
     * @param timeMs
     */
    def advanceClock(timeMs: Long): Unit = {
        if (timeMs >= currentTime + tickMs) {
            // 尝试推动指针，可能会往前推进多个时间格
            currentTime = timeMs - (timeMs % tickMs)

            // 尝试推动上层时间轮指针
            if (overflowWheel != null)
                overflowWheel.advanceClock(currentTime)
        }
    }
}
