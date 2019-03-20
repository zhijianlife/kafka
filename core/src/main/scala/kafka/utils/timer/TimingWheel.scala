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
 * Hierarchical Timing Wheels
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
private[timer] class TimingWheel(tickMs: Long, // 当前时间轮中一个时间格表示的时间跨度
                                 wheelSize: Int, // 时间轮的格数，即 buckets 的大小
                                 startMs: Long, // 当前时间轮的创建时间
                                 taskCounter: AtomicInteger, // 各层级时间轮中的任务总数
                                 queue: DelayQueue[TimerTaskList] // 整个层级时间轮共用一个任务队列
                                ) {

    /**
     * 当前时间轮的时间跨度
     * 当前时间轮只能处理时间范围在 [currentTime, currentTime + interval] 之间的定时任务，超过该范围，则需要将任务添加到上层时间轮中
     */
    private[this] val interval = tickMs * wheelSize

    /** 每一项都对应时间轮中的一个时间格 */
    private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }

    /** 时间轮指针，将时间轮划分为到期部分和未到期部分 */
    private[this] var currentTime = startMs - (startMs % tickMs) // 修剪成 tickMs 的倍数，近似等于创建时间

    /** 对于上层时间轮的引用 */
    @volatile private[this] var overflowWheel: TimingWheel = _

    /**
     * 创建上层时间轮，默认情况下上层时间轮的 tickMs 是当前整个时间轮时间跨度 interval
     */
    private[this] def addOverflowWheel(): Unit = {
        synchronized {
            if (overflowWheel == null) {
                // 创建上层时间轮
                overflowWheel = new TimingWheel(
                    tickMs = interval, // tickMs 是当前整个时间轮时间跨度 interval
                    wheelSize = wheelSize, // 时间轮的格数不变
                    startMs = currentTime,
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
        // 获取任务的过期时间
        val expiration = timerTaskEntry.expirationMs

        // 任务已经被取消
        if (timerTaskEntry.cancelled) {
            false
        }
        // 任务已经过期
        else if (expiration < currentTime + tickMs) {
            false
        }
        // 任务正好在当前时间轮的跨度范围内
        else if (expiration < currentTime + interval) {
            // 依据任务的到期时间查找此任务属于的时间格，并将任务添加到对应的 TimerTaskList 中
            val virtualId = expiration / tickMs
            val bucket = buckets((virtualId % wheelSize.toLong).toInt)
            bucket.add(timerTaskEntry)

            // 设置 bucket 的到期时间
            if (bucket.setExpiration(virtualId * tickMs)) {
                // The bucket needs to be enqueued because it was an expired bucket
                // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
                // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
                // will pass in the same value and hence return false, thus the bucket with the same expiration will not
                // be enqueued multiple times.

                /*
                 * 整个时间轮所表示的时间跨度是不变的，随着表针 currentTime 的后移，当前时间轮能够处理的时间段也在不断后移，
                 * 新来的 TimerTaskEntry 会复用原来已经清理过的 TimerTaskList(bucket)。此时需要重置 TimerTaskList 的到期时间，
                 * 并将 bucket 重新添加到 DelayQueue 中。
                 */
                queue.offer(bucket)
            }
            true
        }
        // 已经超出了当前时间轮的时间跨度范围，则将任务添加到上层时间轮中处理
        else {
            if (overflowWheel == null)
                addOverflowWheel()
            overflowWheel.add(timerTaskEntry)
        }
    }

    /**
     * 尝试推进当前时间轮的表针 currentTime，同时也会尝试推进上层时间轮的表针
     *
     * @param timeMs
     */
    def advanceClock(timeMs: Long): Unit = {
        if (timeMs >= currentTime + tickMs) {
            // 尝试移动表针，推进可能不止一格
            currentTime = timeMs - (timeMs % tickMs)

            // 尝试推动上层时间轮的表针
            if (overflowWheel != null)
                overflowWheel.advanceClock(currentTime)
        }
    }
}
