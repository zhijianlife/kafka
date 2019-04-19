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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{Time, Utils}

trait Timer {

    /**
     * Add a new task to this executor. It will be executed after the task's delay
     * (beginning from the time of submission)
     *
     * @param timerTask the task to add
     */
    def add(timerTask: TimerTask): Unit

    /**
     * Advance the internal clock, executing any tasks whose expiration has been
     * reached within the duration of the passed timeout.
     *
     * @param timeoutMs
     * @return whether or not any tasks were executed
     */
    def advanceClock(timeoutMs: Long): Boolean

    /**
     * Get the number of tasks pending execution
     *
     * @return the number of tasks
     */
    def size: Int

    /**
     * Shutdown the timer service, leaving pending tasks unexecuted
     */
    def shutdown(): Unit
}

/**
 * 定时器实现，在 TimeWheel 的基础上添加了执行到期任务、阻塞等待最近到期任务的功能
 *
 * @param executorName
 * @param tickMs
 * @param wheelSize
 * @param startMs
 */
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1, // 默认时间格时间为 1 毫秒
                  wheelSize: Int = 20, // 默认时间格大小为 20
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

    /** 定时任务执行线程池 */
    private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
        def newThread(runnable: Runnable): Thread = Utils.newThread("executor-" + executorName, runnable, false)
    })

    /** 各个层级时间轮共用的队列 */
    private[this] val delayQueue = new DelayQueue[TimerTaskList]()

    /** 各个层级时间轮共用的任务个数计数器 */
    private[this] val taskCounter = new AtomicInteger(0)

    /** 层级时间轮中最底层的时间轮 */
    private[this] val timingWheel = new TimingWheel(
        tickMs = tickMs,
        wheelSize = wheelSize,
        startMs = startMs,
        taskCounter = taskCounter,
        delayQueue
    )

    /** 用来同步修改时间轮指针的读写锁 */
    private[this] val readWriteLock = new ReentrantReadWriteLock()
    private[this] val readLock = readWriteLock.readLock()
    private[this] val writeLock = readWriteLock.writeLock()

    /**
     * 添加定时任务
     *
     * @param timerTask the task to add
     */
    def add(timerTask: TimerTask): Unit = {
        readLock.lock()
        try {
            // 将 TimerTask 封装成 TimerTaskEntry，并添加到时间轮中
            this.addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
        } finally {
            readLock.unlock()
        }
    }

    private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
        // 往时间轮中添加定时任务，同时检测添加的任务是否已经过期
        if (!timingWheel.add(timerTaskEntry)) {
            // 任务过期但未被取消，则立即提交执行
            if (!timerTaskEntry.cancelled)
                taskExecutor.submit(timerTaskEntry.timerTask)
        }
    }

    /** 将定时任务重新添加到时间轮中 */
    private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => this.addTimerTaskEntry(timerTaskEntry)

    /**
     * 推进时间轮指针，同时处理时间格中到期的任务
     */
    def advanceClock(timeoutMs: Long): Boolean = {
        // 超时等待获取时间格对象
        var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
        // 如果有到期的时间格
        if (bucket != null) {
            writeLock.lock()
            try {
                while (bucket != null) {
                    // 推进时间轮指针
                    timingWheel.advanceClock(bucket.getExpiration)
                    /*
                     * 遍历处理当前时间格中的任务列表，提交执行到期但未被取消的任务，
                     * 对于未到期的任务重新添加到时间轮中继续等待被执行，期间可能会对任务在层级上执行降级
                     */
                    bucket.flush(reinsert)
                    bucket = delayQueue.poll()
                }
            } finally {
                writeLock.unlock()
            }
            true
        } else {
            false
        }
    }

    def size: Int = taskCounter.get

    override def shutdown() {
        taskExecutor.shutdown()
    }

}

