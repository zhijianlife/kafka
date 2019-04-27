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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Delayed, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

/**
 * 环形双向链表，使用 TimerTaskEntry 封装定时任务，对应时间轮中的一格
 *
 * @param taskCounter 任务计数器，记录各层级时间轮中的任务总数
 */
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

    /** 根结点 */
    private[this] val root = new TimerTaskEntry(null, -1)
    root.next = root
    root.prev = root

    /** 记录当前时间格对应时间区间上界 */
    private[this] val expiration = new AtomicLong(-1L)

    /**
     * 设置当前时间格对应的时间区间上界
     *
     * @param expirationMs
     * @return true 表示往当前时间格中第一次添加延时任务
     */
    def setExpiration(expirationMs: Long): Boolean = {
        expiration.getAndSet(expirationMs) != expirationMs
    }

    def getExpiration: Long = {
        expiration.get()
    }

    /**
     * 遍历处理当前格子中的是任务
     *
     * @param f
     */
    def foreach(f: TimerTask => Unit): Unit = {
        synchronized {
            var entry = root.next
            while (entry ne root) {
                val nextEntry = entry.next
                // 如果对应的任务未被取消，则应用给定的函数 f
                if (!entry.cancelled) f(entry.timerTask)
                entry = nextEntry
            }
        }
    }

    /**
     * 添加 entry 到当前时间格中
     *
     * @param timerTaskEntry
     */
    def add(timerTaskEntry: TimerTaskEntry): Unit = {
        var done = false
        while (!done) {
            // 从格子中移除当前任务（如果存在的话）
            timerTaskEntry.remove()

            synchronized {
                timerTaskEntry.synchronized {
                    if (timerTaskEntry.list == null) {
                        // 将当前任务添加到时间格的尾部
                        val tail = root.prev
                        timerTaskEntry.next = root
                        timerTaskEntry.prev = tail
                        timerTaskEntry.list = this
                        tail.next = timerTaskEntry
                        root.prev = timerTaskEntry
                        taskCounter.incrementAndGet() // 任务计数加 1
                        done = true
                    }
                }
            }
        }
    }

    /**
     * 从时间格中移除指定的任务
     *
     * @param timerTaskEntry
     */
    def remove(timerTaskEntry: TimerTaskEntry): Unit = {
        synchronized {
            timerTaskEntry.synchronized {
                if (timerTaskEntry.list eq this) {
                    timerTaskEntry.next.prev = timerTaskEntry.prev
                    timerTaskEntry.prev.next = timerTaskEntry.next
                    timerTaskEntry.next = null
                    timerTaskEntry.prev = null
                    timerTaskEntry.list = null
                    taskCounter.decrementAndGet() // 任务计数减 1
                }
            }
        }
    }

    /**
     * 遍历移除所有的定时任务，并对任务应用函数 f
     *
     * @param f
     */
    def flush(f: TimerTaskEntry => Unit): Unit = {
        synchronized {
            var head = root.next
            while (head ne root) {
                remove(head) // 移除任务
                f(head) // 应用函数 f
                head = root.next
            }
            expiration.set(-1L)
        }
    }

    /**
     * 获取剩余的过期时间
     *
     * @param unit
     * @return
     */
    def getDelay(unit: TimeUnit): Long = {
        unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
    }

    def compareTo(d: Delayed): Int = {
        val other = d.asInstanceOf[TimerTaskList]
        if (getExpiration < other.getExpiration) -1
        else if (getExpiration > other.getExpiration) 1
        else 0
    }

}

/**
 * 封装延时任务
 *
 * @param timerTask    对应的延时任务
 * @param expirationMs 延时时间戳，即延时任务的延时时间 + 当前时间戳
 */
private[timer] class TimerTaskEntry(val timerTask: TimerTask, // 封装的延时任务
                                    val expirationMs: Long // 延时时间戳，即延时任务的延时时间 + 当前时间戳
                                   ) extends Ordered[TimerTaskEntry] {

    /** 所属时间格 */
    @volatile var list: TimerTaskList = _
    /** 后置指针 */
    var next: TimerTaskEntry = _
    /** 前置指针 */
    var prev: TimerTaskEntry = _

    /**
     * 绑定延时任务，如果之前添加过，则先移除历史记录
     */
    if (timerTask != null) timerTask.setTimerTaskEntry(this)

    /**
     * 判断当前延时任务是否已经被取消
     *
     * @return
     */
    def cancelled: Boolean = {
        timerTask.getTimerTaskEntry != this
    }

    /**
     * 从时间格中移除当前延时任务，即移除对应的节点即可
     */
    def remove(): Unit = {
        var currentList = list
        /*
         * If remove is called when another thread is moving the entry from a task entry list to another,
         * this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
         * In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
         */
        while (currentList != null) {
            currentList.remove(this)
            currentList = list
        }
    }

    override def compare(that: TimerTaskEntry): Int = {
        this.expirationMs compare that.expirationMs
    }
}

