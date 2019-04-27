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

/**
 * 延时任务
 */
trait TimerTask extends Runnable {

    /** 当前任务的延迟时长（单位：毫秒） */
    val delayMs: Long

    /** 封装当前定时任务的链表节点 */
    private[this] var timerTaskEntry: TimerTaskEntry = _

    /**
     * 取消当前延时任务
     */
    def cancel(): Unit = {
        synchronized {
            // 从时间格中移除封装当前定时任务的结点
            if (timerTaskEntry != null) timerTaskEntry.remove()
            timerTaskEntry = null
        }
    }

    /**
     * 绑定当前任务与对应的结点
     *
     * @param entry
     */
    private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
        synchronized {
            // 如果对应的结点之前被添加过，则先移除之前的添加记录
            if (timerTaskEntry != null && timerTaskEntry != entry) {
                timerTaskEntry.remove()
            }
            timerTaskEntry = entry
        }
    }

    /**
     * 获取当前延时任务对应的结点
     *
     * @return
     */
    private[timer] def getTimerTaskEntry: TimerTaskEntry = {
        timerTaskEntry
    }

}
