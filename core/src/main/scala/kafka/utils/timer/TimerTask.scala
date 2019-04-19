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
 * 定时任务
 */
trait TimerTask extends Runnable {

    /** 当前任务的延迟时长（单位：毫秒） */
    val delayMs: Long

    private[this] var timerTaskEntry: TimerTaskEntry = _

    def cancel(): Unit = {
        synchronized {
            if (timerTaskEntry != null) timerTaskEntry.remove()
            timerTaskEntry = null
        }
    }

    private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
        synchronized {
            // 如果对应的 entry 之前被添加过，则先移除之前的添加记录
            if (timerTaskEntry != null && timerTaskEntry != entry) {
                timerTaskEntry.remove()
            }
            timerTaskEntry = entry
        }
    }

    private[timer] def getTimerTaskEntry: TimerTaskEntry = {
        timerTaskEntry
    }

}
