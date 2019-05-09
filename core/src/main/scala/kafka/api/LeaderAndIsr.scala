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

package kafka.api

import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.utils._

import scala.collection.Set

object LeaderAndIsr {
    val initialLeaderEpoch: Int = 0
    val initialZKVersion: Int = 0
    val NoLeader: Int = -1
    val LeaderDuringDelete: Int = -2
}

/**
 *
 * @param leader      leader 副本 ID
 * @param leaderEpoch leader 的年代信息
 * @param isr         ISR 集合
 * @param zkVersion   ZK 的版本信息
 */
case class LeaderAndIsr(var leader: Int,
                        var leaderEpoch: Int,
                        var isr: List[Int],
                        var zkVersion: Int) {

    def this(leader: Int, isr: List[Int]) =
        this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion)

    override def toString: String = {
        Json.encode(Map("leader" -> leader, "leader_epoch" -> leaderEpoch, "isr" -> isr))
    }
}

/**
 *
 * @param leaderIsrAndControllerEpoch
 * @param allReplicas AR 集合
 */
case class PartitionStateInfo(leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                              allReplicas: Set[Int]) {

    def replicationFactor: Int = allReplicas.size

    override def toString: String = {
        val partitionStateInfo = new StringBuilder
        partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString)
        partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")")
        partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")")
        partitionStateInfo.toString()
    }
}
