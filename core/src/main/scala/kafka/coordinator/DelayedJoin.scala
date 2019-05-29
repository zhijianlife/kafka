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

package kafka.coordinator

import kafka.server.DelayedOperation

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 *
 * 等待 group 名下所有的消费者发送 JoinGroupRequest 请求申请加入到当前 group
 */
private[coordinator] class DelayedJoin(coordinator: GroupCoordinator,
                                       group: GroupMetadata,
                                       rebalanceTimeout: Long // 指定 DelayedJoin 延时任务的到期时长，对应 group 中所有消费者设置的超时时间最大值
                                      ) extends DelayedOperation(rebalanceTimeout) {

    // overridden since tryComplete already synchronizes on the group. This makes it safe to
    // call purgatory operations while holding the group lock.
    override def safeTryComplete(): Boolean = tryComplete()

    override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete)

    override def onExpiration(): Unit = coordinator.onExpireJoin()

    override def onComplete(): Unit = coordinator.onCompleteJoin(group)
}
