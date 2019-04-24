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

import org.apache.kafka.common.TopicPartition

/**
 * Keys used for delayed operation metrics recording
 */
trait DelayedOperationKey {
    def keyLabel: String
}

object DelayedOperationKey {
    val globalLabel = "All"
}

/**
 * used by delayed-produce and delayed-fetch operations
 *
 * @param topic
 * @param partition
 */
case class TopicPartitionOperationKey(topic: String, partition: Int) extends DelayedOperationKey {

    def this(topicPartition: TopicPartition) = this(topicPartition.topic, topicPartition.partition)

    override def keyLabel: String = "%s-%d".format(topic, partition)
}

/* used by delayed-join-group operations */
case class MemberKey(groupId: String, consumerId: String) extends DelayedOperationKey {

    override def keyLabel: String = "%s-%s".format(groupId, consumerId)
}

/* used by delayed-rebalance operations */
case class GroupKey(groupId: String) extends DelayedOperationKey {

    override def keyLabel: String = groupId
}

/* used by delayed-topic operations */
case class TopicKey(topic: String) extends DelayedOperationKey {

    override def keyLabel: String = topic
}
