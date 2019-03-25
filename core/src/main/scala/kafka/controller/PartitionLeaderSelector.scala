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

package kafka.controller

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.log.LogConfig
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.Logging

/**
 * Leader 副本选举策略
 */
trait PartitionLeaderSelector {

    /**
     * @param topicAndPartition   需要执行 leader 选举的分区
     * @param currentLeaderAndIsr 当前 leader 副本信息、ISR 集合信息
     * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
     * @return 选举后的新的 leader 副本和新 ISR 集合信息，以及需要接收 LeaderAndIsrRequest 的 brokerId 集合
     */
    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 * isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig) extends PartitionLeaderSelector with Logging {

    this.logIdent = "[OfflinePartitionLeaderSelector]: "

    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
        // 获取分区对应的 AR 集合
        controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
            case Some(assignedReplicas) =>
                // 获取分区 AR 集合中可用的副本
                val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
                // 获取 ISR 集合中可用的副本
                val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
                val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
                val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

                // 检测当前 ISR 集合中是否有可用的副本
                val newLeaderAndIsr =
                    if (liveBrokersInIsr.isEmpty) {
                        // 依据配置决定是否从 AR 集合中选择 leader 副本，如果不允许则抛出异常
                        // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
                        // for unclean leader election.
                        if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(
                            controllerContext.zkUtils, ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                            throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                                    "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                                    " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
                        }
                        debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
                                .format(topicAndPartition, liveAssignedReplicas.mkString(",")))
                        if (liveAssignedReplicas.isEmpty) {
                            // AR 集合中没有可用副本，直接抛出异常
                            throw new NoReplicaOnlineException(("No replica for partition " +
                                    "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                                    " Assigned replicas are: [%s]".format(assignedReplicas))
                        } else {
                            // 从可用 AR 集合中选取新的 leader 副本，新的 ISR 集合中只有新的 leader 副本自己
                            ControllerStats.uncleanLeaderElectionRate.mark()
                            val newLeader = liveAssignedReplicas.head
                            warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                                    .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
                        }
                    } else {
                        val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
                        // 从当前 ISR 集合中选择 leader 副本和 ISR 集合
                        val newLeader = liveReplicasInIsr.head
                        debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
                                .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
                        // 构造 LeaderAndIsr 对象并返回
                        new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr, currentLeaderIsrZkPathVersion + 1)
                    }
                info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
                // 需要向 AR 集合中所有的可用副本发送 LeaderAndIsrRequest 请求
                (newLeaderAndIsr, liveAssignedReplicas)
            case None =>
                throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
        }
    }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

    this.logIdent = "[ReassignedPartitionLeaderSelector]: "

    /**
     * The reassigned replicas are already in the ISR when selectLeader is called.
     *
     * 新的 leader 副本必须在新指定的 AR 集合中，且同时在当前的 ISR 集合中，
     * 当前 ISR 集合为新 ISR 集合，并向新指定的 AR 集合副本发送 LeaderAndIsrRequest 请求
     *
     * @param topicAndPartition   需要执行 leader 选举的分区
     * @param currentLeaderAndIsr 当前 leader 副本信息、ISR 集合信息
     * @return 选举后的新的 leader 副本和新 ISR 集合信息，以及需要接收 LeaderAndIsrRequest 的 brokerId 集合
     */
    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
        val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        val aliveReassignedInSyncReplicas = reassignedInSyncReplicas
                .filter(r => controllerContext.liveBrokerIds.contains(r) && currentLeaderAndIsr.isr.contains(r))
        val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
        newLeaderOpt match {
            case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
                currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
            case None =>
                reassignedInSyncReplicas.size match {
                    case 0 =>
                        throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
                                " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
                    case _ =>
                        throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
                                "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
                }
        }
    }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
 *
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

    this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

    /**
     * 如果优先副本可用且在 ISR 集合中，则选举其为 leader 副本，
     * 当前的 ISR 集合为新的 ISR 集合，并向 AR 集合中所有的可用副本发送 LeaderAndIsrRequest 请求
     *
     * @param topicAndPartition   需要执行 leader 选举的分区
     * @param currentLeaderAndIsr 当前 leader 副本信息、ISR 集合信息
     * @return 选举后的新的 leader 副本和新 ISR 集合信息，以及需要接收 LeaderAndIsrRequest 的 brokerId 集合
     */
    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
        val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
        val preferredReplica = assignedReplicas.head
        // check if preferred replica is the current leader
        val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
        if (currentLeader == preferredReplica) {
            throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                    .format(preferredReplica, topicAndPartition))
        } else {
            info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
                    " Triggering preferred replica leader election")
            // check if preferred replica is not the current leader and is alive and in the isr
            if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
                (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
                    currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
            } else {
                throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
                        "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
            }
        }
    }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

    this.logIdent = "[ControlledShutdownLeaderSelector]: "

    /**
     * 从当前 ISR 集合中排除正在关闭的副本后作为新的 ISR 集合，从新的 ISR 集合中选择新的 leader，
     * 并向 AR 集合中可用的副本发送 LeaderAndIsrRequest 请求
     *
     * @param topicAndPartition   需要执行 leader 选举的分区
     * @param currentLeaderAndIsr 当前 leader 副本信息、ISR 集合信息
     * @return 选举后的新的 leader 副本和新 ISR 集合信息，以及需要接收 LeaderAndIsrRequest 的 brokerId 集合
     */
    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

        val currentLeader = currentLeaderAndIsr.leader

        val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
        val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
        val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))

        val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
        liveAssignedReplicas.find(newIsr.contains) match {
            case Some(newLeader) =>
                debug("Partition %s : current leader = %d, new leader = %d".format(topicAndPartition, currentLeader, newLeader))
                (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
            case None =>
                throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
                        " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
        }
    }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

    this.logIdent = "[NoOpLeaderSelector]: "

    def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
        warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
        (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
    }
}
