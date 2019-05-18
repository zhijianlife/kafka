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
     * 选举分区 leader 副本
     *
     * @param topicAndPartition   需要执行 leader 副本选举的分区
     * @param currentLeaderAndIsr 目标分区当前 leader 副本、ISR 集合
     * @return 选举后的新的 leader 副本和新 ISR 集合信息，以及需要接收 LeaderAndIsrRequest 的 brokerId 集合
     * @throws NoReplicaOnlineException 如果 AR 集合中不存在可用的副本，则抛出异常
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
        controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
            // 处理分区的 AR 集合
            case Some(assignedReplicas) =>
                // 获取分区 AR 集合中可用的副本
                val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
                // 获取 ISR 集合中可用的副本
                val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
                val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
                val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

                // 依据 ISR 集合中是否有可用的副本来决定是从 ISR 集合中选举新的 leader 副本，还是从 AR 集合中选举新的 leader 副本
                val newLeaderAndIsr =
                    if (liveBrokersInIsr.isEmpty) { // 如果 ISR 集合中不存在可用的副本
                        // 依据配置（unclean.leader.election.enable）决定是否从 AR 集合中选择 leader 副本，如果不允许则抛出异常
                        if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(
                            controllerContext.zkUtils, ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                            throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                                    "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                                    " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
                        }
                        debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s".format(topicAndPartition, liveAssignedReplicas.mkString(",")))
                        if (liveAssignedReplicas.isEmpty) {
                            // AR 集合中没有可用副本，直接抛出异常
                            throw new NoReplicaOnlineException(("No replica for partition " +
                                    "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) + " Assigned replicas are: [%s]".format(assignedReplicas))
                        } else {
                            // 从 AR 集合可用的副本中选取第一个副本作为新的 leader 副本，新的 ISR 集合中只有新的 leader 副本自己
                            ControllerStats.uncleanLeaderElectionRate.mark()
                            val newLeader = liveAssignedReplicas.head
                            warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss.".format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
                        }
                    } else { // ISR 集合中存在可用的副本
                        val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
                        // 从 ISR 集合中选择第一个副本作为新的 leader 副本，以 ISR 集合中可用的副本集合作为新的 ISR 集合
                        val newLeader = liveReplicasInIsr.head
                        debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader.".format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
                        // 构造 LeaderAndIsr 对象并返回
                        new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr, currentLeaderIsrZkPathVersion + 1)
                    }
                info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))

                // 需要向 AR 集合中所有的可用副本发送 LeaderAndIsrRequest 请求
                (newLeaderAndIsr, liveAssignedReplicas)
            // 当前 topic 分区没有可用的副本
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
        // 获取新分配的 AR 集合
        val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        // 新选择的 leader 副本必须在新分配的 AR 集合和 ISR 集合中
        val aliveReassignedInSyncReplicas = reassignedInSyncReplicas
                .filter(r => controllerContext.liveBrokerIds.contains(r) && currentLeaderAndIsr.isr.contains(r))
        val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
        newLeaderOpt match {
            // 存在满足条件的 leader 副本，以之前的 ISR 集合作为新的 ISR 集合
            case Some(newLeader) =>
                (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr, currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
            // 不存在满足条件的 leader 副本，抛出异常
            case None =>
                reassignedInSyncReplicas.size match {
                    case 0 =>
                        throw new NoReplicaOnlineException("List of reassigned replicas for partition %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
                    case _ =>
                        throw new NoReplicaOnlineException("None of the reassigned replicas for partition %s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
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
        // 获取分区 AR 集合
        val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
        // 以分区 AR 集合的第一个副本作为优先副本
        val preferredReplica = assignedReplicas.head
        val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
        if (currentLeader == preferredReplica) {
            // 优先副本已经是 leader 副本
            throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s".format(preferredReplica, topicAndPartition))
        } else {
            info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) + " Triggering preferred replica leader election")
            // 如果优先副本所在的 broker 节点可用，且位于 ISR 集合中，则选择成为新的 leader 副本
            if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
                (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr, currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
            } else {
                throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) + "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
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
        // 获取当前可用的 brokerId 集合
        val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
        val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))
        // 从当前 ISR 集合中移除副本所在 broker 节点正在关闭的副本
        val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
        // 从可用的 AR 集合中选择位于新的 ISR 集合中的副本作为 leader 副本
        liveAssignedReplicas.find(newIsr.contains) match {
            case Some(newLeader) =>
                debug("Partition %s : current leader = %d, new leader = %d".format(topicAndPartition, currentLeader, newLeader))
                (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
            case None =>
                throw new StateChangeFailedException("No other replicas in ISR %s for %s besides shutting down brokers %s".format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
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
