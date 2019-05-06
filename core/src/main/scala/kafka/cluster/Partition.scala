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

package kafka.cluster

import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogConfig}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.PartitionState
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 *
 * 分区对象，负责管理每个副本对应的 Replica 对象，进行 leader 切换，ISR 集合的管理，以及调用日志存储子系统完成写入消息
 */
class Partition(val topic: String, // 分区所属的 topic
                val partitionId: Int, // 分区编号
                time: Time, // 时间戳工具
                replicaManager: ReplicaManager // 副本管理员
               ) extends Logging with KafkaMetricsGroup {

    val topicPartition = new TopicPartition(topic, partitionId)

    /** 当前 broker 的 ID */
    private val localBrokerId = replicaManager.config.brokerId

    /** 管理分区日志数据 */
    private val logManager = replicaManager.logManager

    /** ZK 工具类 */
    private val zkUtils = replicaManager.zkUtils

    /** AR 集合，维护当前分区全部副本的集合，key 是副本 ID */
    private val assignedReplicaMap = new Pool[Int, Replica]

    // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
    private val leaderIsrUpdateLock = new ReentrantReadWriteLock

    private var zkVersion: Int = LeaderAndIsr.initialZKVersion

    /** Leader 副本的年代信息 */
    @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1

    /** Leader 副本的 ID */
    @volatile var leaderReplicaIdOpt: Option[Int] = None

    /** 当前分区的 ISR 集合 */
    @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

    /** 当前 KafkaController 的年代信息，会在切换副本角色时进行更新 */
    private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1

    this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

    /**
     * 判断是不是本地副本
     *
     * @param replicaId
     * @return
     */
    private def isReplicaLocal(replicaId: Int): Boolean = replicaId == localBrokerId

    val tags: Map[String, String] = Map("topic" -> topic, "partition" -> partitionId.toString)

    newGauge("UnderReplicated",
        new Gauge[Int] {
            def value: Int = {
                if (isUnderReplicated) 1 else 0
            }
        },
        tags
    )

    newGauge("InSyncReplicasCount",
        new Gauge[Int] {
            def value: Int = {
                if (isLeaderReplicaLocal) inSyncReplicas.size else 0
            }
        },
        tags
    )

    newGauge("ReplicasCount",
        new Gauge[Int] {
            def value: Int = {
                if (isLeaderReplicaLocal) assignedReplicas.size else 0
            }
        },
        tags
    )

    private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

    def isUnderReplicated: Boolean = isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

    /**
     * 负责在 AR 集合中查找指定副本 ID 对应的 Replica 对象，如果不存在则创建并添加到 AR 集合中，
     * 如果创建的是本地副本，则会创建或恢复对应的 Log，并初始化或恢复 HW。
     *
     * HW 与 Log.recoveryPoint 类似，也会记录到文件中，对应 replication-offset-checkpoint 文件
     *
     * @param replicaId
     * @return
     */
    def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
        // 尝试从 AR 集合中获取 replicaId 对应的 Replica 对象，如果不存在则创建一个
        assignedReplicaMap.getAndMaybePut(replicaId, {
            // 如果是本地副本
            if (this.isReplicaLocal(replicaId)) {
                // 获取 log 相关配置信息，ZK 中的配置会覆盖默认配置
                val config = LogConfig.fromProps(logManager.defaultConfig.originals, AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
                // 创建对应的 Log 对象，如果已经存在则直接返回
                val log = logManager.createLog(topicPartition, config)
                // 获取指定 log 目录对应的 OffsetCheckpoint 对象
                val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
                // 加载 replication-offset-checkpoint 文件中记录的 HW 信息
                val offsetMap = checkpoint.read()
                if (!offsetMap.contains(topicPartition)) info(s"No checkpointed highwatermark is found for partition $topicPartition")
                // 获取 topic 分区对应的 HW 值，并与 LEO 比较，并选择较小的值作为此副本的 HW
                val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
                // 创建 Replica 对象
                new Replica(replicaId, this, time, offset, Some(log))
            }
            // 如果是远程副本，无需加载本地对应的日志数据
            else new Replica(replicaId, this, time)
        })
    }

    /**
     * 获取指定副本 ID 对应的副本
     *
     * @param replicaId
     * @return
     */
    def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(assignedReplicaMap.get(replicaId))

    /**
     * 如果是本地副本，则获取 leader 副本对应的 Replica 对象
     *
     * @return
     */
    def leaderReplicaIfLocal: Option[Replica] = leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

    def addReplicaIfNotExists(replica: Replica): Replica = assignedReplicaMap.putIfNotExists(replica.brokerId, replica)

    def assignedReplicas: Set[Replica] = assignedReplicaMap.values.toSet

    private def removeReplica(replicaId: Int) {
        assignedReplicaMap.remove(replicaId)
    }

    /**
     * 删除对应分区在当前 broker 上的 Log 文件
     */
    def delete() {
        // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
        inWriteLock(leaderIsrUpdateLock) {
            assignedReplicaMap.clear()
            inSyncReplicas = Set.empty[Replica]
            leaderReplicaIdOpt = None
            try {
                logManager.asyncDelete(topicPartition)
                removePartitionMetrics()
            } catch {
                case e: IOException =>
                    fatal(s"Error deleting the log for partition $topicPartition", e)
                    Runtime.getRuntime.halt(1)
            }
        }
    }

    def getLeaderEpoch: Int = this.leaderEpoch

    /**
     * 将本地副本设置成 leader 副本
     *
     * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
     * from the time when this broker was the leader last time) and setting the new leader and ISR.
     *
     * @param controllerId
     * @param partitionStateInfo
     * @param correlationId
     * @return If the leader replica id does not change, return false to indicate the replica manager.
     */
    def makeLeader(controllerId: Int,
                   partitionStateInfo: PartitionState,
                   correlationId: Int): Boolean = {
        val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
            // 获取需要分配的 AR 集合
            val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
            // record the epoch of the controller that made the leadership decision.
            // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
            // 记录 controller 的年代信息
            controllerEpoch = partitionStateInfo.controllerEpoch
            // 1. 获取/创建 AR 集合中所有副本对应的 Replica 对象
            allReplicas.foreach(replica => getOrCreateReplica(replica))
            // 2. 获取/创建 ISR 集合中所有副本对应的 Replica 对象
            val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
            // remove assigned replicas that have been removed by the controller
            // 3. 移除本地缓存的所有不在 allReplicas 中的副本信息
            (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
            // 4. 更新本地相关信息
            inSyncReplicas = newInSyncReplicas // 更新 ISR 集合
            leaderEpoch = partitionStateInfo.leaderEpoch // 更新 leader 副本的年代信息
            zkVersion = partitionStateInfo.zkVersion // 更新 ZK 的版本信息

            // 5. 检测 leader 是否发生变化
            val isNewLeader =
                if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
                    false // 未发生变化
                } else {
                    // leader 发生变化，更新分区 leader 副本 ID
                    leaderReplicaIdOpt = Some(localBrokerId)
                    true
                }
            val leaderReplica = getReplica().get // 获取本地副本 Replica 对象
            val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset
            val curTimeMs = time.milliseconds
            // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
            // 遍历除 leader 副本以外的副本，更新对应副本的相关信息
            (assignedReplicas - leaderReplica).foreach { replica =>
                val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
                replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
            }
            // 如果当前 leader 是新选举出来的
            // we may need to increment high watermark since ISR could be down to 1
            if (isNewLeader) {
                /*
                 * 6. 初始化 leader 副本的 highWatermarkMetadata
                 *
                 * 如果 leader 副本发生迁移，则表示 leader 副本通过上面的步骤刚刚分配到此 broker 上，
                 * 可能是刚启动，也可能是 follower 变成了 leader
                 *
                 * construct the high watermark metadata for the new leader replica
                 */
                leaderReplica.convertHWToLocalOffsetMetadata()
                // 7. 重置本地缓存的所有远程副本的 LEO 值为 -1
                assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
            }
            // 8. 尝试更新 HW
            (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
        }
        // 如果 leader 副本的 HW 增加了，则可能有 DelayedFetch 满足执行条件，尝试执行
        if (leaderHWIncremented)
            tryCompleteDelayedRequests()
        isNewLeader
    }

    /**
     * Make the local replica the follower by setting the new leader and ISR to empty
     * If the leader replica id does not change, return false to indicate the replica manager
     *
     * 依据 partitionStateInfo 信息，将本地副本设置为 follower
     */
    def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
        inWriteLock(leaderIsrUpdateLock) {
            val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
            val newLeaderBrokerId: Int = partitionStateInfo.leader
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
            // to maintain the decision maker controller's epoch in the zookeeper path
            controllerEpoch = partitionStateInfo.controllerEpoch
            // 获取/创建所有副本对应的 Replica 对象
            allReplicas.foreach(r => getOrCreateReplica(r))
            // remove assigned replicas that have been removed by the controller
            // 移除本地缓存的所有不在 allReplicas 中的副本信息
            (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
            // ISR 集合由 leader 副本维护，将 follower 副本上的 ISR 集合置空
            inSyncReplicas = Set.empty[Replica]
            leaderEpoch = partitionStateInfo.leaderEpoch // 更新 leader 副本的年代信息
            zkVersion = partitionStateInfo.zkVersion // 更新 zk 版本信息

            // 检测 leader 副本是否发生变化
            if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
                false
            } else {
                // 发生变化，更新分区 leader 副本的 ID
                leaderReplicaIdOpt = Some(newLeaderBrokerId)
                true
            }
        }
    }

    /**
     * Update the log end offset of a certain replica of this partition
     */
    def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
        getReplica(replicaId) match {
            case Some(replica) =>
                // 更新 follower 副本的状态
                replica.updateLogReadResult(logReadResult)
                // check if we need to expand ISR to include this replica
                // if it is not in the ISR yet
                // 检测 ISR 是需要扩张，并同步到 ZK
                maybeExpandIsr(replicaId, logReadResult)

                debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
                        .format(replicaId, logReadResult.info.fetchOffsetMetadata.messageOffset, topicPartition))
            case None =>
                throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
                        " is not recognized to be one of the assigned replicas %s for partition %s.")
                        .format(localBrokerId,
                            replicaId,
                            logReadResult.info.fetchOffsetMetadata.messageOffset,
                            assignedReplicas.map(_.brokerId).mkString(","),
                            topicPartition))
        }
    }

    /**
     * Check and maybe expand the ISR of the partition.
     * A replica will be added to ISR if its LEO >= current hw of the partition.
     *
     * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs, even if its log end offset is >= HW.
     * However, to be consistent with how the follower determines whether a replica is in-sync, we only check HW.
     *
     * This function can be triggered when a replica's LEO has incremented
     *
     * 扩张 ISR 集合
     */
    def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult) {
        val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
            // 如果本地副本是 leader 副本，则检查待处理副本是否应该被加入到 ISR 集合中
            leaderReplicaIfLocal match {
                case Some(leaderReplica) =>
                    // 获取 follower 副本对应的 Replica 对象
                    val replica = getReplica(replicaId).get
                    // 获取 leader 副本对应的 HW
                    val leaderHW = leaderReplica.highWatermark
                    if (!inSyncReplicas.contains(replica) // follower 副本不在 ISR 集合中
                            && assignedReplicas.map(_.brokerId).contains(replicaId) // AR 集合中包含该 follower 副本
                            && replica.logEndOffset.offsetDiff(leaderHW) >= 0) { // follower 副本的 LEO 已经追赶上 leader 的 HW
                        // 将 follower 副本添加到 ISR 集合中
                        val newInSyncReplicas = inSyncReplicas + replica
                        info(s"Expanding ISR for partition $topicPartition from ${inSyncReplicas.map(_.brokerId).mkString(",")} to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
                        // 将新的 ISR 集合信息记录到 ZK，并更新本地 ISR 集合信息
                        updateIsr(newInSyncReplicas)
                        replicaManager.isrExpandRate.mark()
                    }

                    // check if the HW of the partition can now be incremented
                    // since the replica may already be in the ISR and its LEO has just incremented
                    // 尝试后移 leader 副本的 HW
                    maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)

                // 如果不是 leader 副本，啥也不干
                case None => false
            }
        }

        // some delayed operations may be unblocked after HW changed
        // 尝试执行延时任务
        if (leaderHWIncremented)
            tryCompleteDelayedRequests()
    }

    /**
     * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
     * and the second element is an error (which would be `Errors.NONE` for no error).
     *
     * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
     * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
     * produce request.
     *
     * 检测当前分区对应 offset 的消息是否已经被 ISR 集合中所有的 follower 副本同步
     */
    def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
        // 获取 leader 副本对应的 Replica 对象
        leaderReplicaIfLocal match {
            case Some(leaderReplica) =>
                val curInSyncReplicas = inSyncReplicas

                // 获取 ISR 集合中已经 ack 的副本数目
                def numAcks: Int = curInSyncReplicas.count { r =>
                    if (!r.isLocal)
                        if (r.logEndOffset.messageOffset >= requiredOffset) {
                            trace(s"Replica ${r.brokerId} of $topic-$partitionId received offset $requiredOffset")
                            true
                        }
                        else
                            false
                    else
                        true /* also count the local (leader) replica */
                }

                trace(s"$numAcks acks satisfied for $topic-$partitionId with acks = -1")

                /* 对应 min.insync.replicas 配置 */
                val minIsr = leaderReplica.log.get.config.minInSyncReplicas

                // 如果当前请求的 offset 小于等于 HW 的 offset
                if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
                    /*
                     * The topic may be configured not to accept messages if there are not enough replicas in ISR,
                     * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
                     */
                    if (minIsr <= curInSyncReplicas.size)
                        (true, Errors.NONE)
                    else
                        (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
                } else {
                    // 如果当前请求的 offset 大于 HW，则直接返回 false
                    (false, Errors.NONE)
                }

            // 如果当前本地副本不是 leader 副本，则返回 NOT_LEADER_FOR_PARTITION 错误
            case None =>
                (false, Errors.NOT_LEADER_FOR_PARTITION)
        }
    }

    /**
     * Check and maybe increment the high watermark of the partition;
     * this function can be triggered when
     *
     * 1. Partition ISR changed
     * 2. Any replica's LEO changed
     *
     * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
     * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
     * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
     * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
     * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
     * will never be added to ISR.
     *
     * Returns true if the HW was incremented, and false otherwise.
     * Note There is no need to acquire the leaderIsrUpdate lock here
     * since all callers of this private API acquire that lock
     *
     * 尝试后移 leader 副本的 HW
     */
    private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
        // 获取 ISR 集合中所有副本的 LEO，排除已经超过给定时间阈值（对应 replica.lag.time.max.ms 配置）还未收到拉取消息请求的 follower
        val allLogEndOffsets = assignedReplicas.filter { replica =>
            curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
        }.map(_.logEndOffset)

        // 以 ISR 集合中最小的 LEO 作为新的 HW 值
        val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)

        // 比较新旧 HW 值，如果旧的 HW 小于新的 HW，或者旧的 HW 对应的 LogSegment 的 baseOffset 小于新的 HW 的 LogSegment 对象的 baseOffset，则更新
        val oldHighWatermark = leaderReplica.highWatermark
        if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
            leaderReplica.highWatermark = newHighWatermark
            debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
            true
        } else {
            debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
                    .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
            false
        }
    }

    /**
     * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
     */
    private def tryCompleteDelayedRequests() {
        val requestKey = new TopicPartitionOperationKey(topicPartition)
        replicaManager.tryCompleteDelayedFetch(requestKey)
        replicaManager.tryCompleteDelayedProduce(requestKey)
    }

    /**
     * 在分布式系统中，由于网路的原因，可能导致 ISR 集合中的部分 follower 副本无法与 leader 副本进行同步，
     * 此时如果生产者请求时指定 acks = -1，那么需要长时间等待，而 maybeShrinkIsr 就是用来对 ISR 集合执行缩减操作
     *
     * @param replicaMaxLagTimeMs
     */
    def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
        val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
            // 如果本地副本是 leader 副本，则检查待处理副本是否应该被加入到 ISR 集合中
            leaderReplicaIfLocal match {
                case Some(leaderReplica) =>
                    // 从 ISR 集合中获取滞后的 follower 副本集合
                    val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
                    if (outOfSyncReplicas.nonEmpty) {
                        // 将滞后的 follower 副本从 ISR 集合中剔除
                        val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
                        assert(newInSyncReplicas.nonEmpty)
                        info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
                            inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
                        // 将新的 ISR 集合信息上报给 ZK，同时更新本地记录的 ISR 集合信息
                        updateIsr(newInSyncReplicas)
                        // we may need to increment high watermark since ISR could be down to 1
                        replicaManager.isrShrinkRate.mark()
                        // 尝试后移 leader 副本的 HW
                        maybeIncrementLeaderHW(leaderReplica)
                    } else {
                        false
                    }

                // 如果不是 leader 副本，则啥也不做
                case None => false
            }
        }

        // some delayed operations may be unblocked after HW changed
        if (leaderHWIncremented)
        // 尝试执行延时任务
            tryCompleteDelayedRequests()
    }

    /**
     * there are two cases that will be handled here:
     * 1. Stuck（卡住） followers: If the leo of the replica hasn't been updated for maxLagMs ms, the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms, then the follower is lagging and should be removed from the ISR
     *
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents the last time when the replica was fully caught up.
     * If either of the above conditions is violated, that replica is considered to be out of sync
     *
     * @param leaderReplica
     * @param maxLagMs
     * @return
     */
    def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
        // 获取 ISR 集合中除 leader 以外的副本
        val candidateReplicas = inSyncReplicas - leaderReplica

        // 获取超过给定时间（对应 replica.lag.time.max.ms 配置）没有发送拉取消息的 follower 集合
        val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
        if (laggingReplicas.nonEmpty)
            debug("Lagging replicas for partition %s are %s".format(topicPartition, laggingReplicas.map(_.brokerId).mkString(",")))

        laggingReplicas
    }

    /**
     * 往 leader 副本对应的 Log 追加消息
     *
     * @param records      待追加的消息
     * @param requiredAcks acks 参数
     * @return
     */
    def appendRecordsToLeader(records: MemoryRecords, requiredAcks: Int = 0): LogAppendInfo = {
        val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
            // 只有 leader 副本支持追加消息操作
            leaderReplicaIfLocal match {
                case Some(leaderReplica) =>
                    // 获取 leader 副本对应的 Log 对象
                    val log = leaderReplica.log.get
                    // 对应 min.insync.replicas 配置，表示 ISR 集合的最小值
                    val minIsr = log.config.minInSyncReplicas
                    val inSyncSize = inSyncReplicas.size

                    // 如果当前 ISR 集合中的副本数小于允许的最小值，且 acks = -1，则不允许追加消息，防止数据丢失
                    if (inSyncSize < minIsr && requiredAcks == -1) {
                        throw new NotEnoughReplicasException(
                            "Number of insync replicas for partition %s is [%d], below required minimum [%s]".format(topicPartition, inSyncSize, minIsr))
                    }

                    // 往 leader 的 Log 对象中追加消息
                    val info = log.append(records)
                    // 有新的日志数据被追加，尝试执行对应的 DelayedFetch 任务
                    replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
                    // 尝试后移 leader 副本的 HW
                    (info, maybeIncrementLeaderHW(leaderReplica))

                // 如果不是 leader，则抛出异常
                case None =>
                    throw new NotLeaderForPartitionException(
                        "Leader not local for partition %s on broker %d".format(topicPartition, localBrokerId))
            }
        }

        // some delayed operations may be unblocked after HW changed
        if (leaderHWIncremented)
            tryCompleteDelayedRequests()

        info
    }

    private def updateIsr(newIsr: Set[Replica]) {
        val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(
            zkUtils, topic, partitionId, newLeaderAndIsr, controllerEpoch, zkVersion)

        if (updateSucceeded) {
            replicaManager.recordIsrChange(topicPartition)
            inSyncReplicas = newIsr
            zkVersion = newVersion
            trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
        } else {
            info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
        }
    }

    /**
     * remove deleted log metrics
     */
    private def removePartitionMetrics() {
        removeMetric("UnderReplicated", tags)
        removeMetric("InSyncReplicasCount", tags)
        removeMetric("ReplicasCount", tags)
    }

    override def equals(that: Any): Boolean = that match {
        case other: Partition => partitionId == other.partitionId && topic == other.topic
        case _ => false
    }

    override def hashCode: Int = 31 + topic.hashCode + 17 * partitionId

    override def toString: String = {
        val partitionString = new StringBuilder
        partitionString.append("Topic: " + topic)
        partitionString.append("; Partition: " + partitionId)
        partitionString.append("; Leader: " + leaderReplicaIdOpt)
        partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
        partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
        partitionString.toString
    }
}
