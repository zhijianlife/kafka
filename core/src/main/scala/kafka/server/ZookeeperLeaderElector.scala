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

import kafka.controller.{ControllerContext, KafkaController}
import kafka.utils.CoreUtils._
import kafka.utils.{Json, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.IZkDataListener
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 *
 * 用于执行 controller leader 故障转移
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext, // Controller 上下文对象
                             electionPath: String, // /controller
                             onBecomingLeader: () => Unit, // KafkaController#onControllerFailover
                             onResigningAsLeader: () => Unit, // KafkaController#onControllerResignation
                             brokerId: Int, // broker 节点 ID
                             time: Time // 时间戳工具类
                            ) extends LeaderElector with Logging {

    /** 当前 controller leader 的 ID */
    var leaderId: Int = -1

    // create the election path in ZK, if one does not exist
    val index: Int = electionPath.lastIndexOf("/")
    if (index > 0) controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))

    /** 监听 ZK 的 /controller 节点的数据变化 */
    val leaderChangeListener = new LeaderChangeListener

    def startup {
        inLock(controllerContext.controllerLock) {
            // 注册 ZK 监听器，监听 /controller 节点下的数据变更
            controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
            // 执行 leader 选举
            elect
        }
    }

    /**
     * 从 ZK 上获取当前的 Controller ID
     *
     * @return
     */
    def getControllerID: Int = {
        controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
            case Some(controller) => KafkaController.parseControllerId(controller)
            case None => -1
        }
    }

    /**
     * 触发 leader 选举的场景有：
     * 1. controller 第一次启动的时候
     * 2. 监听到路径 /controller 节点中的数据被删除
     * 3. ZK 连接过期并重新建立连接之后
     *
     * @return
     */
    def elect: Boolean = {
        val timestamp = time.milliseconds.toString
        val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

        // 获取 ZK 中记录的 controller leader 的 ID
        leaderId = this.getControllerID
        // 已经存在 controller leader，放弃选举
        if (leaderId != -1) {
            debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
            return amILeader
        }

        try {
            // 尝试创建 ZK 临时节点，如果临时节点已经存在，则抛出异常
            val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                electString,
                controllerContext.zkUtils.zkConnection.getZookeeper,
                JaasUtils.isZkSecurityEnabled)
            zkCheckedEphemeral.create()
            info(brokerId + " successfully elected as leader")
            // 创建成功，更新 leader 节点 ID
            leaderId = brokerId
            // 回调
            onBecomingLeader()
        } catch {
            // leader 已经存在
            case _: ZkNodeExistsException =>
                // If someone else has written the path, then
                leaderId = getControllerID

                if (leaderId != -1)
                    debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
                else
                    warn("A leader has been elected but just resigned, this will result in another round of election")

            case e2: Throwable =>
                error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
                // 重置 leaderId，并删除 /controller 节点
                resign()
        }
        // 检测当前 broker 节点是否成为 leader
        amILeader
    }

    def close: Unit = {
        leaderId = -1
    }

    def amILeader: Boolean = leaderId == brokerId

    def resign(): Boolean = {
        leaderId = -1
        controllerContext.zkUtils.deletePath(electionPath)
    }

    /**
     * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
     * have its own session expiration listener and handler
     */
    class LeaderChangeListener extends IZkDataListener with Logging {
        /**
         * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
         *
         * @throws Exception On any error.
         */
        @throws[Exception]
        def handleDataChange(dataPath: String, data: Object) {
            val shouldResign = inLock(controllerContext.controllerLock) {
                val amILeaderBeforeDataChange = amILeader
                // 更新本地记录的新的 controller leader 的 ID
                leaderId = KafkaController.parseControllerId(data.toString)
                info("New leader is %d".format(leaderId))
                // 之前是 leader，但是现在切换成了 follower 角色
                amILeaderBeforeDataChange && !amILeader
            }
            // 如果当前 broker 由 leader 变为 follower，则需要执行相应的清理工作
            if (shouldResign) onResigningAsLeader()
        }

        /**
         * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
         *
         * @throws Exception
         * On any error.
         */
        @throws[Exception]
        def handleDataDeleted(dataPath: String) {
            val shouldResign = inLock(controllerContext.controllerLock) {
                debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader".format(brokerId, dataPath))
                amILeader
            }

            // 如果 ZK 上记录的 leader 节点被删除，且当前节点之前是 leader，则需要执行相应的清理工作
            if (shouldResign) onResigningAsLeader()

            // 尝试竞选成为新的 leader
            inLock(controllerContext.controllerLock) {
                elect
            }
        }
    }

}
