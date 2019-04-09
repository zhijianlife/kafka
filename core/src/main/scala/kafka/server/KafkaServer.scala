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

import java.io.{File, IOException}
import java.net.SocketTimeoutException
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.api.KAFKA_0_9_0
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException}
import kafka.controller.{ControllerStats, KafkaController}
import kafka.coordinator.GroupCoordinator
import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.network.{BlockingChannel, SocketServer}
import kafka.security.CredentialProvider
import kafka.security.auth.Authorizer
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.{ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.common.{ClusterResource, Node}

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

object KafkaServer {

    /**
     * Copy the subset of properties that are relevant to Logs
     * I'm listing out individual properties here since the names are slightly different in each Config class...
     *
     * @param kafkaConfig
     * @return
     */
    private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
        val logProps = new util.HashMap[String, Object]()
        logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
        logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
        logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
        logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
        logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
        logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
        logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
        logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
        logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
        logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
        logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
        logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
        logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
        logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
        logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
        logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
        logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
        logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
        logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
        logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
        logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
        logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs)
        logProps
    }

    private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
        new MetricConfig()
                .samples(kafkaConfig.metricNumSamples)
                .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
                .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
    }

}

/**
 * Represents the lifecycle of a single Kafka broker.
 * Handles all functionality required to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, // 配置信息对象
                  time: Time = Time.SYSTEM, // 时间戳工具
                  threadNamePrefix: Option[String] = None,
                  kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List() // 监控上报程序
                 ) extends Logging with KafkaMetricsGroup {

    /** 标识 kafka 已经启动完成 */
    private val startupComplete = new AtomicBoolean(false)
    /** 标识 kafka 正在执行关闭操作 */
    private val isShuttingDown = new AtomicBoolean(false)
    /** 标识 kafka 正在执行启动操作 */
    private val isStartingUp = new AtomicBoolean(false)

    /** 用于阻塞主线程等待 KafkaServer 的关闭 */
    private var shutdownLatch = new CountDownLatch(1)

    private val jmxPrefix: String = "kafka.server"

    var metrics: Metrics = _

    /** 记录当前 broker 节点的状态 */
    val brokerState: BrokerState = new BrokerState

    var apis: KafkaApis = _
    var authorizer: Option[Authorizer] = None
    var socketServer: SocketServer = _
    var requestHandlerPool: KafkaRequestHandlerPool = _

    var logManager: LogManager = _

    var replicaManager: ReplicaManager = _
    var adminManager: AdminManager = _

    var dynamicConfigHandlers: Map[String, ConfigHandler] = _
    var dynamicConfigManager: DynamicConfigManager = _
    var credentialProvider: CredentialProvider = _

    var groupCoordinator: GroupCoordinator = _

    var kafkaController: KafkaController = _

    val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)

    var kafkaHealthcheck: KafkaHealthcheck = _
    var metadataCache: MetadataCache = _
    var quotaManagers: QuotaFactory.QuotaManagers = _

    var zkUtils: ZkUtils = _
    val correlationId: AtomicInteger = new AtomicInteger(0)
    val brokerMetaPropsFile = "meta.properties"

    // 在每个 log 目录下面创建一个 meta.properties 文件
    val brokerMetadataCheckpoints: Predef.Map[String, BrokerMetadataCheckpoint] =
        config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

    private var _clusterId: String = _

    def clusterId: String = _clusterId

    newGauge(
        "BrokerState",
        new Gauge[Int] {
            def value: Int = brokerState.currentState
        }
    )

    newGauge(
        "ClusterId",
        new Gauge[String] {
            def value: String = clusterId
        }
    )

    newGauge(
        "yammer-metrics-count",
        new Gauge[Int] {
            def value: Int = {
                com.yammer.metrics.Metrics.defaultRegistry().allMetrics().size()
            }
        }
    )

    /**
     * Start up API for bringing up a single instance of the Kafka server.
     * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
     */
    def startup() {
        try {
            info("starting")

            // kafka 当前正在执行关闭操作，期间不允许重新启动
            if (isShuttingDown.get)
                throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

            // kafka 已经启动完成，不需要重复启动
            if (startupComplete.get)
                return

            // 标识 kafka 正在启动，防止启动过程中重复启动
            val canStartup = isStartingUp.compareAndSet(false, true)
            if (canStartup) {
                // 设置 broker 节点的状态为 Starting
                brokerState.newState(Starting)

                // 初始化定时任务调度器
                kafkaScheduler.startup()

                // 创建 ZkUtils 工具类对象，期间会在 ZK 上创建一些基本的节点
                zkUtils = initZk()

                // 从 ZK 上获取 clusterId，如果不存在则创建一个
                _clusterId = getOrGenerateClusterId(zkUtils)
                info(s"Cluster ID = $clusterId")

                /*
                 * 获取 brokerId
                 * - 如果在配置中配置了 broker.id，则需要保证与对应 log 目录下 meta.properties 文件中记录的一致
                 * - 如果没有配置，则自动生成一个，基于 ZK 保证各个 broker 的 ID 不重复
                 */
                config.brokerId = getBrokerId
                this.logIdent = "[Kafka Server " + config.brokerId + "], "

                // 初始化一些监控相关的配置
                val reporters = config.getConfiguredInstances(KafkaConfig.MetricReporterClassesProp,
                    classOf[MetricsReporter],
                    Map[String, AnyRef](KafkaConfig.BrokerIdProp -> config.brokerId.toString).asJava)
                reporters.add(new JmxReporter(jmxPrefix))
                val metricConfig = KafkaServer.metricConfig(config)
                metrics = new Metrics(metricConfig, reporters, time, true)

                quotaManagers = QuotaFactory.instantiate(config, metrics, time)
                notifyClusterListeners(kafkaMetricsReporters ++ reporters.asScala)

                // 创建并启动 LogManager，提供了对 log 文件管理的功能
                logManager = this.createLogManager(zkUtils.zkClient, brokerState)
                logManager.startup()

                // 创建 MetadataCache 对象，broker 使用该对象缓存整个集群中全部分区的状态
                metadataCache = new MetadataCache(config.brokerId)
                // 权限相关
                credentialProvider = new CredentialProvider(config.saslEnabledMechanisms)

                // 创建并启动 SocketServer，用于接收并处理请求
                socketServer = new SocketServer(config, metrics, time, credentialProvider)
                socketServer.startup()

                // 创建并启动 ReplicaManager，用于管理当前 broker 节点上的分区信息
                replicaManager = new ReplicaManager(config, metrics, time, zkUtils, kafkaScheduler, logManager, isShuttingDown, quotaManagers.follower)
                replicaManager.startup()

                /*
                 * 创建并启动 KafkaController，每个 broker 节点都会创建并启动一个 KafkaController 对象，但是只有一个 broker 会成为 controller leader，
                 * controller leader 负责管理集群中所有的分区和副本的状态，也是 kafka 与 ZK 进行交互的桥梁。
                 */
                kafkaController = new KafkaController(config, zkUtils, brokerState, time, metrics, threadNamePrefix)
                kafkaController.startup()

                // 创建 AdminManager 对象
                adminManager = new AdminManager(config, metrics, metadataCache, zkUtils)

                // 创建并启动 GroupCoordinator，负责管理分配给当前 broker 的消费者 group 的一个子集
                groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
                groupCoordinator.startup()

                // 创建并初始化 Authorizer 对象
                authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
                    val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
                    authZ.configure(config.originals())
                    authZ
                }

                // 创建 KafkaApis 对象，用于分发接收到的所有的请求
                apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator,
                    kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers, clusterId, time)

                // 创建 KafkaRequestHandlerPool 对象，简单的线程池实现，用于管理所有 KafkaRequestHandler 线程
                requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, time, config.numIoThreads)

                Mx4jLoader.maybeLoad()

                // 创建并启动动态配置管理器，用于监听 ZK 的变更
                dynamicConfigHandlers = Map[String, ConfigHandler](
                    ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers),
                    ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                    ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                    ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))
                dynamicConfigManager = new DynamicConfigManager(zkUtils, dynamicConfigHandlers)
                dynamicConfigManager.startup()

                /* tell everyone we are alive */
                val listeners = config.advertisedListeners.map { endpoint =>
                    if (endpoint.port == 0)
                        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
                    else
                        endpoint
                }
                kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack, config.interBrokerProtocolVersion)
                kafkaHealthcheck.startup()

                // Now that the broker id is successfully registered via KafkaHealthcheck, checkpoint it
                checkpointBrokerId(config.brokerId)

                /* register broker metrics */
                registerStats()

                // 设置 broker 状态为 RunningAsBroker
                brokerState.newState(RunningAsBroker)
                // 启动成功后将 shutdownLatch 设置为 1，阻塞主线程关闭
                shutdownLatch = new CountDownLatch(1)
                // 标识 kafka 启动完成
                startupComplete.set(true)
                isStartingUp.set(false)
                AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
                info("started")
            }
        }
        catch {
            case e: Throwable =>
                fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
                isStartingUp.set(false)
                shutdown()
                throw e
        }
    }

    def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
        val clusterResourceListeners = new ClusterResourceListeners
        clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
        clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
    }

    private def initZk(): ZkUtils = {
        info(s"Connecting to zookeeper on ${config.zkConnect}")

        // localhost:2181/kafka
        val chrootIndex = config.zkConnect.indexOf("/")
        val chrootOption = {
            if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
            else None
        }

        val secureAclsEnabled = config.zkEnableSecureAcls
        val isZkSecurityEnabled = JaasUtils.isZkSecurityEnabled

        if (secureAclsEnabled && !isZkSecurityEnabled)
            throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but the verification of the JAAS login file failed.")

        // 如果指定了 chroot 节点，则在 ZK 上进行创建
        chrootOption.foreach { chroot =>
            val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
            val zkClientForChrootCreation = ZkUtils(
                zkConnForChrootCreation,
                sessionTimeout = config.zkSessionTimeoutMs,
                connectionTimeout = config.zkConnectionTimeoutMs,
                secureAclsEnabled)
            // 在 ZK 上创建 chroot 节点
            zkClientForChrootCreation.makeSurePersistentPathExists(chroot)
            info(s"Created zookeeper path $chroot")
            zkClientForChrootCreation.zkClient.close()
        }

        val zkUtils = ZkUtils(
            config.zkConnect,
            sessionTimeout = config.zkSessionTimeoutMs,
            connectionTimeout = config.zkConnectionTimeoutMs,
            secureAclsEnabled)
        // 遍历在 ZK 上创建一些基础节点
        zkUtils.setupCommonPaths()
        zkUtils
    }

    /**
     * 从 ZK 上获取 clusterId，如果不存在则创建一个
     *
     * @param zkUtils
     * @return
     */
    def getOrGenerateClusterId(zkUtils: ZkUtils): String = {
        zkUtils.getClusterId.getOrElse(zkUtils.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
    }

    /**
     * Forces some dynamic jmx beans to be registered on server startup.
     */
    private def registerStats() {
        BrokerTopicStats.getBrokerAllTopicsStats
        ControllerStats.uncleanLeaderElectionRate
        ControllerStats.leaderElectionTimer
    }

    /**
     * Performs controlled shutdown
     */
    private def controlledShutdown() {

        def node(broker: Broker): Node = {
            val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
            new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
        }

        val socketTimeoutMs = config.controllerSocketTimeoutMs

        def networkClientControlledShutdown(retries: Int): Boolean = {
            val metadataUpdater = new ManualMetadataUpdater()
            val networkClient = {
                val channelBuilder = ChannelBuilders.clientChannelBuilder(
                    config.interBrokerSecurityProtocol,
                    LoginType.SERVER,
                    config.values,
                    config.saslMechanismInterBrokerProtocol,
                    config.saslInterBrokerHandshakeRequestEnable)
                val selector = new Selector(
                    NetworkReceive.UNLIMITED,
                    config.connectionsMaxIdleMs,
                    metrics,
                    time,
                    "kafka-server-controlled-shutdown",
                    Map.empty[String, String].asJava,
                    false,
                    channelBuilder
                )

                new NetworkClient(
                    selector,
                    metadataUpdater,
                    config.brokerId.toString,
                    1,
                    0,
                    Selectable.USE_DEFAULT_BUFFER_SIZE,
                    Selectable.USE_DEFAULT_BUFFER_SIZE,
                    config.requestTimeoutMs,
                    time,
                    false)
            }

            var shutdownSucceeded: Boolean = false

            try {

                var remainingRetries = retries
                var prevController: Broker = null
                var ioException = false

                while (!shutdownSucceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1

                    import NetworkClientBlockingOps._

                    // 1. Find the controller and establish a connection to it.

                    // Get the current controller info. This is to ensure we use the most recent info to issue the
                    // controlled shutdown request
                    val controllerId = zkUtils.getController
                    zkUtils.getBrokerInfo(controllerId) match {
                        case Some(broker) =>
                            // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                            // attempt, connect to the most recent controller
                            if (ioException || broker != prevController) {

                                ioException = false

                                if (prevController != null)
                                    networkClient.close(node(prevController).idString)

                                prevController = broker
                                metadataUpdater.setNodes(Seq(node(prevController)).asJava)
                            }
                        case None => //ignore and try again
                    }

                    // 2. issue a controlled shutdown to the controller
                    if (prevController != null) {
                        try {

                            if (!networkClient.blockingReady(node(prevController), socketTimeoutMs)(time))
                                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

                            // send the controlled shutdown request
                            val controlledShutdownRequest = new ControlledShutdownRequest.Builder(config.brokerId)
                            val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                                time.milliseconds(), true)
                            val clientResponse = networkClient.blockingSendAndReceive(request)(time)

                            val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
                            if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining.isEmpty) {
                                shutdownSucceeded = true
                                info("Controlled shutdown succeeded")
                            }
                            else {
                                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.asScala.mkString(",")))
                                info("Error code from controller: %d".format(shutdownResponse.errorCode))
                            }
                        }
                        catch {
                            case ioe: IOException =>
                                ioException = true
                                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                            // ignore and try again
                        }
                    }
                    if (!shutdownSucceeded) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs)
                        warn("Retrying controlled shutdown after the previous attempt failed...")
                    }
                }
            }
            finally
                networkClient.close()

            shutdownSucceeded
        }

        def blockingChannelControlledShutdown(retries: Int): Boolean = {
            var remainingRetries = retries
            var channel: BlockingChannel = null
            var prevController: Broker = null
            var shutdownSucceeded: Boolean = false
            try {
                while (!shutdownSucceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1

                    // 1. Find the controller and establish a connection to it.

                    // Get the current controller info. This is to ensure we use the most recent info to issue the
                    // controlled shutdown request
                    val controllerId = zkUtils.getController
                    zkUtils.getBrokerInfo(controllerId) match {
                        case Some(broker) =>
                            if (channel == null || prevController == null || !prevController.equals(broker)) {
                                // if this is the first attempt or if the controller has changed, create a channel to the most recent
                                // controller
                                if (channel != null)
                                    channel.disconnect()

                                val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
                                channel = new BlockingChannel(brokerEndPoint.host,
                                    brokerEndPoint.port,
                                    BlockingChannel.UseDefaultBufferSize,
                                    BlockingChannel.UseDefaultBufferSize,
                                    config.controllerSocketTimeoutMs)
                                channel.connect()
                                prevController = broker
                            }
                        case None => //ignore and try again
                    }

                    // 2. issue a controlled shutdown to the controller
                    if (channel != null) {
                        var response: NetworkReceive = null
                        try {
                            // send the controlled shutdown request
                            val request = new kafka.api.ControlledShutdownRequest(0, correlationId.getAndIncrement, None, config.brokerId)
                            channel.send(request)

                            response = channel.receive()
                            val shutdownResponse = kafka.api.ControlledShutdownResponse.readFrom(response.payload())
                            if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining != null &&
                                    shutdownResponse.partitionsRemaining.isEmpty) {
                                shutdownSucceeded = true
                                info("Controlled shutdown succeeded")
                            }
                            else {
                                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.mkString(",")))
                                info("Error code from controller: %d".format(shutdownResponse.errorCode))
                            }
                        }
                        catch {
                            case ioe: java.io.IOException =>
                                channel.disconnect()
                                channel = null
                                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                            // ignore and try again
                        }
                    }
                    if (!shutdownSucceeded) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs)
                        warn("Retrying controlled shutdown after the previous attempt failed...")
                    }
                }
            }
            finally {
                if (channel != null) {
                    channel.disconnect()
                    channel = null
                }
            }
            shutdownSucceeded
        }

        if (startupComplete.get() && config.controlledShutdownEnable) {
            // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
            // of time and try again for a configured number of retries. If all the attempt fails, we simply force the shutdown.
            info("Starting controlled shutdown")

            brokerState.newState(PendingControlledShutdown)

            val shutdownSucceeded =
            // Before 0.9.0.0, `ControlledShutdownRequest` did not contain `client_id` and it's a mandatory field in
            // `RequestHeader`, which is used by `NetworkClient`
                if (config.interBrokerProtocolVersion >= KAFKA_0_9_0)
                    networkClientControlledShutdown(config.controlledShutdownMaxRetries.intValue)
                else blockingChannelControlledShutdown(config.controlledShutdownMaxRetries.intValue)

            if (!shutdownSucceeded)
                warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")

        }
    }

    /**
     * Shutdown API for shutting down a single instance of the Kafka server.
     * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
     */
    def shutdown() {
        try {
            info("shutting down")

            // 如果正在启动，则不允许关闭
            if (isStartingUp.get)
                throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

            if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
                CoreUtils.swallow(controlledShutdown())
                // 设置 broker 状态为 BrokerShuttingDown，标识当前 broker 正在执行关闭
                brokerState.newState(BrokerShuttingDown)

                /* 关闭相应注册的组件 */

                if (socketServer != null)
                    CoreUtils.swallow(socketServer.shutdown())
                if (requestHandlerPool != null)
                    CoreUtils.swallow(requestHandlerPool.shutdown())
                CoreUtils.swallow(kafkaScheduler.shutdown())
                if (apis != null)
                    CoreUtils.swallow(apis.close())
                CoreUtils.swallow(authorizer.foreach(_.close()))
                if (replicaManager != null)
                    CoreUtils.swallow(replicaManager.shutdown())
                if (adminManager != null)
                    CoreUtils.swallow(adminManager.shutdown())
                if (groupCoordinator != null)
                    CoreUtils.swallow(groupCoordinator.shutdown())
                if (logManager != null)
                    CoreUtils.swallow(logManager.shutdown())
                if (kafkaController != null)
                    CoreUtils.swallow(kafkaController.shutdown())
                if (zkUtils != null)
                    CoreUtils.swallow(zkUtils.close())
                if (metrics != null)
                    CoreUtils.swallow(metrics.close())

                // 设置 broker 状态为 NotRunning，表示关闭成功
                brokerState.newState(NotRunning)

                // 设置状态标记
                startupComplete.set(false)
                isShuttingDown.set(false)
                CoreUtils.swallow(AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString))
                shutdownLatch.countDown()
                info("shut down completed")
            }
        } catch {
            case e: Throwable =>
                fatal("Fatal error during KafkaServer shutdown.", e)
                isShuttingDown.set(false)
                throw e
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    def awaitShutdown(): Unit = shutdownLatch.await()

    def getLogManager: LogManager = logManager

    def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

    private def createLogManager(zkClient: ZkClient, brokerState: BrokerState): LogManager = {
        // 使用 LogConfig 封装日志相关的配置
        val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
        val defaultLogConfig = LogConfig(defaultProps)

        // 从 ZK 上获取所有 topic 的配置信息，和 defaultProps 一起封装成 LogConfig 对象
        val configs = AdminUtils.fetchAllTopicConfigs(zkUtils).map {
            case (topic, cfs) => topic -> LogConfig.fromProps(defaultProps, cfs)
        }

        // 读取 log cleaner 相关配置，封装成 CleanerConfig 对象
        val cleanerConfig = CleanerConfig(
            numThreads = config.logCleanerThreads,
            dedupeBufferSize = config.logCleanerDedupeBufferSize,
            dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
            ioBufferSize = config.logCleanerIoBufferSize,
            maxMessageSize = config.messageMaxBytes,
            maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
            backOffMs = config.logCleanerBackoffMs,
            enableCleaner = config.logCleanerEnable)

        // 创建并返回 LogManager 对象
        new LogManager(
            logDirs = config.logDirs.map(new File(_)).toArray,
            topicConfigs = configs,
            defaultConfig = defaultLogConfig,
            cleanerConfig = cleanerConfig,
            ioThreads = config.numRecoveryThreadsPerDataDir,
            flushCheckMs = config.logFlushSchedulerIntervalMs,
            flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
            retentionCheckMs = config.logCleanupIntervalMs,
            scheduler = kafkaScheduler,
            brokerState = brokerState,
            time = time)
    }

    /**
     * Generates new brokerId if enabled or reads from meta.properties based on following conditions
     * <ol>
     * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
     * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
     * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
     * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
     * <ol>
     *
     * @return A brokerId.
     */
    private def getBrokerId: Int = {
        // 获取配置的 brokerId
        var brokerId = config.brokerId
        val brokerIdSet = mutable.HashSet[Int]()

        // 遍历 log.dirs
        for (logDir <- config.logDirs) {
            // 在每一个 log 目录下面创建一个 meta.properties 文件，内容包含当前 broker 节点的 ID 和版本信息
            val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
            brokerMetadataOpt.foreach { brokerMetadata =>
                brokerIdSet.add(brokerMetadata.brokerId)
            }
        }

        if (brokerIdSet.size > 1) {
            // 不允许多个 broker 共享一个 log 目录
            throw new InconsistentBrokerIdException(
                s"Failed to match broker.id across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
                        s"or partial data was manually copied from another broker. Found $brokerIdSet")
        }
        else if (brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId) {
            // 配置的 brokerId 与 meta.properties 中记录的 brokerId 不一致
            throw new InconsistentBrokerIdException(
                s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerIdSet.last} in meta.properties. " +
                        s"If you moved your data, make sure your configured broker.id matches. " +
                        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
        }
        else if (brokerIdSet.isEmpty && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
             // 如果没有配置，则自动创建 brokerId，通过 ZK 保证 brokerId 的全局唯一性
                 brokerId = generateBrokerId
        else if (brokerIdSet.size == 1) // pick broker.id from meta.properties
             // 从 meta.properties 中获取 brokerId
                 brokerId = brokerIdSet.last

        brokerId
    }

    private def checkpointBrokerId(brokerId: Int) {
        var logDirsWithoutMetaProps: List[String] = List()

        for (logDir <- config.logDirs) {
            val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
            if (brokerMetadataOpt.isEmpty)
                logDirsWithoutMetaProps ++= List(logDir)
        }

        for (logDir <- logDirsWithoutMetaProps) {
            val checkpoint = brokerMetadataCheckpoints(logDir)
            checkpoint.write(BrokerMetadata(brokerId))
        }
    }

    private def generateBrokerId: Int = {
        try {
            zkUtils.getBrokerSequenceId(config.maxReservedBrokerId)
        } catch {
            case e: Exception =>
                error("Failed to generate broker.id due to ", e)
                throw new GenerateBrokerIdException("Failed to generate broker.id", e)
        }
    }
}
