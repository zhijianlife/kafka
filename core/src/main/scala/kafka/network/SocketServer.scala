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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, ListenerName, Selectable, Selector => KSelector}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is:
 * - 1 Acceptor thread that handles new connections
 * - Acceptor has N Processor threads that each have their own selector and read requests from sockets
 * - M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

    /** 封装服务器对应的多张网卡，kafka 可以同时监听这些 IP 和端口，每个 EndPoint 对应一个 Acceptor */
    private val endpoints: Map[ListenerName, EndPoint] = config.listeners.map(l => l.listenerName -> l).toMap
    /** Processor 对应的线程数 */
    private val numProcessorThreads = config.numNetworkThreads
    /** Processor 线程总数 */
    private val totalProcessorThreads = numProcessorThreads * endpoints.size
    /** 请求队列中缓存的最大请求个数 */
    private val maxQueuedRequests = config.queuedMaxRequests
    /** 每个 IP 允许创建的最大连接数 */
    private val maxConnectionsPerIp = config.maxConnectionsPerIp
    /** 针对特定 IP 指定的允许创建的最大连接数，会覆盖 maxConnectionsPerIp 配置 */
    private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

    this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

    /** Processor 线程与 Handler 线程之间交换数据的通道 */
    val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)

    /** Acceptor 对象集合，每个 EndPoint 对应一个 Acceptor */
    private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
    /** Processor 对象集合，封装所有的 Processor 对象 */
    private val processors = new Array[Processor](totalProcessorThreads)
    /** 用于控制每个 IP 上的最大连接数 */
    private var connectionQuotas: ConnectionQuotas = _

    private val allMetricNames = (0 until totalProcessorThreads).map { i =>
        val tags = new util.HashMap[String, String]()
        tags.put("networkProcessor", i.toString)
        metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
    }

    /**
     * Start the socket server
     */
    def startup() {
        synchronized {

            // 创建控制 IP 最大连接数的 ConnectionQuotas 对象
            connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

            // 指定 socket send buffer 的大小（对应 socket.send.buffer.bytes 配置）
            val sendBufferSize = config.socketSendBufferBytes
            // 指定 socket receive buffer 的大小（对应 socket.receive.buffer.bytes 配置）
            val recvBufferSize = config.socketReceiveBufferBytes
            // 获取 brokerId
            val brokerId = config.brokerId

            var processorBeginIndex = 0
            // 遍历为每个 EndPoint 创建并绑定对应的 Acceptor 和 Processors
            config.listeners.foreach { endpoint =>
                val listenerName = endpoint.listenerName
                val securityProtocol = endpoint.securityProtocol
                val processorEndIndex = processorBeginIndex + numProcessorThreads

                /*
                 * 按照指定的 processor 线程数，为每个 EndPoint 创建对应数量的 Processor 对象，
                 * 编号区间 [processorBeginIndex, processorEndIndex)
                 */
                for (i <- processorBeginIndex until processorEndIndex)
                    processors(i) = this.newProcessor(i, connectionQuotas, listenerName, securityProtocol)

                // 为当前 EndPoint 创建并绑定一个 Acceptor 对象
                val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
                    processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
                acceptors.put(endpoint, acceptor)

                // 启动 Acceptor 线程
                Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()

                // 主线程等待 Acceptor 线程启动完成
                acceptor.awaitStartup()

                processorBeginIndex = processorEndIndex
            }
        }

        newGauge("NetworkProcessorAvgIdlePercent",
            new Gauge[Double] {
                def value: Double = allMetricNames.map { metricName =>
                    Option(metrics.metric(metricName)).fold(0.0)(_.value)
                }.sum / totalProcessorThreads
            }
        )

        info("Started " + acceptors.size + " acceptor threads")
    }

    /**
     * 添加监听器，当 Handler 线程向某个 Processor 的 responseQueue 写入响应数据时，会唤醒该 Processor 线程进行处理
     */
    requestChannel.addResponseListener(id => processors(id).wakeup())

    /**
     * Shutdown the socket server
     */
    def shutdown(): Unit = {
        info("Shutting down")
        this.synchronized {
            acceptors.values.foreach(_.shutdown())
            processors.foreach(_.shutdown())
        }
        info("Shutdown completed")
    }

    /**
     * 获取 listener 对应绑定的 socket 端口
     *
     * @param listenerName
     * @return
     */
    def boundPort(listenerName: ListenerName): Int = {
        try {
            acceptors(endpoints(listenerName)).serverChannel.socket.getLocalPort
        } catch {
            case e: Exception =>
                throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
        }
    }

    /**
     * 创建一个 Processor 对象
     *
     * @param id
     * @param connectionQuotas
     * @param listenerName
     * @param securityProtocol
     * @return
     */
    protected[network] def newProcessor(id: Int,
                                        connectionQuotas: ConnectionQuotas,
                                        listenerName: ListenerName,
                                        securityProtocol: SecurityProtocol): Processor = {
        new Processor(
            id,
            time,
            config.socketRequestMaxBytes,
            requestChannel,
            connectionQuotas,
            config.connectionsMaxIdleMs,
            listenerName,
            securityProtocol,
            config.values,
            metrics,
            credentialProvider
        )
    }

    /* For test usage */
    private[network] def connectionCount(address: InetAddress): Int = Option(connectionQuotas).fold(0)(_.get(address))

    /* For test usage */
    private[network] def processor(index: Int): Processor = processors(index)

} // ~ end of SocketServer

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

    /** 标识当前线程 startup 操作是否完成 */
    private val startupLatch = new CountDownLatch(1)

    /**
     * 标识当前线程 shutdown 操作是否完成
     *
     * `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
     * (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
     * latch and then replace it in `startupComplete()`.
     */
    @volatile private var shutdownLatch = new CountDownLatch(0)

    /** 标识当前线程是否存活 */
    private val alive = new AtomicBoolean(true)

    def wakeup(): Unit

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    def shutdown(): Unit = {
        alive.set(false)
        wakeup()
        // 阻塞等待 shutdown 操作的完成
        shutdownLatch.await()
    }

    /**
     * 阻塞等待启动过程的完成
     *
     * Wait for the thread to completely start up
     */
    def awaitStartup(): Unit = startupLatch.await()

    /**
     * 标识启动操作已经完成
     *
     * Record that the thread startup is complete
     */
    protected def startupComplete(): Unit = {
        // Replace the open latch with a closed one
        shutdownLatch = new CountDownLatch(1)
        startupLatch.countDown()
    }

    /**
     * 标识关闭操作已经完成
     *
     * Record that the thread shutdown is complete
     */
    protected def shutdownComplete(): Unit = shutdownLatch.countDown()

    /**
     * 当前线程是否处于运行态
     *
     * Is the server still running?
     */
    protected def isRunning: Boolean = alive.get

    /**
     * Close the connection identified by `connectionId` and decrement the connection count.
     */
    def close(selector: KSelector, connectionId: String): Unit = {
        val channel = selector.channel(connectionId)
        if (channel != null) {
            debug(s"Closing selector connection $connectionId")
            val address = channel.socketAddress
            // 减少当前 IP 的连接数
            if (address != null) connectionQuotas.dec(address)
            selector.close(connectionId) // 关闭连接
        }
    }

    /**
     * Close `channel` and decrement the connection count.
     */
    def close(channel: SocketChannel): Unit = {
        if (channel != null) {
            debug("Closing connection from " + channel.socket.getRemoteSocketAddress)
            connectionQuotas.dec(channel.socket.getInetAddress)
            swallowError(channel.socket().close())
            swallowError(channel.close())
        }
    }
}

/**
 * 接收客户端建立连接的请求，创建对应的 socket 连接，然后分配给 Processor 进行处理
 *
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint, // 对应的网卡信息
                              val sendBufferSize: Int, // socket send buffer size
                              val recvBufferSize: Int, // socket receive buffer size
                              brokerId: Int, // broker id
                              processors: Array[Processor], // 绑定的 Processor 集合
                              connectionQuotas: ConnectionQuotas // 控制 IP 连接数的对象
                             ) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    /** NIO Selector */
    private val nioSelector = NSelector.open()
    /** ServerSocketChannel 对象，监听对应网卡的指定端口 */
    val serverChannel: ServerSocketChannel = this.openServerSocket(endPoint.host, endPoint.port)

    synchronized {
        // 遍历启动分配给当前 Acceptor 的 Processor 线程
        processors.foreach { processor =>
            Utils.newThread(
                s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
                processor, false).start()
        }
    }

    /**
     * Accept loop that checks for new connection attempts
     */
    def run() {
        // 注册监听 OP_ACCEPT 事件
        serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
        // 标记当前线程启动完成，以便 SocketServer 能够继续为另外的网卡创建对应的 Acceptor
        this.startupComplete()
        try {
            var currentProcessor = 0 // 当前生效的 processor 编号
            while (isRunning) {
                try {
                    // 等待关注的事件
                    val ready = nioSelector.select(500)
                    if (ready > 0) {
                        val keys = nioSelector.selectedKeys()
                        val iter = keys.iterator()
                        // 遍历处理接收到的请求
                        while (iter.hasNext && isRunning) {
                            try {
                                val key = iter.next
                                iter.remove()
                                // 如果是 OP_ACCEPT 事件，则调用 accept 方法进行处理
                                if (key.isAcceptable)
                                    this.accept(key, processors(currentProcessor))
                                else
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                                // 基于轮询算法选择下一个处理的 Processor，负载均衡
                                currentProcessor = (currentProcessor + 1) % processors.length
                            } catch {
                                case e: Throwable => error("Error while accepting connection", e)
                            }
                        }
                    }
                } catch {
                    case e: ControlThrowable => throw e
                    case e: Throwable => error("Error occurred", e)
                }
            }
        } finally {
            debug("Closing server socket and selector.")
            this.swallowError(serverChannel.close())
            this.swallowError(nioSelector.close())
            this.shutdownComplete()
        }
    }

    /**
     * Create a server socket to listen for connections on.
     */
    private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
        val socketAddress =
            if (host == null || host.trim.isEmpty)
                new InetSocketAddress(port)
            else
                new InetSocketAddress(host, port)
        val serverChannel = ServerSocketChannel.open()
        serverChannel.configureBlocking(false)
        if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            serverChannel.socket().setReceiveBufferSize(recvBufferSize)

        try {
            serverChannel.socket.bind(socketAddress)
            info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
        } catch {
            case e: SocketException =>
                throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
        }
        serverChannel
    }

    /**
     * 处理 OP_ACCEPT 事件
     */
    def accept(key: SelectionKey, processor: Processor) {
        val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
        // 创建 SocketChannel 对象
        val socketChannel = serverSocketChannel.accept()
        try {
            // 增加对应 IP 上的连接数，如果连接数超过阈值，则抛 TooManyConnectionsException 异常
            connectionQuotas.inc(socketChannel.socket().getInetAddress)
            // 配置 SocketChannel 对象，非阻塞模式
            socketChannel.configureBlocking(false)
            socketChannel.socket().setTcpNoDelay(true)
            socketChannel.socket().setKeepAlive(true)
            if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
                socketChannel.socket().setSendBufferSize(sendBufferSize)

            this.debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
                    .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                        socketChannel.socket.getSendBufferSize, sendBufferSize,
                        socketChannel.socket.getReceiveBufferSize, recvBufferSize))
            // 将 SocketChannel 交给 Processor 进行处理
            processor.accept(socketChannel)
        } catch {
            // 连接数过多，关闭当前通道上的连接，并将连接计数减 1
            case e: TooManyConnectionsException =>
                info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
                this.close(socketChannel)
        }
    }

    /**
     * Wakeup the thread for selection.
     */
    @Override
    def wakeup(): Unit = nioSelector.wakeup()

} // ~ end of Acceptor

/**
 * 用于读取请求和写回响应，不参与处理具体的业务逻辑
 *
 * Thread that processes all requests from a single connection.
 * There are N of these running in parallel each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel, // Processor 与 Handler 线程之间传递数据的队列
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics,
                               credentialProvider: CredentialProvider)
        extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    private object ConnectionId {
        def fromString(s: String): Option[ConnectionId] = s.split("-") match {
            case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap {
                case (localHost, localPort) =>
                    BrokerEndPoint.parseHostPort(remote).map {
                        case (remoteHost, remotePort) => ConnectionId(localHost, localPort, remoteHost, remotePort)
                    }
            }
            case _ => None
        }
    }

    private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
        override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
    }

    /** 记录分配给当前 Processor 的待处理的 SocketChannel 对象 */
    private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
    /** 缓存未发送给客户端的响应，由于客户端不会进行确认，所以服务端在发送成功之后会将其移除 */
    private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

    private val metricTags = Map("networkProcessor" -> id.toString).asJava

    newGauge("IdlePercent",
        new Gauge[Double] {
            def value: Double = {
                Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)
            }
        },
        metricTags.asScala
    )

    /** 负责管理网络连接 */
    private val selector = new KSelector(
        maxRequestSize,
        connectionsMaxIdleMs,
        metrics,
        time,
        "socket-server",
        metricTags,
        false,
        ChannelBuilders.serverChannelBuilder(securityProtocol, channelConfigs, credentialProvider.credentialCache))

    override def run() {
        // 标识当前线程启动完成
        this.startupComplete()
        while (isRunning) {
            try {
                // 1. 遍历队列获取分配给当前 Processor 的 SocketChannel 对象，注册 OP_READ 事件
                this.configureNewConnections()

                // 2. 遍历处理当前 Processor 的响应队列，依据响应类型进行处理
                this.processNewResponses()

                // 3. 发送之前缓存的响应给客户端
                this.poll()

                /*
                 * 4.
                 * 遍历处理 poll 操作放置在 Selector 的 completedReceives 队列中的请求，
                 * 封装请求信息为 Request 对象，并记录到请求队列中，等待 Handler 线程处理，
                 * 同时标记当前 Selector 暂时不再接收新的请求
                 */
                this.processCompletedReceives()

                /*
                 * 5.
                 * 遍历处理 poll 操作放置在 Selector 的 completedSends 队列中的请求，
                 * 将其从 inflightResponses 集合中移除，并标记当前 Selector 可以继续读取数据
                 */
                this.processCompletedSends()

                /*
                 * 6.
                 * 遍历处理 poll 操作放置在 Selector 的 disconnected 集合中的断开的连接，
                 * 将连接对应的所有响应从 inflightResponses 中移除，同时更新对应的连接数
                 */
                this.processDisconnected()
            } catch {
                case e: ControlThrowable => throw e
                case e: Throwable =>
                    error("Processor got uncaught exception.", e)
            }
        }

        debug("Closing selector - processor " + id)
        // 关闭所有的连接以及选择器
        swallowError(closeAll())
        shutdownComplete()
    }

    /**
     * 遍历处理当前 Processor 的响应队列，依据响应类型进行处理
     */
    private def processNewResponses() {
        // 获取当前 Processor 的响应队列
        var curr = requestChannel.receiveResponse(id)
        while (curr != null) {
            try {
                // 依据响应类型对响应进行处理
                curr.responseAction match {
                    // 暂时没有响应需要发送，如果对应的通道未被关闭，则继续注册 OP_READ 事件读取请求数据
                    case RequestChannel.NoOpAction =>
                        curr.request.updateRequestMetrics()
                        trace("Socket server received empty response to send, registering for read: " + curr)
                        val channelId = curr.request.connectionId
                        if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                            selector.unmute(channelId) // 注册 OP_READ 事件
                    // 当前响应需要发送给客户端
                    case RequestChannel.SendAction =>
                        // 发送该响应，并将响应对象记录到 inflightResponses 集合中
                        this.sendResponse(curr)
                    // 需要关闭当前连接
                    case RequestChannel.CloseConnectionAction =>
                        curr.request.updateRequestMetrics()
                        trace("Closing socket connection actively according to the response code.")
                        // 关闭连接
                        this.close(selector, curr.request.connectionId)
                }
            } finally {
                // 获取下一个待处理的响应
                curr = requestChannel.receiveResponse(id)
            }
        }
    }

    /**
     * 发送响应，并记录到 inflightResponses 集合中
     *
     * @param response
     */
    protected[network] def sendResponse(response: RequestChannel.Response) {
        trace(s"Socket server received response to send, registering for write and sending data: $response")
        // 查找响应对应的通道
        val channel = selector.channel(response.responseSend.destination)
        // `channel` can be null if the selector closed the connection because it was idle for too long
        if (channel == null) {
            warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
            response.request.updateRequestMetrics()
        } else {
            // 发送响应
            selector.send(response.responseSend)
            // 将响应缓存到 inflightResponses 集合中
            inflightResponses += (response.request.connectionId -> response)
        }
    }

    private def poll() {
        // 读取并发送请求
        try selector.poll(300)
        catch {
            // 将 e 绑定为 IllegalStateException 或 IOException 对象
            case e@(_: IllegalStateException | _: IOException) =>
                error(s"Closing processor $id due to illegal state or IO exception")
                swallow(closeAll())
                shutdownComplete()
                throw e
        }
    }

    /**
     * 遍历处理接收到的请求，封装请求信息为 Request 对象，并记录到请求队列中，等待 Handler 线程处理，
     * 同时标记当前 KSelector 暂时不再接收数据
     */
    private def processCompletedReceives() {
        // 遍历处理接收到的请求
        selector.completedReceives.asScala.foreach { receive =>
            try {
                // 获取请求对应的通道
                val openChannel = selector.channel(receive.source)
                // 创建通道对应的 Session 对象，用于权限控制
                val session = {
                    // Only methods that are safe to call on a disconnected channel should be invoked on 'channel'.
                    val channel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
                    RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName), channel.socketAddress)
                }
                // 封装请求信息为 Request 对象
                val req = RequestChannel.Request(
                    processor = id,
                    connectionId = receive.source,
                    session = session,
                    buffer = receive.payload,
                    startTimeMs = time.milliseconds,
                    listenerName = listenerName,
                    securityProtocol = securityProtocol)
                // 将请求对象放入请求队列中，等待 Handler 线程处理
                requestChannel.sendRequest(req)
                // 取消注册的 OP_READ 事件，处理期间不再接收新的请求（即不读取请求对应的数据）
                selector.mute(receive.source)
            } catch {
                case e@(_: InvalidRequestException | _: SchemaException) =>
                    // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
                    error(s"Closing socket for ${receive.source} because of error", e)
                    close(selector, receive.source)
            }
        }
    }

    /**
     * 遍历处理已经发送出去的请求，并将其从 inflightResponses 集合中移除，同时标记当前 KSelector 继续读取数据
     */
    private def processCompletedSends() {
        // 遍历处理已经完全发送出去的请求
        selector.completedSends.asScala.foreach { send =>
            // 因为当前响应已经发送成功，从 inflightResponses 中移除，不需要客户端确认
            val resp = inflightResponses.remove(send.destination).getOrElse {
                throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
            }
            resp.request.updateRequestMetrics()
            // 注册 OP_READ 事件，继续读取请求数据
            selector.unmute(send.destination)
        }
    }

    /**
     * 遍历处理 KSelector 的 disconnected 集合中记录的断开的连接，
     * 将连接对应的所有响应从 inflightResponses 中移除，同时更新记录的连接数
     */
    private def processDisconnected() {
        // 遍历处理已经断开的连接
        selector.disconnected.asScala.foreach { connectionId =>
            val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
                throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
            }.remoteHost
            // 将连接对应的所有响应从 inflightResponses 中移除
            inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
            // 对应的通道已经被关闭，所以需要减少对应 IP 上的连接数
            connectionQuotas.dec(InetAddress.getByName(remoteHost))
        }
    }

    /**
     * Queue up a new connection for reading
     */
    def accept(socketChannel: SocketChannel) {
        // 将 Acceptor 分配的 SocketChannel 对象缓存到同步队列中
        newConnections.add(socketChannel)
        // 唤醒 Processor 线程处理队列
        this.wakeup() // 本质上调用 NIO Server 的 wakeup 方法
    }

    /**
     * 遍历获取队列中缓存的 SocketChannel，注册 OP_READ 事件
     */
    private def configureNewConnections() {
        while (!newConnections.isEmpty) {
            // 获取待处理 SocketChannel 对象
            val channel = newConnections.poll()
            try {
                debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
                val localHost = channel.socket().getLocalAddress.getHostAddress
                val localPort = channel.socket().getLocalPort
                val remoteHost = channel.socket().getInetAddress.getHostAddress
                val remotePort = channel.socket().getPort
                val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
                // 注册 OP_READ 事件
                selector.register(connectionId, channel)
            } catch {
                // 对于不致命的异常，则捕获并关闭对应的通道
                case NonFatal(e) =>
                    val remoteAddress = channel.getRemoteAddress
                    this.close(channel)
                    error(s"Processor $id closed connection from $remoteAddress", e)
            }
        }
    }

    /**
     * Close the selector and all open connections
     */
    private def closeAll() {
        selector.channels.asScala.foreach { channel =>
            close(selector, channel.id)
        }
        selector.close()
    }

    /* For test usage */
    private[network] def channel(connectionId: String): Option[KafkaChannel] =
        Option(selector.channel(connectionId))

    /**
     * Wakeup the thread for selection.
     */
    @Override
    def wakeup(): Unit = selector.wakeup()

} // ~ end of Processor

/**
 * 用于控制每个 IP 上的最大连接数
 *
 * @param defaultMax
 * @param overrideQuotas
 */
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

    private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
    private val counts = mutable.Map[InetAddress, Int]()

    def inc(address: InetAddress) {
        counts.synchronized {
            val count = counts.getOrElseUpdate(address, 0)
            counts.put(address, count + 1)
            val max = overrides.getOrElse(address, defaultMax)
            if (count >= max)
                throw new TooManyConnectionsException(address, max)
        }
    }

    def dec(address: InetAddress) {
        counts.synchronized {
            val count = counts.getOrElse(address,
                throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
            if (count == 1)
                counts.remove(address)
            else
                counts.put(address, count - 1)
        }
    }

    def get(address: InetAddress): Int = counts.synchronized {
        counts.getOrElse(address, 0)
    }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
