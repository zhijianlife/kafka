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

    /** 封装服务器对应的存在多张网卡信息，kafka 可以同时监听这些 IP 和端口，每个 EndPoint 对应一个 Acceptor */
    private val endpoints: Map[ListenerName, EndPoint] = config.listeners.map(l => l.listenerName -> l).toMap
    /** Processor 对应的线程数 */
    private val numProcessorThreads = config.numNetworkThreads
    /** Processor 线程总数 */
    private val totalProcessorThreads = numProcessorThreads * endpoints.size
    /** RequestQueue 中缓存的最大请求个数 */
    private val maxQueuedRequests = config.queuedMaxRequests
    /** 每个 IP 上能够创建的最大连接数 */
    private val maxConnectionsPerIp = config.maxConnectionsPerIp
    /** 针对特定 IP 指定的能够创建的最大连接数，会覆盖 maxConnectionsPerIp 配置 */
    private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

    this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

    /** Processor 线程与 Handler 线程之间交换数据的队列 */
    val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)

    /** Acceptor 对象集合，每个 EndPoint 对应一个 Acceptor */
    private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
    /** Processor 对象集合，包含所有 EndPoint 对应的 Processor 对象 */
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

            connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

            // 指定 socket send buffer 的大小
            val sendBufferSize = config.socketSendBufferBytes
            // 指定 socket receive buffer 的大小
            val recvBufferSize = config.socketReceiveBufferBytes
            // 获取 brokerId
            val brokerId = config.brokerId

            var processorBeginIndex = 0
            // 遍历处理所有的 EndPoint，创建并绑定对应的 Acceptor 和 Processors
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

                // 为当前 EndPoint 创建一个 Acceptor 对象
                val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
                    processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
                // 记录 EndPoint 与 Acceptor 之间的映射关闭
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
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas)
        extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    /** 创建 NIO Selector */
    private val nioSelector = NSelector.open()
    /** 创建 ServerSocketChannel */
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
        // 标记线程启动完成
        this.startupComplete()
        try {
            var currentProcessor = 0
            while (isRunning) {
                try {
                    // 等待关注的事件
                    val ready = nioSelector.select(500)
                    if (ready > 0) {
                        val keys = nioSelector.selectedKeys()
                        val iter = keys.iterator()
                        while (iter.hasNext && isRunning) {
                            try {
                                val key = iter.next
                                iter.remove()
                                // 如果是 OP_ACCEPT 事件，则调用 accept 方法进行处理
                                if (key.isAcceptable)
                                    this.accept(key, processors(currentProcessor))
                                else
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                                // 基于轮询算法选择下一个处理的 Processor
                                currentProcessor = (currentProcessor + 1) % processors.length
                            } catch {
                                case e: Throwable => error("Error while accepting connection", e)
                            }
                        }
                    }
                }
                catch {
                    // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
                    // to a select operation on a specific channel or a bad request. We don't want
                    // the broker to stop responding to requests from other clients in these scenarios.
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

    /*
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
        // 创建 SocketChannel
        val socketChannel = serverSocketChannel.accept()
        try {
            // 增加对应 IP 上的连接数
            connectionQuotas.inc(socketChannel.socket().getInetAddress)
            // 配置 SocketChannel
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
            // 连接数过多
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

    /** 缓存当前 Processor 待处理的 SocketChannel 对象 */
    private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
    /** 缓存未发送给客户端的响应，由于客户端不会进行确认，所以服务端在发送响应成功之后即将其移除 */
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
                // 遍历获取队列中缓存的 SocketChannel，注册 OP_READ 事件
                this.configureNewConnections()

                // 遍历处理当前 Processor 对应的缓存在 RequestChannel 中的响应，依据响应类型进行处理
                this.processNewResponses()

                // 读取并发送之前缓存的响应给客户端
                this.poll()

                /*
                 * 遍历处理 poll 操作放置在 Selector 的 completedReceives 队列中的请求，
                 * 封装请求信息为 Request 对象到 RequestChannel 的 requestQueue 同步队列中，等待 Handler 线程处理
                 * 并标记当前 KSelector 暂时不再读取数据
                 */
                this.processCompletedReceives()

                /*
                 * 遍历处理 poll 操作放置在 KSelector 的 completedSends 队列中的请求，
                 * 将其中 inflightResponses 集合中移除，并标记当前 KSelector 继续读取数据
                 */
                this.processCompletedSends()

                /*
                 * 遍历处理 poll 操作放置在 KSelector 的 disconnected 集合中的断开的连接，
                 * 将连接对应的所有响应从 inflightResponses 中移除，同时更新记录的连接数
                 */
                this.processDisconnected()
            } catch {
                // We catch all the throwables here to prevent the processor thread from exiting. We do this because
                // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
                // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
                // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
                case e: ControlThrowable => throw e
                case e: Throwable =>
                    error("Processor got uncaught exception.", e)
            }
        }

        debug("Closing selector - processor " + id)
        swallowError(closeAll())
        shutdownComplete()
    }

    /**
     * 遍历处理当前 Processor 对应的缓存在 RequestChannel 中的响应，依据响应类型进行处理
     */
    private def processNewResponses() {
        // 获取当前 Processor 对应的缓存在 RequestChannel 中的响应
        var curr = requestChannel.receiveResponse(id)
        while (curr != null) {
            try {
                curr.responseAction match {
                    // 暂时没有响应需要发送
                    case RequestChannel.NoOpAction =>
                        // There is no response to send to the client,
                        // we need to read more pipelined requests that are sitting in the server's socket buffer
                        curr.request.updateRequestMetrics()
                        trace("Socket server received empty response to send, registering for read: " + curr)
                        val channelId = curr.request.connectionId
                        if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                            selector.unmute(channelId) // 注册 OP_READ 事件
                    // 当前响应需要发送给客户端
                    case RequestChannel.SendAction =>
                        // 发送该响应，并记录到 inflightResponses 集合中
                        this.sendResponse(curr)
                    // 需要关闭当前连接
                    case RequestChannel.CloseConnectionAction =>
                        curr.request.updateRequestMetrics()
                        trace("Closing socket connection actively according to the response code.")
                        // 关闭连接
                        this.close(selector, curr.request.connectionId)
                }
            } finally {
                // 获取缓存的下一个响应
                curr = requestChannel.receiveResponse(id)
            }
        }
    }

    /**
     * 发送该响应，并记录到 inflightResponses 集合中
     *
     * @param response
     */
    protected[network] def sendResponse(response: RequestChannel.Response) {
        trace(s"Socket server received response to send, registering for write and sending data: $response")
        // 查找对应的 KafkaChannel
        val channel = selector.channel(response.responseSend.destination)
        // `channel` can be null if the selector closed the connection because it was idle for too long
        if (channel == null) {
            warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
            response.request.updateRequestMetrics()
        }
        else {
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
     * 遍历处理 KSelector 的 completedReceives 集合中读取到的请求，
     * 封装请求信息为 Request 对象到 RequestChannel 的 requestQueue 同步队列中，等待 Handler 线程处理
     * 并标记当前 KSelector 暂时不再接收数据
     */
    private def processCompletedReceives() {
        // 遍历处理 Selector 的 completedReceives 队列
        selector.completedReceives.asScala.foreach { receive =>
            try {
                // 获取请求对应的 KafkaChannel
                val openChannel = selector.channel(receive.source)
                // 创建 KafkaChannel 对应的 Session 对象，用于权限控制
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
                // 将请求对象放入 requestQueue 同步队列中，等待 Handler 线程处理
                requestChannel.sendRequest(req)
                // 取消注册的 OP_READ 事件，不再读取数据
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
     * 遍历处理 KSelector 的 completedSends 集合中记录的已经发送出去的请求，
     * 将其中 inflightResponses 集合中移除，并标记当前 KSelector 继续读取数据
     */
    private def processCompletedSends() {
        // 遍历处理 KSelector 的 completedSends 队列
        selector.completedSends.asScala.foreach { send =>
            // 当前响应已经发送成功，从 inflightResponses 中移除，不需要客户端确认
            val resp = inflightResponses.remove(send.destination).getOrElse {
                throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
            }
            resp.request.updateRequestMetrics()
            // 注册 OP_READ 事件，继续读取数据
            selector.unmute(send.destination)
        }
    }

    /**
     * 遍历处理 KSelector 的 disconnected 集合中记录的断开的连接，
     * 将连接对应的所有响应从 inflightResponses 中移除，同时更新记录的连接数
     */
    private def processDisconnected() {
        // 遍历处理 KSelector 的 disconnected 集合
        selector.disconnected.asScala.foreach { connectionId =>
            val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
                throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
            }.remoteHost
            // 将连接对应的所有响应从 inflightResponses 中移除
            inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
            // 对应的 channel 已经被关闭了，所以需要减少 ConnectionQuotas 记录的连接数
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
            // 获取队首的 SocketChannel
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
                // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
                // throwables will be caught in processor and logged as uncaught exceptions.
                case NonFatal(e) =>
                    val remoteAddress = channel.getRemoteAddress
                    // need to close the channel here to avoid a socket leak.
                    close(channel)
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
