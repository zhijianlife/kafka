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

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._

import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import kafka.api.{ControlledShutdownRequest, RequestOrResponse}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaId
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.network.{ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Protocol, SecurityProtocol}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.apache.log4j.Logger

object RequestChannel extends Logging {
    val AllDone = Request(processor = 1, connectionId = "2", Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost),
        buffer = getShutdownReceive, startTimeMs = 0, listenerName = new ListenerName(""),
        securityProtocol = SecurityProtocol.PLAINTEXT)
    private val requestLogger = Logger.getLogger("kafka.request.logger")

    private def getShutdownReceive = {
        val emptyProduceRequest = new ProduceRequest.Builder(0, 0, new util.HashMap[TopicPartition, MemoryRecords]()).build()
        val emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, emptyProduceRequest.version, "", 0)
        AbstractRequestResponse.serialize(emptyRequestHeader, emptyProduceRequest)
    }

    case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
        val sanitizedUser: String = QuotaId.sanitize(principal.getName)
    }

    case class Request(processor: Int,
                       connectionId: String,
                       session: Session,
                       private var buffer: ByteBuffer,
                       startTimeMs: Long,
                       listenerName: ListenerName,
                       securityProtocol: SecurityProtocol) {
        // These need to be volatile because the readers are in the network thread and the writers are in the request
        // handler threads or the purgatory threads
        @volatile var requestDequeueTimeMs: Long = -1L
        @volatile var apiLocalCompleteTimeMs: Long = -1L
        @volatile var responseCompleteTimeMs: Long = -1L
        @volatile var responseDequeueTimeMs: Long = -1L
        @volatile var apiRemoteCompleteTimeMs: Long = -1L

        val requestId: Short = buffer.getShort()

        // TODO: this will be removed once we remove support for v0 of ControlledShutdownRequest (which depends on a non-standard request header)
        val requestObj: RequestOrResponse = if (requestId == ApiKeys.CONTROLLED_SHUTDOWN_KEY.id)
                                                ControlledShutdownRequest.readFrom(buffer)
                                            else
                                                null

        // if we failed to find a server-side mapping, then try using the client-side request / response format
        val header: RequestHeader =
            if (requestObj == null) {
                buffer.rewind
                try RequestHeader.parse(buffer) // 解析请求头
                catch {
                    case ex: Throwable =>
                        throw new InvalidRequestException(s"Error parsing request header. Our best guess of the apiKey is: $requestId", ex)
                }
            } else
                  null

        // 解析请求体
        val body: AbstractRequest =
            if (requestObj == null)
                try {
                    // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
                    if (header.apiKey == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey, header.apiVersion))
                        new ApiVersionsRequest.Builder().build()
                    else
                        AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
                } catch {
                    case ex: Throwable =>
                        throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
                }
            else
                null

        buffer = null

        def requestDesc(details: Boolean): String = {
            if (requestObj != null)
                requestObj.describe(details)
            else
                s"$header -- $body"
        }

        trace("Processor %d received request : %s".format(processor, requestDesc(true)))

        def updateRequestMetrics() {
            val endTimeMs = Time.SYSTEM.milliseconds
            // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes if the remote
            // processing time is really small. This value is set in KafkaApis from a request handling thread.
            // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
            // see a negative value here. In that case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
            if (apiLocalCompleteTimeMs < 0)
                apiLocalCompleteTimeMs = responseCompleteTimeMs
            // If the apiRemoteCompleteTimeMs is not set (i.e., for requests that do not go through a purgatory), then it is
            // the same as responseCompleteTimeMs.
            if (apiRemoteCompleteTimeMs < 0)
                apiRemoteCompleteTimeMs = responseCompleteTimeMs

            val requestQueueTime = math.max(requestDequeueTimeMs - startTimeMs, 0)
            val apiLocalTime = math.max(apiLocalCompleteTimeMs - requestDequeueTimeMs, 0)
            val apiRemoteTime = math.max(apiRemoteCompleteTimeMs - apiLocalCompleteTimeMs, 0)
            val apiThrottleTime = math.max(responseCompleteTimeMs - apiRemoteCompleteTimeMs, 0)
            val responseQueueTime = math.max(responseDequeueTimeMs - responseCompleteTimeMs, 0)
            val responseSendTime = math.max(endTimeMs - responseDequeueTimeMs, 0)
            val totalTime = endTimeMs - startTimeMs
            val fetchMetricNames =
                if (requestId == ApiKeys.FETCH.id) {
                    val isFromFollower = body.asInstanceOf[FetchRequest].isFromFollower
                    Seq(
                        if (isFromFollower) RequestMetrics.followFetchMetricName
                        else RequestMetrics.consumerFetchMetricName
                    )
                }
                else Seq.empty
            val metricNames = fetchMetricNames :+ ApiKeys.forId(requestId).name
            metricNames.foreach { metricName =>
                val m = RequestMetrics.metricsMap(metricName)
                m.requestRate.mark()
                m.requestQueueTimeHist.update(requestQueueTime)
                m.localTimeHist.update(apiLocalTime)
                m.remoteTimeHist.update(apiRemoteTime)
                m.throttleTimeHist.update(apiThrottleTime)
                m.responseQueueTimeHist.update(responseQueueTime)
                m.responseSendTimeHist.update(responseSendTime)
                m.totalTimeHist.update(totalTime)
            }

            if (requestLogger.isDebugEnabled) {
                val detailsEnabled = requestLogger.isTraceEnabled
                requestLogger.trace("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,principal:%s,listener:%s"
                        .format(requestDesc(detailsEnabled), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal, listenerName.value))
            }
        }
    }

    case class Response(processor: Int,
                        request: Request,
                        responseSend: Send,
                        responseAction: ResponseAction) {
        request.responseCompleteTimeMs = Time.SYSTEM.milliseconds

        def this(processor: Int, request: Request, responseSend: Send) =
            this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

        def this(request: Request, send: Send) =
            this(request.processor, request, send)

        def this(request: Request, response: AbstractResponse) =
            this(request, response.toSend(request.connectionId, request.header))
    }

    /**
     * 响应行为
     */
    trait ResponseAction

    /**
     * 表示当前响应需要发送给客户端
     */
    case object SendAction extends ResponseAction

    /**
     * 表示当前暂时没有响应需要发送
     */
    case object NoOpAction extends ResponseAction

    /**
     * 表示需要关闭对应的连接
     */
    case object CloseConnectionAction extends ResponseAction

}

/**
 * Processor 线程与 Handler 线程之间交换数据的队列，
 * Processor 将读取到的请求存入 requestQueue 中，Handler 线程从该队列中读取请求并处理，
 * 然后将响应存储到放置该请求的 Processor 的 responseQueue 中，
 * Processor 负责从自己的 responseQueue 中取出响应发送给客户端
 *
 * @param numProcessors processor 线程数
 * @param queueSize     请求队列大小
 */
class RequestChannel(val numProcessors: Int, // Processor 数目
                     val queueSize: Int // 请求队列的最大长度
                    ) extends KafkaMetricsGroup {

    /** 响应监听器列表，用于在 Handler 往响应队列中防止响应时唤醒对应的 Processor */
    private var responseListeners: List[Int => Unit] = Nil

    /** 请求队列 */
    private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)

    /** 响应队列，每个 Processor 对应一个响应队列 */
    private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)

    // 为每个 Processor 注册一个响应队列
    for (i <- 0 until numProcessors)
        responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

    this.newGauge(
        "RequestQueueSize",
        new Gauge[Int] {
            def value: Int = requestQueue.size
        }
    )

    this.newGauge("ResponseQueueSize", new Gauge[Int] {
        def value: Int = responseQueues.foldLeft(0) { (total, q) => total + q.size() }
    })

    for (i <- 0 until numProcessors) {
        this.newGauge("ResponseQueueSize",
            new Gauge[Int] {
                def value: Int = responseQueues(i).size()
            },
            Map("processor" -> i.toString)
        )
    }

    /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
    def sendRequest(request: RequestChannel.Request) {
        requestQueue.put(request)
    }

    /** Send a response back to the socket server to be sent over the network */
    def sendResponse(response: RequestChannel.Response) {
        responseQueues(response.processor).put(response)
        // 遍历激活监听器，唤醒对应的 Processor 线程
        for (onResponse <- responseListeners)
            onResponse(response.processor)
    }

    /** No operation to take for the request, need to read more over the network */
    def noOperation(processor: Int, request: RequestChannel.Request) {
        responseQueues(processor).put(RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
        for (onResponse <- responseListeners)
            onResponse(processor)
    }

    /** Close the connection for the request */
    def closeConnection(processor: Int, request: RequestChannel.Request) {
        responseQueues(processor).put(RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
        for (onResponse <- responseListeners)
            onResponse(processor)
    }

    /** Get the next request or block until specified time has elapsed */
    def receiveRequest(timeout: Long): RequestChannel.Request =
        requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

    /** Get the next request or block until there is one */
    def receiveRequest(): RequestChannel.Request =
        requestQueue.take()

    /** Get a response for the given processor if there is one */
    def receiveResponse(processor: Int): RequestChannel.Response = {
        val response = responseQueues(processor).poll()
        if (response != null)
            response.request.responseDequeueTimeMs = Time.SYSTEM.milliseconds
        response
    }

    def addResponseListener(onResponse: Int => Unit) {
        responseListeners ::= onResponse
    }

    def shutdown() {
        requestQueue.clear()
    }
}

object RequestMetrics {
    val metricsMap = new scala.collection.mutable.HashMap[String, RequestMetrics]
    val consumerFetchMetricName: String = ApiKeys.FETCH.name + "Consumer"
    val followFetchMetricName: String = ApiKeys.FETCH.name + "Follower"
    (ApiKeys.values().toList.map(e => e.name)
            ++ List(consumerFetchMetricName, followFetchMetricName)).foreach(name => metricsMap.put(name, new RequestMetrics(name)))
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {
    val tags: Map[String, String] = Map("request" -> name)
    val requestRate: Meter = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags)
    // time a request spent in a request queue
    val requestQueueTimeHist: Histogram = newHistogram("RequestQueueTimeMs", biased = true, tags)
    // time a request takes to be processed at the local broker
    val localTimeHist: Histogram = newHistogram("LocalTimeMs", biased = true, tags)
    // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
    val remoteTimeHist: Histogram = newHistogram("RemoteTimeMs", biased = true, tags)
    // time a request is throttled (only relevant to fetch and produce requests)
    val throttleTimeHist: Histogram = newHistogram("ThrottleTimeMs", biased = true, tags)
    // time a response spent in a response queue
    val responseQueueTimeHist: Histogram = newHistogram("ResponseQueueTimeMs", biased = true, tags)
    // time to send the response to the requester
    val responseSendTimeHist: Histogram = newHistogram("ResponseSendTimeMs", biased = true, tags)
    val totalTimeHist: Histogram = newHistogram("TotalTimeMs", biased = true, tags)
}
