/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 *
 * 缓存已经发出去但没有收到响应的 {@link ClientRequest}
 */
final class InFlightRequests {

    private final int maxInFlightRequestsPerConnection;

    /** key 是节点 ID，value 是发送到该节点的 ClientRequest 对象集合 */
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    public void add(NetworkClient.InFlightRequest request) {
        String destination = request.destination;
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(destination, reqs);
        }
        reqs.addFirst(request);
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty()) {
            throw new IllegalStateException("Response from server for which there are no in-flight requests.");
        }
        return reqs;
    }

    /**
     * Get the oldest request (the one that that will be completed next) for the given node
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        return requestQueue(node).pollLast();
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     *
     * 获取发送到目标节点的最后一次请求
     *
     * @param node The node id
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     *
     * @param node The node the request was sent to
     * @return The request
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        return requestQueue(node).pollFirst();
    }

    /**
     * NetworkClient 调用此方法判断是否可以向指定节点发送请求
     *
     * Can we send more requests to this node?
     *
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    public boolean canSendMore(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty() || // 本地没有缓存的 ClientRequest 对象
                (queue.peekFirst().send.completed() // 对头请求已经发送完成，否则可能存在网络问题，导致请求发送不出去
                        && queue.size() < this.maxInFlightRequestsPerConnection); // 如果队列大小大于等于该阈值则说明网络可能存在问题
    }

    /**
     * Return the number of inflight requests directed at the given node
     *
     * @param node The node
     * @return The request count.
     */
    public int inFlightRequestCount(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Count all in-flight requests for all nodes
     */
    public int inFlightRequestCount() {
        int total = 0;
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values())
            total += deque.size();
        return total;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            return requests.remove(node);
        }
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @param requestTimeout max time to wait for the request to be completed
     * @return list of nodes
     */
    public List<String> getNodesWithTimedOutRequests(long now, int requestTimeout) {
        List<String> nodeIds = new LinkedList<>();
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();

            if (!deque.isEmpty()) {
                NetworkClient.InFlightRequest request = deque.peekLast();
                long timeSinceSend = now - request.sendTimeMs;
                if (timeSinceSend > requestTimeout) {
                    nodeIds.add(nodeId);
                }
            }
        }

        return nodeIds;
    }

}
