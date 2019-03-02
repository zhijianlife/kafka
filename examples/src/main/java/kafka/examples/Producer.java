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

package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer extends Thread {

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        // 设置 kafka 集群地址
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        // 设置客户端 ID
        props.put("client.id", "DemoProducer");
        // 设置 key 序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        // 设置 value 序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 创建生产者实例
        this.producer = new KafkaProducer<>(props);
        // 设置目标主题
        this.topic = topic;
        // 是否以异步的方式发送消息
        this.isAsync = isAsync;
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (!Thread.currentThread().isInterrupted()) {
            final String messageStr = "Message_" + messageNo;
            final long startTime = System.currentTimeMillis();
            if (isAsync) {
                // 发送消息，通过回调获取消息发送的结果
                producer.send(new ProducerRecord<>(topic, messageNo, messageStr), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (metadata != null) {
                            // 消息发送正常
                            System.out.println("message(" + messageStr + ") sent to partition(" + metadata.partition() + "), " +
                                    "offset(" + metadata.offset() + ") in " + (System.currentTimeMillis() - startTime) + " ms");
                        } else {
                            // 消息发送异常
                            e.printStackTrace();
                        }
                    }
                });
            } else {
                // 发送消息，基于 future 的方式获取消息发送结果
                try {
                    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, messageNo, messageStr));
                    RecordMetadata metadata = future.get();
                    System.out.println("message(" + messageStr + ") sent to partition(" + metadata.partition() + "), " +
                            "offset(" + metadata.offset() + ") in " + (System.currentTimeMillis() - startTime) + " ms");
                } catch (InterruptedException | ExecutionException e) {
                    // 消息发送异常
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}
