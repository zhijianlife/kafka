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

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

object Kafka extends Logging {

    def getPropsFromArgs(args: Array[String]): Properties = {
        val optionParser = new OptionParser
        val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
                .withRequiredArg()
                .ofType(classOf[String])

        if (args.length == 0) {
            CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
        }

        val props = Utils.loadProps(args(0))

        if (args.length > 1) {
            val options = optionParser.parse(args.slice(1, args.length): _*)

            if (options.nonOptionArguments().size() > 0) {
                CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
            }

            props.putAll(CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala))
        }
        props
    }

    /**
     * Kafka server 程序的启动方法
     *
     * ./kafka-server-start.sh [-daemon] server.properties [--override property=value]*
     *
     * @param args
     */
    def main(args: Array[String]): Unit = {
        try {
            // 解析命令行参数
            val serverProps = getPropsFromArgs(args)
            // 创建 kafkaServerStartable 对象，期间会初始化监控上报程序
            val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

            // 注册一个钩子方法，当 JVM 被关闭时执行 shutdown 逻辑，本质上是在执行 KafkaServer#shutdown 方法
            Runtime.getRuntime.addShutdownHook(new Thread() {
                override def run(): Unit = {
                    kafkaServerStartable.shutdown()
                }
            })

            // 本质上调用的是 KafkaServer#startup 方法
            kafkaServerStartable.startup()
            // 阻塞等待 kafka server 运行线程关闭
            kafkaServerStartable.awaitShutdown()
        } catch {
            case e: Throwable =>
                fatal(e)
                System.exit(1)
        }
        System.exit(0)
    }
}
