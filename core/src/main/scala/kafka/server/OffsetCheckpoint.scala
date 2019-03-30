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

import java.io._
import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection._

object OffsetCheckpoint {
    private val WhiteSpacesPattern = Pattern.compile("\\s+")
    private val CurrentVersion = 0
}

/**
 * This class saves out a map of topic/partition=>offsets to a file
 *
 * 管理 replication-offset-checkpoint 文件，该文件记录了 log 目录下每个 TP 的 HW offset，该文件的内容示例：
 *
 * 0
 * 8
 * topic-default 3 115
 * topic-default 2 118
 * topic-default 4 145
 * topic-default 0 122
 * topic-default 5 122
 * topic-default 1 117
 * topic-default 7 140
 * topic-default 6 121
 */
class OffsetCheckpoint(val file: File) extends Logging {

    import OffsetCheckpoint._

    private val path = file.toPath.toAbsolutePath
    private val tempPath = Paths.get(path.toString + ".tmp")
    private val lock = new Object()
    file.createNewFile() // in case the file doesn't exist

    /**
     * 写 replication-offset-checkpoint 文件
     *
     * @param offsets
     */
    def write(offsets: Map[TopicPartition, Long]) {
        lock synchronized {
            // 创建一个临时文件
            val fileOutputStream = new FileOutputStream(tempPath.toFile)
            val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
            try {
                // 写入版本号
                writer.write(CurrentVersion.toString)
                writer.newLine()

                // 写入记录条数（topic 数目 × 每个 topic 的分区数）
                writer.write(offsets.size.toString)
                writer.newLine()

                // 循环写入数据，格式为 "topic 分区编号 HW"
                offsets.foreach { case (topicPart, offset) =>
                    writer.write(s"${topicPart.topic} ${topicPart.partition} $offset")
                    writer.newLine()
                }

                // 落盘
                writer.flush()
                fileOutputStream.getFD.sync()
            } catch {
                case e: FileNotFoundException =>
                    if (FileSystems.getDefault.isReadOnly) {
                        fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
                        Runtime.getRuntime.halt(1)
                    }
                    throw e
            } finally {
                writer.close()
            }

            // 使用临时文件替换之前的 recovery-point-offset-checkpoint 文件
            Utils.atomicMoveWithFallback(tempPath, path)
        }
    }

    /**
     * 读取 replication-offset-checkpoint 文件
     *
     * @return
     */
    def read(): Map[TopicPartition, Long] = {

        def malformedLineException(line: String) = new IOException(s"Malformed line in offset checkpoint file: $line'")

        lock synchronized {
            val reader = new BufferedReader(new FileReader(file))
            var line: String = null
            try {
                line = reader.readLine()
                if (line == null)
                    return Map.empty
                // 读取版本号
                val version = line.toInt
                version match {
                    case CurrentVersion =>
                        line = reader.readLine()
                        if (line == null)
                            return Map.empty
                        val expectedSize = line.toInt
                        val offsets = mutable.Map[TopicPartition, Long]()
                        line = reader.readLine()
                        while (line != null) {
                            WhiteSpacesPattern.split(line) match {
                                case Array(topic, partition, offset) =>
                                    offsets += new TopicPartition(topic, partition.toInt) -> offset.toLong
                                    line = reader.readLine()
                                // 格式错误
                                case _ => throw malformedLineException(line)
                            }
                        }
                        if (offsets.size != expectedSize)
                            throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
                        offsets
                    // 目前只有 0 的版本号
                    case _ =>
                        throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
                }
            } catch {
                case _: NumberFormatException => throw malformedLineException(line)
            } finally {
                reader.close()
            }
        }
    }

}
