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

package kafka.log

import java.io._
import java.util.concurrent._

import kafka.common.{KafkaException, KafkaStorageException}
import kafka.server.{BrokerState, OffsetCheckpoint, RecoveringFromUncleanShutdown}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 *
 * 提供了加载 Log、创建、删除、查询 Log 集合等功能
 */
@threadsafe
class LogManager(val logDirs: Array[File], // log 目录集合，对应 log.dirs 配置，一般选择 log 数目最少的目录进行创建
                 val topicConfigs: Map[String, LogConfig], // topic 相关配置
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig, // log cleaner 相关配置
                 ioThreads: Int, // 每个 log 目录下分配的执行加载任务的线程数目
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler, // 定时任务调度器
                 val brokerState: BrokerState, // 当前 broker 节点的状态
                 time: Time) extends Logging {

    /**
     * 每个 log 目录下面都有一个 recovery-point-offset-checkpoint 文件，
     * 记录了当前 log 目录每个 Log 的 recoveryPoint 值，用于在 broker 启动时恢复 Log，示例：
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
    val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
    val LockFile = ".lock"
    val InitialTaskDelayMs: Int = 30 * 1000

    /** 创建或删除 Log 时的锁对象 */
    private val logCreationOrDeletionLock = new Object

    /** 记录每个 TP 与 Log 对象之间的映射关系 */
    private val logs = new Pool[TopicPartition, Log]()

    /** 记录需要被删除的 log */
    private val logsToBeDeleted = new LinkedBlockingQueue[Log]()

    /**
     * 确保 log.dirs 配置中没有重复的路径，且配置中的路径都是目录且可读，如果不存在则会创建
     */
    this.createAndValidateLogDirs(logDirs)

    /** 尝试对每个 log 目录在文件系统层面加锁，这里加的是进程锁 */
    private val dirLocks = this.lockLogDirs(logDirs)

    /**
     * 遍历为每个 log 目录创建一个操作其名下 recovery-point-offset-checkpoint 文件的 OffsetCheckpoint 对象，
     * 并建立映射关系
     */
    private val recoveryPointCheckpoints = logDirs.map(
        // recovery-point-offset-checkpoint 文件
        dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap

    /**
     * 加载所有 log 路径下的 Log
     */
    this.loadLogs()

    /** 用于清理过期或者过大的日志 */
    val cleaner: LogCleaner = if (cleanerConfig.enableCleaner)
                                  new LogCleaner(cleanerConfig, logDirs, logs, time = time) else null

    /**
     * 遍历配置的 log.dirs，如果不存在则创建，需要保证以下几点：
     * 1. log.dirs 不能包含重复的路径配置
     * 2. 对应的路径必须是目录，且是可读的
     *
     * @param dirs
     */
    private def createAndValidateLogDirs(dirs: Seq[File]) {
        // 存在重复的 log 目录配置
        if (dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
            throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))

        // 遍历处理每个 log 目录
        for (dir <- dirs) {
            // 如果目录不存在则创建
            if (!dir.exists) {
                info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
                val created = dir.mkdirs()
                if (!created)
                    throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
            }
            // 校验路径是不是目录，是不是可读
            if (!dir.isDirectory || !dir.canRead)
                throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
        }
    }

    /**
     * 尝试对每个 log 目录在文件系统层面加锁，这里加的是进程锁
     *
     * @param dirs
     * @return
     */
    private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
        dirs.map { dir =>
            val lock = new FileLock(new File(dir, LockFile))
            if (!lock.tryLock())
                throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath
                        + ". A Kafka instance in another process or thread is using this directory.")
            lock
        }
    }

    /**
     * 遍历处理 log.dirs 配置的 log 目录，多线程并发加载每个 log 目录下面 TP 对应的子目录：
     * - 使用 Log 对象封装每个 TP 目录对应的数据，记录到 LogManager#logs 字段中；
     * - 对于需要被删除的 TP 目录，添加到 LogManager#logsToBeDeleted 字段中，等待后续周期性任务进行删除。
     */
    private def loadLogs(): Unit = {
        info("Loading logs.")
        val startMs = time.milliseconds
        // 用于记录所有 log 目录对应的线程池
        val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
        val jobs = mutable.Map.empty[File, Seq[Future[_]]]

        // 遍历处理每个 log 目录
        for (dir <- this.logDirs) {
            // 为每个 log 目录创建一个 ioThreads 大小的线程池
            val pool = Executors.newFixedThreadPool(ioThreads)
            threadPools.append(pool)

            // 尝试获取 .kafka_cleanshutdown 文件，如果该文件存在则说明 broker 节点是正常关闭的
            val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
            if (cleanShutdownFile.exists) {
                debug("Found clean shutdown file. Skipping recovery for all logs in data directory: " + dir.getAbsolutePath)
            } else {
                // 当前 broker 不是正常关闭，设置 broker 状态为 RecoveringFromUncleanShutdown，表示正在从上次异常关闭中恢复
                brokerState.newState(RecoveringFromUncleanShutdown)
            }

            // 读取每个 log 目录下的 recovery-point-offset-checkpoint 文件，返回 TP 与 HW offset 之间的映射关系
            var recoveryPoints = Map[TopicPartition, Long]()
            try {
                recoveryPoints = this.recoveryPointCheckpoints(dir).read()
            } catch {
                case e: Exception =>
                    warn("Error occured while reading recovery-point-offset-checkpoint file of directory " + dir, e)
                    warn("Resetting the recovery checkpoint to 0")
            }

            // 遍历当前 log 目录的子目录，仅处理目录，忽略文件
            val jobsForDir = for {
                dirContent <- Option(dir.listFiles).toList
                logDir <- dirContent if logDir.isDirectory
            } yield {
                // 为每个 Log 目录创建一个 Runnable 任务
                CoreUtils.runnable {
                    debug("Loading log '" + logDir.getName + "'")

                    // 依据目录名解析得到对应的 topic 分区对象
                    val topicPartition = Log.parseTopicPartitionName(logDir)
                    // 获取当前 topic 分区对应的配置
                    val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
                    // 获取 topic 分区对应的 HW offset
                    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

                    // 创建对应的 Log 对象，每个 topic 分区目录对应一个 Log 对象
                    val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
                    // 如果当前 log 是需要被删除的文件，则记录到 logsToBeDeleted 队列中，会有周期性任务对其执行删除操作
                    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) { // -delete
                        logsToBeDeleted.add(current)
                    } else {
                        // 建立 topic 分区对象与其 Log 对象之间的映射关系，不允许一个 topic 对象对应多个目录
                        val previous = logs.put(topicPartition, current)
                        if (previous != null) {
                            throw new IllegalArgumentException(
                                "Duplicate log directories found: %s, %s!".format(current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
                        }
                    }
                }
            }

            // 提交上面创建的任务，并将提交结果封装到 jobs 集合中，jobsForDir 是 List[Runnable] 类型
            jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
        }

        // 阻塞等待上面提交的任务执行完成，即等待所有 log 目录下 topic 分区对应的目录文件加载完成
        try {
            for ((cleanShutdownFile, dirJobs) <- jobs) {
                dirJobs.foreach(_.get)
                // 删除对应的 .kafka_cleanshutdown 文件
                cleanShutdownFile.delete()
            }
        } catch {
            case e: ExecutionException =>
                error("There was an error in one of the threads during logs loading: " + e.getCause)
                throw e.getCause
        } finally {
            // 遍历关闭线程池
            threadPools.foreach(_.shutdown())
        }

        info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
    }

    /**
     * 启动 LogManager 中的周期性定时任务和 LogCleaner 线程
     */
    def startup() {
        if (scheduler != null) {
            // 1. 启动 kafka-log-retention 周期性任务，对过期或过大的日志文件执行清理工作
            info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
            scheduler.schedule("kafka-log-retention",
                this.cleanupLogs,
                delay = InitialTaskDelayMs,
                period = retentionCheckMs,
                TimeUnit.MILLISECONDS)

            // 2. 启动 kafka-log-flusher 周期性任务，对日志文件执行刷盘操作
            info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
            scheduler.schedule("kafka-log-flusher",
                this.flushDirtyLogs,
                delay = InitialTaskDelayMs,
                period = flushCheckMs,
                TimeUnit.MILLISECONDS)

            // 3. 启动 kafka-recovery-point-checkpoint 周期性任务，更新 recovery-point-offset-checkpoint 文件
            scheduler.schedule("kafka-recovery-point-checkpoint",
                this.checkpointRecoveryPointOffsets,
                delay = InitialTaskDelayMs,
                period = flushCheckpointMs,
                TimeUnit.MILLISECONDS)

            // 4. 启动 kafka-delete-logs 周期性任务，删除标记为需要被删除的 log 目录
            scheduler.schedule("kafka-delete-logs",
                this.deleteLogs,
                delay = InitialTaskDelayMs,
                period = defaultConfig.fileDeleteDelayMs,
                TimeUnit.MILLISECONDS)
        }

        // 启动 LogCleaner 线程
        if (cleanerConfig.enableCleaner) cleaner.startup()
    }

    /**
     * Close all the logs
     */
    def shutdown() {
        info("Shutting down.")

        val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
        val jobs = mutable.Map.empty[File, Seq[Future[_]]]

        // stop the cleaner first
        if (cleaner != null) {
            CoreUtils.swallow(cleaner.shutdown())
        }

        // close logs in each dir
        for (dir <- this.logDirs) {
            debug("Flushing and closing logs at " + dir)

            val pool = Executors.newFixedThreadPool(ioThreads)
            threadPools.append(pool)

            val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

            val jobsForDir = logsInDir map { log =>
                CoreUtils.runnable {
                    // flush the log to ensure latest possible recovery point
                    log.flush()
                    log.close()
                }
            }

            jobs(dir) = jobsForDir.map(pool.submit).toSeq
        }

        try {
            for ((dir, dirJobs) <- jobs) {
                dirJobs.foreach(_.get)

                // update the last flush point
                debug("Updating recovery points at " + dir)
                checkpointLogsInDir(dir)

                // mark that the shutdown was clean by creating marker file
                debug("Writing clean shutdown marker at " + dir)
                CoreUtils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
            }
        } catch {
            case e: ExecutionException => {
                error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
                throw e.getCause
            }
        } finally {
            threadPools.foreach(_.shutdown())
            // regardless of whether the close succeeded, we need to unlock the data directories
            dirLocks.foreach(_.destroy())
        }

        info("Shutdown complete.")
    }

    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionOffsets Partition logs that need to be truncated
     */
    def truncateTo(partitionOffsets: Map[TopicPartition, Long]) {
        for ((topicPartition, truncateOffset) <- partitionOffsets) {
            val log = logs.get(topicPartition)
            // If the log does not exist, skip it
            if (log != null) {
                //May need to abort and pause the cleaning of the log, and resume after truncation is done.
                val needToStopCleaner: Boolean = truncateOffset < log.activeSegment.baseOffset
                if (needToStopCleaner && cleaner != null)
                    cleaner.abortAndPauseCleaning(topicPartition)
                log.truncateTo(truncateOffset)
                if (needToStopCleaner && cleaner != null) {
                    cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
                    cleaner.resumeCleaning(topicPartition)
                }
            }
        }
        checkpointRecoveryPointOffsets()
    }

    /**
     * Delete all data in a partition and start the log at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long) {
        val log = logs.get(topicPartition)
        // If the log does not exist, skip it
        if (log != null) {
            //Abort and pause the cleaning of the log, and resume after truncation is done.
            if (cleaner != null)
                cleaner.abortAndPauseCleaning(topicPartition)
            log.truncateFullyAndStartAt(newOffset)
            if (cleaner != null) {
                cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
                cleaner.resumeCleaning(topicPartition)
            }
        }
        checkpointRecoveryPointOffsets()
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     *
     * 定时将每个 Log 的 recoveryPoint 写入 recovery-point-offset-checkpoint 文件
     */
    def checkpointRecoveryPointOffsets() {
        // 为每个 log 目录应用 checkpointLogsInDir 方法
        logDirs.foreach(checkpointLogsInDir)
    }

    /**
     * Make a checkpoint for all logs in provided directory.
     */
    private def checkpointLogsInDir(dir: File): Unit = {
        // 获取指定 log 目录对应的 Map[TopicPartition, Log] 集合
        val recoveryPoints = logsByDir.get(dir.toString)
        if (recoveryPoints.isDefined) {
            // 更新对应的 recovery-point-offset-checkpoint 文件
            this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
        }
    }

    /**
     * Get the log if it exists, otherwise return None
     */
    def getLog(topicPartition: TopicPartition): Option[Log] = Option(logs.get(topicPartition))

    /**
     * Create a log for the given topic and the given partition
     * If the log already exists, just return a copy of the existing log
     *
     * 选择或创建 log 对应的目录，优先选择 Log 数目最少的目录
     */
    def createLog(topicPartition: TopicPartition, config: LogConfig): Log = {
        logCreationOrDeletionLock synchronized {
            // create the log if it has not already been created in another thread
            getLog(topicPartition).getOrElse {
                val dataDir = nextLogDir()
                val dir = new File(dataDir, topicPartition.topic + "-" + topicPartition.partition)
                dir.mkdirs()
                val log = new Log(dir, config, recoveryPoint = 0L, scheduler, time)
                logs.put(topicPartition, log)
                info("Created log for partition [%s,%d] in %s with properties {%s}."
                        .format(topicPartition.topic,
                            topicPartition.partition,
                            dataDir.getAbsolutePath,
                            config.originals.asScala.mkString(", ")))
                log
            }
        }
    }

    /**
     * Delete logs marked for deletion.
     *
     * 定时删除标记为删除的日志文件
     */
    private def deleteLogs(): Unit = {
        try {
            var failed = 0
            // 如果存在需要删除的目录
            while (!logsToBeDeleted.isEmpty && failed < logsToBeDeleted.size()) {
                // 获取需要删除的目录对应的 Log 对象
                val removedLog = logsToBeDeleted.take()
                if (removedLog != null) {
                    try {
                        // 调用 Log.delete 方法执行删除操作
                        removedLog.delete()
                        info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
                    } catch {
                        case e: Throwable =>
                            error(s"Exception in deleting $removedLog. Moving it to the end of the queue.", e)
                            failed = failed + 1
                            // 如果删除异常，则归还，下一次周期性调用时再删除
                            logsToBeDeleted.put(removedLog)
                    }
                }
            }
        } catch {
            case e: Throwable =>
                error(s"Exception in kafka-delete-logs thread.", e)
        }
    }

    /**
     * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
     * add it in the queue for deletion.
     *
     * @param topicPartition TopicPartition that needs to be deleted
     */
    def asyncDelete(topicPartition: TopicPartition): Unit = {
        val removedLog: Log = logCreationOrDeletionLock synchronized {
            logs.remove(topicPartition)
        }
        if (removedLog != null) {
            //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
            if (cleaner != null) {
                cleaner.abortCleaning(topicPartition)
                cleaner.updateCheckpoints(removedLog.dir.getParentFile)
            }
            val dirName = Log.logDeleteDirName(removedLog.name)
            removedLog.close()
            val renamedDir = new File(removedLog.dir.getParent, dirName)
            val renameSuccessful = removedLog.dir.renameTo(renamedDir)
            if (renameSuccessful) {
                removedLog.dir = renamedDir
                // change the file pointers for log and index file
                for (logSegment <- removedLog.logSegments) {
                    logSegment.log.setFile(new File(renamedDir, logSegment.log.file.getName))
                    logSegment.index.file = new File(renamedDir, logSegment.index.file.getName)
                }

                logsToBeDeleted.add(removedLog)
                removedLog.removeLogMetrics()
                info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
            } else {
                throw new KafkaStorageException("Failed to rename log directory from " + removedLog.dir.getAbsolutePath + " to " + renamedDir.getAbsolutePath)
            }
        }
    }

    /**
     * Choose the next directory in which to create a log. Currently this is done
     * by calculating the number of partitions in each directory and then choosing the
     * data directory with the fewest partitions.
     */
    private def nextLogDir(): File = {
        if (logDirs.length == 1) {
            logDirs(0)
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories
            val logCounts = allLogs().groupBy(_.dir.getParent).mapValues(_.size)
            val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
            val dirCounts = (zeros ++ logCounts).toBuffer

            // choose the directory with the least logs in it
            val leastLoaded = dirCounts.minBy(_._2)
            new File(leastLoaded._1)
        }
    }

    /**
     * Delete any eligible（合格的） logs. Only consider logs that are not compacted.
     *
     * 按照以下条件执行 log 清理工作：
     * 1. LogSegment 的存活时长
     * 2. 整个 Log 的大小
     */
    def cleanupLogs() {
        debug("Beginning log cleanup...")
        var total = 0
        val startMs = time.milliseconds
        // 遍历处理每个 topic 分区对应的 Log 对象，只有对应 Log 配置了 cleanup.policy=delete 才会执行删除
        for (log <- allLogs(); if !log.config.compact) {
            debug("Garbage collecting '" + log.name + "'")
            // 遍历删除当前 Log 对象中过期的 LogSegment，并保证 Log 的大小在允许范围内（对应 retention.bytes 配置）
            total += log.deleteOldSegments()
        }
        debug("Log cleanup completed. " + total + " files deleted in " + (time.milliseconds - startMs) / 1000 + " seconds")
    }

    /**
     * Get all the partition logs
     */
    def allLogs(): Iterable[Log] = logs.values

    /**
     * Get a map of TopicPartition => Log
     */
    def logsByTopicPartition: Map[TopicPartition, Log] = logs.toMap

    /**
     * Map of log dir to logs by topic and partitions in that dir
     */
    private def logsByDir: Predef.Map[String, Map[TopicPartition, Log]] = {
        this.logsByTopicPartition.groupBy {
            case (_, log) => log.dir.getParent
        }
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     *
     * 依据配置定时对 Log 执行 flush 操作
     */
    private def flushDirtyLogs(): Unit = {
        debug("Checking for dirty logs to flush...")

        // 遍历处理每个 topic 分区对应的 Log 对象
        for ((topicPartition, log) <- logs) {
            try {
                val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
                debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
                        " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
                // 如果距离上次 flush 的时间超过 flush.ms 值，则执行 flush 操作
                if (timeSinceLastFlush >= log.config.flushMs)
                    log.flush()
            } catch {
                case e: Throwable =>
                    error("Error flushing topic " + topicPartition.topic, e)
            }
        }
    }
}
