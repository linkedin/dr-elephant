/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.deploy.history


import java.net.URI
import java.security.PrivilegedAction
import java.io.BufferedInputStream
import java.io.InputStream
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.security.HadoopSecurity
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.{MemoryFormatUtils, Utils}
import com.linkedin.drelephant.analysis.{ApplicationType, AnalyticJob, ElephantFetcher}
import org.apache.commons.io.FileUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus, ApplicationEventListener}
import org.apache.spark.storage.{StorageStatusTrackingListener, StorageStatusListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener
import org.apache.spark.io.CompressionCodec


/**
 * A wrapper that replays Spark event history from files and then fill proper data objects.
 */
class SparkFSFetcher(fetcherConfData: FetcherConfigurationData) extends ElephantFetcher[SparkApplicationData] {

  import SparkFSFetcher._

  var confEventLogSizeInMb = defEventLogSizeInMb
  if (fetcherConfData.getParamMap.get(LOG_SIZE_XML_FIELD) != null) {
    val logLimitSize = Utils.getParam(fetcherConfData.getParamMap.get(LOG_SIZE_XML_FIELD), 1)
    if (logLimitSize != null) {
      confEventLogSizeInMb = logLimitSize(0)
    }
  }
  logger.info("The event log limit of Spark application is set to " + confEventLogSizeInMb + " MB")

  var confEventLogDir = fetcherConfData.getParamMap.get(LOG_DIR_XML_FIELD)
  if (confEventLogDir == null || confEventLogDir.isEmpty) {
    confEventLogDir = defEventLogDir
  }
  logger.info("The event log directory of Spark application is set to " + confEventLogDir)

  private val _sparkConf = new SparkConf()

  /* Lazy loading for the log directory is very important. Hadoop Configuration() takes time to load itself to reflect
   * properties in the configuration files. Triggering it too early will sometimes make the configuration object empty.
   */
  private lazy val _logDir: String = {
    val conf = new Configuration()
    val nodeAddress = conf.get("dfs.namenode.http-address", null)
    val hdfsAddress = if (nodeAddress == null) "" else "webhdfs://" + nodeAddress

    val uri = new URI(_sparkConf.get("spark.eventLog.dir", confEventLogDir))
    val logDir = hdfsAddress + uri.getPath
    logger.info("Looking for spark logs at logDir: " + logDir)
    logDir
  }

  private val _security = new HadoopSecurity()

  private def fs: FileSystem = {

    // For test purpose, if no host presented, use the local file system.
    if (new URI(_logDir).getHost == null) {
      FileSystem.getLocal(new Configuration())
    } else {
      val filesystem = new WebHdfsFileSystem()
      filesystem.initialize(new URI(_logDir), new Configuration())
      filesystem
    }
  }

  def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    val appId = analyticJob.getAppId()
    _security.doAs[SparkDataCollection](new PrivilegedAction[SparkDataCollection] {
      override def run(): SparkDataCollection = {
        /* Most of Spark logs will be in directory structure: /LOG_DIR/[application_id].
         *
         * Some logs (Spark 1.3+) are in /LOG_DIR/[application_id].snappy
         *
         * Currently we won't be able to parse them even we manually set up the codec. There is problem
         * in JsonProtocol#sparkEventFromJson that it does not handle unmatched SparkListenerEvent, which means
         * it is only backward compatible but not forward. And switching the dependency to Spark 1.3 will raise more
         * problems due to the fact that we are touching the internal codes.
         *
         * In short, this fetcher only works with Spark <=1.2, and we should switch to JSON endpoints with Spark's
         * future release.
         */
        val replayBus = new ReplayListenerBus()
        val applicationEventListener = new ApplicationEventListener
        val jobProgressListener = new JobProgressListener(new SparkConf())
        val environmentListener = new EnvironmentListener
        val storageStatusListener = new StorageStatusListener
        val executorsListener = new ExecutorsListener(storageStatusListener)
        val storageListener = new StorageListener(storageStatusListener)

        // This is a customized listener that tracks peak used memory
        // The original listener only tracks the current in use memory which is useless in offline scenario.
        val storageStatusTrackingListener = new StorageStatusTrackingListener()
        replayBus.addListener(storageStatusTrackingListener)

        val dataCollection = new SparkDataCollection(applicationEventListener = applicationEventListener,
          jobProgressListener = jobProgressListener,
          environmentListener = environmentListener,
          storageStatusListener = storageStatusListener,
          executorsListener = executorsListener,
          storageListener = storageListener,
          storageStatusTrackingListener = storageStatusTrackingListener)

        replayBus.addListener(applicationEventListener)
        replayBus.addListener(jobProgressListener)
        replayBus.addListener(environmentListener)
        replayBus.addListener(storageStatusListener)
        replayBus.addListener(executorsListener)
        replayBus.addListener(storageListener)

        val logPath = new Path(_logDir, appId)
        val logInput: InputStream =
          if (isLegacyLogDirectory(logPath)) {
            if (!shouldThrottle(logPath)) {
              openLegacyEventLog(logPath)
            } else {
              null
            }
          } else {
            val logFilePath = new Path(logPath + "_1.snappy")
            if (!shouldThrottle(logFilePath)) {
              EventLoggingListener.openEventLog(logFilePath, fs)
            } else {
              null
            }
          }

        if (logInput == null) {
          dataCollection.throttle()
          // Since the data set is empty, we need to set the application id,
          // so that we could detect this is Spark job type
          dataCollection.getGeneralData().setApplicationId(appId)
          dataCollection.getConf().setProperty("spark.app.id", appId)

          logger.info("The event log of Spark application: " + appId + " is over the limit size of "
              + defEventLogSizeInMb + " MB, the parsing process gets throttled.")
        } else {
          logger.info("Replaying Spark logs for application: " + appId)

          replayBus.replay(logInput, logPath.toString(), false)

          logger.info("Replay completed for application: " + appId)
        }

        dataCollection
      }
    })
  }

  /**
   * Checks if the log path stores the legacy event log. (Spark <= 1.2 store an event log in a directory)
   *
   * @param entry The path to check
   * @return true if it is legacy log path, else false
   */
  private def isLegacyLogDirectory(entry: Path) : Boolean = fs.exists(entry) && fs.getFileStatus(entry).isDirectory()

  /**
   * Opens a legacy log path
   *
   * @param dir The directory to open
   * @return an InputStream
   */
  private def openLegacyEventLog(dir: Path): InputStream = {
    val children = fs.listStatus(dir)
    var eventLogPath: Path = null
    var codecName: Option[String] = None

    children.foreach { child =>
      child.getPath().getName() match {
        case name if name.startsWith(LOG_PREFIX) =>
          eventLogPath = child.getPath()
        case codec if codec.startsWith(COMPRESSION_CODEC_PREFIX) =>
          codecName = Some(codec.substring(COMPRESSION_CODEC_PREFIX.length()))
        case _ =>
      }
    }

    if (eventLogPath == null) {
      throw new IllegalArgumentException(s"$dir is not a Spark application log directory.")
    }

    val codec = try {
      codecName.map { c => CompressionCodec.createCodec(_sparkConf, c) }
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Unknown compression codec $codecName.")
    }

    val in = new BufferedInputStream(fs.open(eventLogPath))
    codec.map(_.compressedInputStream(in)).getOrElse(in)
  }

  /**
   * Checks if the log parser should be throttled when the file is too large.
   * Note: the current Spark's implementation of ReplayListenerBus will take more than 80 minutes to read a compressed
   * 500 MB event log file. Allowing such reading might block the entire Dr Elephant thread pool.
   *
   * @param eventLogPath The event log path
   * @return If the event log parsing should be throttled
   */
  private def shouldThrottle(eventLogPath: Path): Boolean = {
    fs.getFileStatus(eventLogPath).getLen() > (confEventLogSizeInMb * FileUtils.ONE_MB)
  }

  def getEventLogSize(): Double = {
    confEventLogSizeInMb
  }

  def getEventLogDir(): String = {
    confEventLogDir
  }

}

private object SparkFSFetcher {
  private val logger = Logger.getLogger(SparkFSFetcher.getClass)

  var defEventLogDir = "/system/spark-history"
  var defEventLogSizeInMb = 100d; // 100MB

  val LOG_SIZE_XML_FIELD = "event_log_size_limit_in_mb"
  val LOG_DIR_XML_FIELD = "event_log_dir"

  // Constants used to parse <= Spark 1.2.0 log directories.
  val LOG_PREFIX = "EVENT_LOG_"
  val COMPRESSION_CODEC_PREFIX = EventLoggingListener.COMPRESSION_CODEC_KEY + "_"
}
