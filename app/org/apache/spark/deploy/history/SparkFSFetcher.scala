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


import java.net.{HttpURLConnection, URL, URI}
import java.security.PrivilegedAction
import java.io.{IOException, BufferedInputStream, InputStream}
import java.{io, util}
import java.util.ArrayList
import javax.ws.rs.core.UriBuilder
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.security.HadoopSecurity
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.{MemoryFormatUtils, Utils}
import com.linkedin.drelephant.analysis.{ApplicationType, AnalyticJob, ElephantFetcher}
import org.apache.commons.io.FileUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.apache.hadoop.security.authentication.client.{AuthenticatedURL, AuthenticationException}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus, ApplicationEventListener}
import org.apache.spark.storage.{StorageStatusTrackingListener, StorageStatusListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener
import org.apache.spark.io.CompressionCodec
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable.ArrayBuffer


/**
 * A wrapper that replays Spark event history from files and then fill proper data objects.
 */
class SparkFSFetcher(fetcherConfData: FetcherConfigurationData) extends ElephantFetcher[SparkApplicationData] {

  import SparkFSFetcher._

  val NAME_SERVICES = "dfs.nameservices";
  val DFS_HA_NAMENODES = "dfs.ha.namenodes";
  val DFS_NAMENODE_HTTP_ADDRESS = "dfs.namenode.http-address";

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
    val nodeAddress = getNamenodeAddress(conf);
    val hdfsAddress = if (nodeAddress == null) "" else "webhdfs://" + nodeAddress

    val uri = new URI(_sparkConf.get("spark.eventLog.dir", confEventLogDir))
    val logDir = hdfsAddress + uri.getPath
    logger.info("Looking for spark logs at logDir: " + logDir)
    logDir
  }

  /**
   * Returns the namenode address of the  active nameNode
   * @param conf The Hadoop configuration
   * @return The namenode address of the active namenode
   */
  def getNamenodeAddress(conf: Configuration): String = {

    // check if the fetcherconf has namenode addresses. There can be multiple addresses and
    // we need to check the active namenode address. If a value is specified in the fetcherconf
    // then the value obtained from hadoop configuration won't be used.
    if (fetcherConfData.getParamMap.get(NAMENODE_ADDRESSES) != null) {
      val nameNodes: Array[String] = fetcherConfData.getParamMap.get(NAMENODE_ADDRESSES).split(",");
      for (nameNode <- nameNodes) {
        if (checkActivation(nameNode)) {
          return nameNode;
        }
      }
    }

    // if we couldn't find the namenode address in fetcherconf, try to find it in hadoop configuration.
    var isHAEnabled: Boolean = false;
    if (conf.get(NAME_SERVICES) != null) {
      isHAEnabled = true;
    }

    // check if HA is enabled
    if (isHAEnabled) {
      // There can be multiple nameservices separated by ',' in case of HDFS federation. It is not supported right now.
      if (conf.get(NAME_SERVICES).split(",").length > 1) {
        logger.info("Multiple name services found. HDFS federation is not supported right now.")
        return null;
      }
      val nameService: String = conf.get(NAME_SERVICES);
      val nameNodeIdentifier: String = conf.get(DFS_HA_NAMENODES + "." + nameService);
      if (nameNodeIdentifier != null) {
        // there can be multiple namenode identifiers separated by ','
        for (nameNodeIdentifierValue <- nameNodeIdentifier.split(",")) {
          val httpValue = conf.get(DFS_NAMENODE_HTTP_ADDRESS + "." + nameService + "." + nameNodeIdentifierValue);
          if (httpValue != null && checkActivation(httpValue)) {
            logger.info("Active namenode : " + httpValue);
            return httpValue;
          }
        }
      }
    }

    // if HA is disabled, return the namenode http-address.
    return conf.get(DFS_NAMENODE_HTTP_ADDRESS);
  }

  /**
   * Checks if the namenode specified is active or not
   * @param httpValue The namenode configuration http value
   * @return True if the namenode is active, otherwise false
   */
  def checkActivation(httpValue: String): Boolean = {
    val url: URL = new URL("http://" + httpValue + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus");
    val rootNode: JsonNode = readJsonNode(url);
    val status: String = rootNode.path("beans").get(0).path("State").getValueAsText();
    if (status.equals("active")) {
      return true;
    }
    return false;
  }

  /**
   * Returns the jsonNode which is read from the url
   * @param url The url of the server
   * @return The jsonNode parsed from the url
   */
  def readJsonNode(url: URL): JsonNode = {
    val _token: AuthenticatedURL.Token = new AuthenticatedURL.Token();
    val _authenticatedURL: AuthenticatedURL = new AuthenticatedURL();
    val _objectMapper: ObjectMapper = new ObjectMapper();
    val conn: HttpURLConnection = _authenticatedURL.openConnection(url, _token)
    return _objectMapper.readTree(conn.getInputStream)
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
      override def run(): SparkDataCollection = doFetchData(appId)
    })
  }

  private def doFetchData(appId: String): SparkDataCollection = {
    val dataCollection = new SparkDataCollection()

    val (logPath, usingDefaultEventLog) =
      Option(defaultEventLogPathForApp(appId))
        .filter(fs.exists)
        .map((_, true))
        .getOrElse((legacyEventLogPathForApp(appId), false))

    if (shouldThrottle(logPath)) {
      dataCollection.throttle()
      // Since the data set is empty, we need to set the application id,
      // so that we could detect this is Spark job type
      dataCollection.getGeneralData().setApplicationId(appId)
      dataCollection.getConf().setProperty("spark.app.id", appId)

      logger.info("The event log of Spark application: " + appId + " is over the limit size of "
        + confEventLogSizeInMb + " MB, the parsing process gets throttled.")
    } else {
      logger.info("Replaying Spark logs for application: " + appId)

      val in = if (usingDefaultEventLog) openDefaultEventLog(logPath) else openLegacyEventLog(logPath)
      dataCollection.load(in, logPath.toString())
      in.close()

      logger.info("Replay completed for application: " + appId)
    }

    dataCollection
  }

  private def defaultEventLogPathForApp(appId: String): Path =
    new Path(_logDir, appId + fetcherConfData.getParamMap.getOrDefault(SPARK_LOG_SUFFIX_KEY, DEFAULT_SPARK_LOG_SUFFIX))

  private def legacyEventLogPathForApp(appId: String): Path = new Path(_logDir, appId)

  private def openDefaultEventLog(path: Path): InputStream = EventLoggingListener.openEventLog(path, fs)

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
  val DEFAULT_SPARK_LOG_SUFFIX = "_1.snappy"

  val LOG_SIZE_XML_FIELD = "event_log_size_limit_in_mb"
  val LOG_DIR_XML_FIELD = "event_log_dir"

  // Constants used to parse <= Spark 1.2.0 log directories.
  val LOG_PREFIX = "EVENT_LOG_"
  val COMPRESSION_CODEC_PREFIX = EventLoggingListener.COMPRESSION_CODEC_KEY + "_"

  // Param map property names that allow users to configer various aspects of the fetcher
  val NAMENODE_ADDRESSES = "namenode_addresses"
  val SPARK_LOG_SUFFIX_KEY = "spark_log_ext"
}
