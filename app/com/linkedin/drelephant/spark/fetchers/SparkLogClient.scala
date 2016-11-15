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

package com.linkedin.drelephant.spark.fetchers

import java.io.{BufferedInputStream, FileNotFoundException, InputStream}
import java.net.URI

import scala.async.Async
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

import com.linkedin.drelephant.spark.data.SparkLogDerivedData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec}
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.jackson.JsonMethods


/**
  * A client for getting data from the Spark event logs, using the location configured for spark.eventLog.dir.
  *
  * This client uses webhdfs to access the location, even if spark.eventLog.dir is provided as an hdfs URL.
  *
  * The codecs used by this client use JNI, which results in some weird classloading issues (at least when testing in the console),
  * so some of the client's implementation is non-lazy or synchronous when needed.
  */
class SparkLogClient(hadoopConfiguration: Configuration, sparkConf: SparkConf) {
  import SparkLogClient._
  import Async.async

  private val logger: Logger = Logger.getLogger(classOf[SparkLogClient])

  private[fetchers] val webhdfsEventLogUri: URI = {
    val eventLogUri = sparkConf.getOption(SPARK_EVENT_LOG_DIR_KEY).map(new URI(_))
    val dfsNamenodeHttpAddress = Option(hadoopConfiguration.get(HADOOP_DFS_NAMENODE_HTTP_ADDRESS_KEY))
    (eventLogUri, dfsNamenodeHttpAddress) match {
      case (Some(eventLogUri), _) if eventLogUri.getScheme == "webhdfs" =>
        eventLogUri
      case (Some(eventLogUri), Some(dfsNamenodeHttpAddress)) if eventLogUri.getScheme == "hdfs" =>
        val dfsNamenodeHttpUri = new URI(null, dfsNamenodeHttpAddress, null, null, null)
        new URI(s"webhdfs://${eventLogUri.getHost}:${dfsNamenodeHttpUri.getPort}${eventLogUri.getPath}")
      case _ =>
        throw new IllegalArgumentException(
          s"""|${SPARK_EVENT_LOG_DIR_KEY} must be provided as webhdfs:// or hdfs://;
              |if hdfs, ${HADOOP_DFS_NAMENODE_HTTP_ADDRESS_KEY} must also be provided for port""".stripMargin.replaceAll("\n", " ")
        )
    }
  }

  private[fetchers] lazy val fs: FileSystem = FileSystem.get(webhdfsEventLogUri, hadoopConfiguration)

  private lazy val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", defaultValue = false)
  private lazy val compressionCodec = if (shouldCompress) Some(compressionCodecFromConf(sparkConf)) else None
  private lazy val compressionCodecShortName = compressionCodec.map(shortNameOfCompressionCodec)

  def fetchData(appId: String, attemptId: Option[String])(implicit ec: ExecutionContext): Future[SparkLogDerivedData] = {
    val logPath = getLogPath(webhdfsEventLogUri, appId, attemptId, compressionCodecShortName)
    logger.info(s"looking for logs at ${logPath}")

    val codec = compressionCodecForLogPath(sparkConf, logPath)

    // Limit scope of async.
    async {
      resource.managed { openEventLog(sparkConf, logPath, fs) }
        .acquireAndGet { in => findDerivedData(codec.map { _.compressedInputStream(in) }.getOrElse(in)) }
    }
  }
}

object SparkLogClient {
  import JsonAST._

  val SPARK_EVENT_LOG_DIR_KEY = "spark.eventLog.dir"
  val HADOOP_DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address"

  private implicit val formats: DefaultFormats = DefaultFormats

  def findDerivedData(in: InputStream, eventsLimit: Option[Int] = None): SparkLogDerivedData = {
    val events = eventsLimit.map { getEvents(in).take(_) }.getOrElse { getEvents(in) }

    var environmentUpdate: Option[SparkListenerEnvironmentUpdate] = None
    while (events.hasNext && environmentUpdate.isEmpty) {
      val event = events.next
      event match {
        case Some(eu: SparkListenerEnvironmentUpdate) => environmentUpdate = Some(eu)
        case _ => {} // Do nothing.
      }
    }

    environmentUpdate
      .map(SparkLogDerivedData(_))
      .getOrElse { throw new IllegalArgumentException("Spark event log doesn't have Spark properties") }
  }

  private def getEvents(in: InputStream): Iterator[Option[SparkListenerEvent]] = getLines(in).map(lineToEvent)

  private def getLines(in: InputStream): Iterator[String] = Source.fromInputStream(in).getLines

  private def lineToEvent(line: String): Option[SparkListenerEvent] = sparkEventFromJson(JsonMethods.parse(line))

  // Below this line are modified utility methods from:
  //
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/io/CompressionCodec.scala
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/Utils.scala

  private val IN_PROGRESS = ".inprogress"
  private val DEFAULT_COMPRESSION_CODEC = "snappy"

  private val compressionCodecClassNamesByShortName = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName
  )

  // A cache for compression codecs to avoid creating the same codec many times
  private val compressionCodecMap = HashMap.empty[String, CompressionCodec]

  private def compressionCodecFromConf(conf: SparkConf): CompressionCodec = {
    val codecName = conf.get("spark.io.compression.codec", DEFAULT_COMPRESSION_CODEC)
    loadCompressionCodec(conf, codecName)
  }

  private def loadCompressionCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass = compressionCodecClassNamesByShortName.getOrElse(codecName.toLowerCase, codecName)
    val classLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val codec = try {
      val ctor = Class.forName(codecClass, true, classLoader).getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
    } catch {
      case e: ClassNotFoundException => None
      case e: IllegalArgumentException => None
    }
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available. "))
  }

  private def shortNameOfCompressionCodec(compressionCodec: CompressionCodec): String = {
    val codecName = compressionCodec.getClass.getName
    if (compressionCodecClassNamesByShortName.contains(codecName)) {
      codecName
    } else {
      compressionCodecClassNamesByShortName
        .collectFirst { case (k, v) if v == codecName => k }
        .getOrElse { throw new IllegalArgumentException(s"No short name for codec $codecName.") }
    }
  }

  private def getLogPath(
    logBaseDir: URI,
    appId: String,
    appAttemptId: Option[String],
    compressionCodecName: Option[String] = None
  ): Path = {
    val base = logBaseDir.toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      new Path(base + "_" + sanitize(appAttemptId.get) + codec)
    } else {
      new Path(base + codec)
    }
  }

  private def openEventLog(conf: SparkConf, logPath: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(logPath)) {
      throw new FileNotFoundException(s"File ${logPath} does not exist.")
    }

    new BufferedInputStream(fs.open(logPath))
  }

  private def compressionCodecForLogPath(conf: SparkConf, logPath: Path): Option[CompressionCodec] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logBaseName = logPath.getName.stripSuffix(IN_PROGRESS)
    logBaseName.split("\\.").tail.lastOption.map { codecName =>
      compressionCodecMap.getOrElseUpdate(codecName, loadCompressionCodec(conf, codecName))
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase
  }

  private def sparkEventFromJson(json: JValue): Option[SparkListenerEvent] = {
    val environmentUpdate = getFormattedClassName(SparkListenerEnvironmentUpdate)

    (json \ "Event").extract[String] match {
      case `environmentUpdate` => Some(environmentUpdateFromJson(json))
      case _ => None
    }
  }

  private def getFormattedClassName(obj: AnyRef): String = obj.getClass.getSimpleName.replace("$", "")

  private def environmentUpdateFromJson(json: JValue): SparkListenerEnvironmentUpdate = {
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  private def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.map { case JField(k, JString(v)) => (k, v) }.toMap
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }
}
