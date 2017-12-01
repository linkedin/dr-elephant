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

package com.linkedin.drelephant.util

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException, InputStream, InputStreamReader}
import java.net.URI
import java.util.Properties

import scala.collection.JavaConverters
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter, FileStatus}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec}


trait SparkUtils {
  import JavaConverters._

  protected def logger: Logger

  protected def hadoopUtils: HadoopUtils

  protected def defaultEnv: Map[String, String]

  val SPARK_EVENT_LOG_DIR_KEY = "spark.eventLog.dir"
  val SPARK_EVENT_LOG_COMPRESS_KEY = "spark.eventLog.compress"
  val DFS_HTTP_PORT = 50070

  /**
    * Returns the webhdfs FileSystem and Path for the configured Spark event log directory and optionally the
    * configured Hadoop namenode.
    *
    * Primarily the FileSystem and Path are based on spark.eventLog.dir but if spark.eventLog.dir is a simple path,
    * then it is combined with the namenode info from the Hadoop configuration.
    *
    * @param hadoopConfiguration a Hadoop configuration containing namenode info
    * @param sparkConf a Spark configuration with the Spark event log directory setting
    * @return a tuple (FileSystem, Path) for the configured Spark event log directory
    */
  def fileSystemAndPathForEventLogDir(hadoopConfiguration: Configuration,
                                      sparkConf: SparkConf,
                                      uriFromFetcherConf : Option[String]): (FileSystem, Path) = {
    if(uriFromFetcherConf.isDefined) {
      logger.info(s"Using log location from FetcherConf ${uriFromFetcherConf}")
      val uri = new URI(uriFromFetcherConf.get)
      (FileSystem.get(uri, hadoopConfiguration), new Path(uri.getPath))
    } else {
      val eventLogUri = sparkConf.getOption(SPARK_EVENT_LOG_DIR_KEY).map(new URI(_))
      eventLogUri match {
        case Some(uri) if uri.getScheme == "webhdfs" =>
          (FileSystem.get(uri, hadoopConfiguration), new Path(uri.getPath))
        case Some(uri) if uri.getScheme == "hdfs" =>
          (FileSystem.get(new URI(s"webhdfs://${uri.getHost}:${DFS_HTTP_PORT}${uri.getPath}"), hadoopConfiguration), new Path(uri.getPath))
        case Some(uri) =>
          val nameNodeAddress
          = hadoopUtils.findHaNameNodeAddress(hadoopConfiguration)
            .orElse(hadoopUtils.httpNameNodeAddress(hadoopConfiguration))
          nameNodeAddress match {
            case Some(address) =>
              (FileSystem.get(new URI(s"webhdfs://${address}${uri.getPath}"), hadoopConfiguration), new Path(uri.getPath))
            case None =>
              throw new IllegalArgumentException("Couldn't find configured namenode")
          }
        case None =>
          throw new IllegalArgumentException("${SPARK_EVENT_LOG_DIR_KEY} not provided")
      }
    }
  }

  /**
    * Returns the path and codec for the event log for the given app and attempt.
    *
    * This invokes JNI to get the codec, so it must be done synchronously, otherwise weird classloading issues will
    * manifest (at least they manifest during testing).
    *
    * The path and codec can then be passed to withEventLog, which can be called asynchronously.
    *
    * @param sparkConf the Spark configuration with the setting for whether Spark event logs are compressed
    * @param fs the filesystem which contains the logs
    * @param basePath the base path for logs on the given filesystem
    * @param appId the app identifier to use for the specific log file
    * @param attemptId the attempt identifier to use for the specific log file
    * @return a tuple (Path, Option[CompressionCodec]) for the specific event log file and the codec to use
    */
  def pathAndCodecforEventLog(
    sparkConf: SparkConf,
    fs: FileSystem,
    basePath: Path,
    appId: String,
    attemptId: Option[String]
  ): (Path, Option[CompressionCodec]) = {
    attemptId match {
      // if attemptid is given, use the existing method
      case x: Some[String] => { val path = {
          val shouldUseCompression = sparkConf.getBoolean(SPARK_EVENT_LOG_COMPRESS_KEY, defaultValue = false)
          val compressionCodecShortName =
            if (shouldUseCompression) Some(shortNameOfCompressionCodec(compressionCodecFromConf(sparkConf))) else None
          getLogPath(fs.getUri.resolve(basePath.toUri), appId, attemptId, compressionCodecShortName)
        }
          val codec = compressionCodecForLogName(sparkConf, path.getName())
          (path, codec)
      }
      case None => {
        val (logPath, codecName) = getLogPathAndCodecName(fs, fs.getUri.resolve(basePath.toUri), appId)

        (logPath, Some(compressionCodecMap.getOrElseUpdate(codecName, loadCompressionCodec(sparkConf, codecName))))
      }
    }

  }

  /**
    * A loan method that performs the given function on the loaned event log inputstream, and closes it after use.
    *
    * The method arguments should have been attained from fileSystemAndPathForEventLogDir and pathAndCodecforEventLog.
    *
    * @param fs the filesystem which contains the log
    * @param path the full path to the log
    * @param codec the codec to use for the log
    */
  def withEventLog[T](fs: FileSystem, path: Path, codec: Option[CompressionCodec])(f: InputStream => T): T = {
    resource.managed { openEventLog(path, fs) }
      .map { in => codec.map { _.compressedInputStream(in) }.getOrElse(in) }
      .acquireAndGet(f)
  }

  // Below this line are modified utility methods from
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/Utils.scala

  /** Return the path of the default Spark properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = defaultEnv): Option[String] = {
    env.get("SPARK_CONF_DIR")
      .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } finally {
      inReader.close()
    }
  }

  def compressionCodecForLogName(conf: SparkConf, logName: String): Option[CompressionCodec] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logBaseName = logName.stripSuffix(IN_PROGRESS)
    logBaseName.split("\\.").tail.lastOption.map { codecName =>
      compressionCodecMap.getOrElseUpdate(codecName, loadCompressionCodec(conf, codecName))
    }
  }

  private val IN_PROGRESS = ".inprogress"
  private val DEFAULT_COMPRESSION_CODEC = "lz4"

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

  private def splitLogPath( logPath: String) : (Option[String],Option[String],Option[String]) = {
    var extension: Option[String] = None
    var attempt: Option[String] = None
    var appId: Option[String] = None
    val nameAndExtension = logPath.split('.')
    if( nameAndExtension.length == 2 ) {
      extension = Some(nameAndExtension(1))
      val name = nameAndExtension(0)
      val appIdAndAttempt = name.split('_')
      if( appIdAndAttempt.length == 4 ) {
        attempt = Some(appIdAndAttempt(3))
        appId = Some(appIdAndAttempt.dropRight(1).mkString("_"))
      } else {
        appId = Some(name)
      }
    }
    (appId, attempt, extension)
  }
  private def getLogPathAndCodecName(
                                    fs: FileSystem,
                                    logBaseDir: URI,
                                    appId: String
                                    ): (Path, String) = {
    val base = logBaseDir.toString.stripSuffix("/");
    val filter = new PathFilter() {
       override def accept(file: Path): Boolean = {
        file.getName().startsWith(appId);
      }
    }
    val attemptsList = fs.listStatus(new Path(base), filter)
    val finalAttempt = attemptsList.length match {
      case 0 => throw new FileNotFoundException(s"logfile does not exist for ${appId}.")
      case 1 => splitLogPath(attemptsList(0).getPath().getName())
      case _ => attemptsList.
                        map( x => splitLogPath(x.getPath().getName())).
                        sortWith( (x,y) => x._2.getOrElse("-1").toInt > y._2.getOrElse("-1").toInt ).
                        head
    }

    finalAttempt match {
      // if attemptId is none and the codec is available, use the appid with no attemptid suffix
      case noAttempt if noAttempt._1 != None & noAttempt._2 == None & noAttempt._3 != None =>
                                                          (new Path(base +
                                                              "/" + finalAttempt._1.get +
                                                              "." + finalAttempt._3.get), finalAttempt._3.get)
      // if attemptId is available and the codec is available, use the appid with attemptid suffix
      case attempt if attempt._1 != None & attempt._2 != None & attempt._3 != None =>
                                                          (new Path(base +
                                                                "/" + attempt._1.get +
                                                                "_" + sanitize(finalAttempt._2.get) +
                                                                "." + finalAttempt._3.get), finalAttempt._3.get)
      // if codec is not available, but we found a file match with appId, use the actual file Path from the first match
      case nocodec if nocodec._1 != None & nocodec._3 == None => (attemptsList(0).getPath(), DEFAULT_COMPRESSION_CODEC)

      // This should be reached only if we can't parse the filename in the path.
      // Try to construct a general path in that case.
      case _ => (new Path(base + "/" + appId + "." + DEFAULT_COMPRESSION_CODEC), DEFAULT_COMPRESSION_CODEC)
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
  private def openEventLog(logPath: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(logPath)) {
      throw new FileNotFoundException(s"File ${logPath} does not exist.")
    }

    new BufferedInputStream(fs.open(logPath))
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase
  }
}

object SparkUtils extends SparkUtils {
  override protected lazy val logger = Logger.getLogger(classOf[SparkUtils])
  override protected lazy val hadoopUtils = HadoopUtils
  override protected lazy val defaultEnv = sys.env
}
