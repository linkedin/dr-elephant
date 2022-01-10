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

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.zip.ZipInputStream
import java.util.{Calendar, SimpleTimeZone}

import com.linkedin.drelephant.spark.legacydata.LegacyDataConverters
import org.apache.spark.deploy.history.SparkDataCollection

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfo, ExecutorSummary, JobData, StageData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl, JobDataImpl, StageDataImpl}
import com.linkedin.drelephant.util.SparkUtils
import javax.ws.rs.client.{Client, ClientBuilder, WebTarget}
import javax.ws.rs.core.MediaType

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.glassfish.jersey.client.ClientProperties

import scala.concurrent.duration.{Duration, SECONDS}

/**
  * A client for getting data from the Spark monitoring REST API, e.g. <https://spark.apache.org/docs/1.4.1/monitoring.html#rest-api>.
  *
  * Jersey classloading seems to be brittle (at least when testing in the console), so some of the implementation is non-lazy
  * or synchronous when needed.
  */
class SparkRestClient(sparkConf: SparkConf) {

  import SparkRestClient._

  private val logger: Logger = Logger.getLogger(classOf[SparkRestClient])

  private val client: Client = ClientBuilder.newClient()

  private val historyServerUri: URI = sparkConf.getOption(HISTORY_SERVER_ADDRESS_KEY) match {
    case Some(historyServerAddress) =>
      val baseUri: URI =
      // Latest versions of CDH include http in their history server address configuration.
      // However, it is not recommended by Spark documentation(http://spark.apache.org/docs/latest/running-on-yarn.html)
        if (historyServerAddress.contains(s"http://")) {
          new URI(historyServerAddress)
        } else {
          new URI(s"http://${historyServerAddress}")
        }
      require(baseUri.getPath == "")
      baseUri
    case None =>
      throw new IllegalArgumentException("spark.yarn.historyServer.address not provided; can't use Spark REST API")
  }

  private val apiTarget: WebTarget = client.property(ClientProperties.CONNECT_TIMEOUT, CONNECTION_TIMEOUT).property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT).target(historyServerUri).path(API_V1_MOUNT_PATH)

  def fetchData(appId: String, fetchLogs: Boolean = false)(
    implicit ec: ExecutionContext
  ): Future[SparkRestDerivedData] = {
    val (applicationInfo, attemptTarget) = getApplicationMetaData(appId)

    Future {
      val futureJobDatas = Future {
        getJobDatas(attemptTarget)
      }
      val futureStageDatas = Future {
        getStageDatas(attemptTarget)
      }
      val futureExecutorSummaries = Future {
        getExecutorSummaries(attemptTarget)
      }
      val futureLogData = if (fetchLogs) {
        Future {
          getLogData(attemptTarget)
        }
      } else Future.successful(None)

      SparkRestDerivedData(
        applicationInfo,
        Await.result(futureJobDatas, DEFAULT_TIMEOUT),
        Await.result(futureStageDatas, DEFAULT_TIMEOUT),
        Await.result(futureExecutorSummaries, Duration(5, SECONDS)),
        Await.result(futureLogData, Duration(5, SECONDS))
      )

    }
  }

  def fetchEventLogAndParse(appId: String): SparkApplicationData = {
    val (_, attemptTarget) = getApplicationMetaData(appId)
    val logTarget = attemptTarget.path("logs")
    logger.info(s"creating SparkApplication by calling REST API at ${logTarget.getUri} to get eventlogs")
    resource.managed {
      getApplicationLogs(logTarget)
    }.acquireAndGet { zipInputStream =>
      getLogInputStream(zipInputStream, logTarget) match {
        case (None, _) => throw new RuntimeException(s"Failed to read log for application ${appId}")
        case (Some(inputStream), fileName) => {
          val dataCollection = new SparkDataCollection()
          dataCollection.load(inputStream, fileName)
          LegacyDataConverters.convert(dataCollection)
        }
      }
    }
  }

  private def getApplicationMetaData(appId: String): (ApplicationInfo, WebTarget) = {
    val appTarget = apiTarget.path(s"applications/${appId}")
    logger.info(s"calling REST API at ${appTarget.getUri}")

    val applicationInfo = getApplicationInfo(appTarget)

    val lastAttemptId = applicationInfo.attempts.maxBy {
      _.startTime
    }.attemptId
    val attemptTarget = lastAttemptId.map(appTarget.path).getOrElse(appTarget)
    (applicationInfo, attemptTarget)
  }

  private def getApplicationInfo(appTarget: WebTarget): ApplicationInfoImpl = {
    try {
      get(appTarget, SparkRestObjectMapper.readValue[ApplicationInfoImpl])
    } catch {
      case NonFatal(e) => {
        logger.error(s"error reading applicationInfo ${appTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }

  private def getLogData(attemptTarget: WebTarget): Option[SparkLogDerivedData] = {
    val target = attemptTarget.path("logs")
    logger.info(s"calling REST API at ${target.getUri} to get eventlogs")
    resource.managed {
      getApplicationLogs(target)
    }.acquireAndGet { zis =>
      val (inputStream, _) = getLogInputStream(zis, target)
      inputStream.map(SparkLogClient.findDerivedData(_))
    }
  }

  private[fetchers] def getApplicationLogs(logTarget: WebTarget): ZipInputStream = {
    try {
      val is = logTarget.request(MediaType.APPLICATION_OCTET_STREAM)
        .get(classOf[java.io.InputStream])
      new ZipInputStream(new BufferedInputStream(is))
    } catch {
      case NonFatal(e) => {
        logger.error(s"error reading logs ${logTarget.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }

  private def getLogInputStream(zis: ZipInputStream, attemptTarget: WebTarget): (Option[InputStream], String) = {
    // The logs are stored in a ZIP archive with a single entry.
    // It should be named as "$logPrefix.$archiveExtension", but
    // we trust Spark to get it right.
    val entry = zis.getNextEntry
    if (entry == null) {
      logger.warn(s"failed to resolve log for ${attemptTarget.getUri}")
      (None, "")
    } else {
      val entryName = entry.getName
      if (entryName.endsWith(IN_PROGRESS)) {
        // Making sure that the application has finished.
        throw new RuntimeException(s"Application for the log ${entryName} has not finished yet.")
      }
      val codec = SparkUtils.compressionCodecForLogName(sparkConf, entryName)
      (Some(codec.map {
        _.compressedInputStream(zis)
      }.getOrElse(zis)), entryName)
    }
  }

  private def getJobDatas(attemptTarget: WebTarget): Seq[JobDataImpl] = {
    val target = attemptTarget.path("jobs")
    try {
      get(target, SparkRestObjectMapper.readValue[Seq[JobDataImpl]])
    } catch {
      case NonFatal(e) => {
        logger.error(s"error reading jobData ${target.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }

  private def getStageDatas(attemptTarget: WebTarget): Seq[StageDataImpl] = {
    val target = attemptTarget.path("stages")
    try {
      get(target, SparkRestObjectMapper.readValue[Seq[StageDataImpl]])
    } catch {
      case NonFatal(e) => {
        logger.warn(s"error reading stageData ${target.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }

  private def getExecutorSummaries(attemptTarget: WebTarget): Seq[ExecutorSummaryImpl] = {
    val target = attemptTarget.path("executors")
    try {
      get(target, SparkRestObjectMapper.readValue[Seq[ExecutorSummaryImpl]])
    } catch {
      case NonFatal(e) => {
        logger.error(s"error reading executorSummary ${target.getUri}. Exception Message = " + e.getMessage)
        logger.debug(e)
        throw e
      }
    }
  }
}

object SparkRestClient {
  val HISTORY_SERVER_ADDRESS_KEY = "spark.yarn.historyServer.address"
  val API_V1_MOUNT_PATH = "api/v1"
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_TIMEOUT = Duration(5, SECONDS);
  val CONNECTION_TIMEOUT = 5000
  val READ_TIMEOUT = 5000

  val SparkRestObjectMapper = {
    val dateFormat = {
      val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
      val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
      iso8601.setCalendar(cal)
      iso8601
    }

    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setDateFormat(dateFormat)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper
  }

  def get[T](webTarget: WebTarget, converter: String => T): T =
    converter(webTarget.request(MediaType.APPLICATION_JSON).get(classOf[String]))
}
