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

import java.util.concurrent.TimeoutException

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}
import com.linkedin.drelephant.analysis.{AnalyticJob, ElephantFetcher}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkConf


/**
  * A fetcher that gets Spark-related data from a combination of the Spark monitoring REST API and Spark event logs.
  */
class SparkFetcher(fetcherConfigurationData: FetcherConfigurationData)
  extends ElephantFetcher[SparkApplicationData] {

  import SparkFetcher._
  import ExecutionContext.Implicits.global

  private val logger: Logger = Logger.getLogger(classOf[SparkFetcher])

  val eventLogUri = Option(fetcherConfigurationData.getParamMap.get(LOG_LOCATION_URI_XML_FIELD))
  logger.info("The event log location of Spark application is set to " + eventLogUri)

  private[fetchers] lazy val hadoopConfiguration: Configuration = new Configuration()

  private[fetchers] lazy val sparkUtils: SparkUtils = SparkUtils

  private[fetchers] lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkUtils.getDefaultPropertiesFile() match {
      case Some(filename) => sparkConf.setAll(sparkUtils.getPropertiesFromFile(filename))
      case None => throw new IllegalStateException("can't find Spark conf; please set SPARK_HOME or SPARK_CONF_DIR")
    }
    sparkConf
  }

  private[fetchers] lazy val eventLogSource: EventLogSource = {
    val eventLogEnabled = sparkConf.getBoolean(SPARK_EVENT_LOG_ENABLED_KEY, false)
    val useRestForLogs = Option(fetcherConfigurationData.getParamMap.get("use_rest_for_eventlogs"))
      .exists(_.toBoolean)
    if (!eventLogEnabled) {
      EventLogSource.None
    } else if (useRestForLogs) EventLogSource.Rest else EventLogSource.WebHdfs
  }

  private[fetchers] lazy val shouldProcessLogsLocally = (eventLogSource == EventLogSource.Rest) &&
    Option(fetcherConfigurationData.getParamMap.get("should_process_logs_locally")).exists(_.toLowerCase == "true")

  private[fetchers] lazy val sparkRestClient: SparkRestClient = new SparkRestClient(sparkConf)

  private[fetchers] lazy val sparkLogClient: SparkLogClient = {
    new SparkLogClient(hadoopConfiguration, sparkConf, eventLogUri)
  }

  override def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    doFetchData(analyticJob) match {
      case Success(data) => data
      case Failure(e) => throw new TimeoutException()
    }
  }

  private def doFetchData(analyticJob: AnalyticJob): Try[SparkApplicationData] = {
    val appId = analyticJob.getAppId
    logger.info(s"Fetching data for ${appId}")
    Try {
      Await.result(doFetchSparkApplicationData(analyticJob), DEFAULT_TIMEOUT)
    }.transform(
      data => {
        logger.info(s"Succeeded fetching data for ${appId}")
        Success(data)
      },
      e => {
        logger.warn(s"Failed fetching data for ${appId}." + " I will retry after some time! " + "Exception Message is: " + e.getMessage)
        Failure(e)
      }
    )
  }

  private def doFetchSparkApplicationData(analyticJob: AnalyticJob): Future[SparkApplicationData] = {
    if (shouldProcessLogsLocally) {
      Future {
        sparkRestClient.fetchEventLogAndParse(analyticJob.getAppId)
      }
    } else {
      doFetchDataUsingRestAndLogClients(analyticJob)
    }
  }

  private def doFetchDataUsingRestAndLogClients(analyticJob: AnalyticJob): Future[SparkApplicationData] = Future {
    val appId = analyticJob.getAppId
    val restDerivedData = Await.result(sparkRestClient.fetchData(appId, eventLogSource == EventLogSource.Rest), DEFAULT_TIMEOUT)

    val logDerivedData = eventLogSource match {
      case EventLogSource.None => None
      case EventLogSource.Rest => restDerivedData.logDerivedData
      case EventLogSource.WebHdfs =>
        val lastAttemptId = restDerivedData.applicationInfo.attempts.maxBy {
          _.startTime
        }.attemptId
        Some(Await.result(sparkLogClient.fetchData(appId, lastAttemptId), DEFAULT_TIMEOUT))
    }
    SparkApplicationData(appId, restDerivedData, logDerivedData)
  }
}

object SparkFetcher {

  sealed trait EventLogSource

  object EventLogSource {

    /** Fetch event logs through REST API. */
    case object Rest extends EventLogSource

    /** Fetch event logs through WebHDFS. */
    case object WebHdfs extends EventLogSource

    /** Event logs are not available. */
    case object None extends EventLogSource

  }

  val SPARK_EVENT_LOG_ENABLED_KEY = "spark.eventLog.enabled"
  val DEFAULT_TIMEOUT = Duration(5, SECONDS)
  val LOG_LOCATION_URI_XML_FIELD = "event_log_location_uri"
  val FETCH_FAILED_TASKS = "fetch_failed_tasks"
}
