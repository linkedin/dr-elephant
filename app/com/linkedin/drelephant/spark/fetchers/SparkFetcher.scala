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

import scala.async.Async
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.control.NonFatal

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

  private[fetchers] lazy val hadoopConfiguration: Configuration = new Configuration()

  private[fetchers] lazy val sparkUtils: SparkUtils = SparkUtils

  private[fetchers] lazy val sparkConf: SparkConf = {
    // Allow to override SPARK_CONF_DIR and SPARK_HOME with values in
    // the fetcher configuration.
    val envOverride = fetcherConfigurationData.getParamMap.asScala
      .map { case (key, value) => key.toUpperCase() -> value }
      .toMap
    val env = sparkUtils.defaultEnv ++ envOverride

    val sparkConf = new SparkConf()
    sparkUtils.getDefaultPropertiesFile(env) match {
      case Some(filename) =>
        logger.info(s"Loading Spark configuration from $filename")
        sparkConf.setAll(sparkUtils.getPropertiesFromFile(filename))
      case None => throw new IllegalStateException(
        "can't find Spark conf; please set SPARK_HOME or SPARK_CONF_DIR " +
        "or define them in lowercase in fetcher params")
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

  private[fetchers] lazy val sparkRestClient: SparkRestClient = new SparkRestClient(sparkConf)

  private[fetchers] lazy val sparkLogClient: SparkLogClient = {
    new SparkLogClient(hadoopConfiguration, sparkConf)
  }

  override def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    val appId = analyticJob.getAppId
    logger.info(s"Fetching data for ${appId}")
    try {
      Await.result(doFetchData(sparkRestClient, sparkLogClient, appId, eventLogSource),
        DEFAULT_TIMEOUT)
    } catch {
      case NonFatal(e) =>
        logger.error(s"Failed fetching data for ${appId}", e)
        throw e
    }
  }
}

object SparkFetcher {
  import Async.{async, await}

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
  val DEFAULT_TIMEOUT = Duration(30, SECONDS)

  private def doFetchData(
    sparkRestClient: SparkRestClient,
    sparkLogClient: SparkLogClient,
    appId: String,
    eventLogSource: EventLogSource
  )(
    implicit ec: ExecutionContext
  ): Future[SparkApplicationData] = async {
    val restDerivedData = await(sparkRestClient.fetchData(
      appId, eventLogSource == EventLogSource.Rest))

    val logDerivedData = eventLogSource match {
      case EventLogSource.None => None
      case EventLogSource.Rest => restDerivedData.logDerivedData
      case EventLogSource.WebHdfs =>
        val lastAttemptId = restDerivedData.applicationInfo.attempts.maxBy { _.startTime }.attemptId
        Some(await(sparkLogClient.fetchData(appId, lastAttemptId)))
    }

    SparkApplicationData(appId, restDerivedData, logDerivedData)
  }
}
