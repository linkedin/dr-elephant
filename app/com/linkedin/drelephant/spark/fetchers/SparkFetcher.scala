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
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

import com.linkedin.drelephant.analysis.{AnalyticJob, ElephantFetcher}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
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
  import Async.{async, await}
  import ExecutionContext.Implicits.global

  private val logger: Logger = Logger.getLogger(classOf[SparkFetcher])

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

  private[fetchers] lazy val sparkRestClient: SparkRestClient = new SparkRestClient(sparkConf)

  private[fetchers] lazy val sparkLogClient: Option[SparkLogClient] = {
    val eventLogEnabled = sparkConf.getBoolean(SPARK_EVENT_LOG_ENABLED_KEY, false)
    if (eventLogEnabled) Some(new SparkLogClient(hadoopConfiguration, sparkConf)) else None
  }

  private[fetchers] lazy val backupFetcher: ElephantFetcher[SparkApplicationData] =
    new LegacyFetcher(fetcherConfigurationData)

  override def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    doFetchData(analyticJob) match {
      case Success(data) => data
      case Failure(e) => throw e
    }
  }

  private def doFetchData(analyticJob: AnalyticJob): Try[SparkApplicationData] = {
    val appId = analyticJob.getAppId
    logger.info(s"Fetching data for ${appId}")
    Try {
      Await.result(doFetchDataUsingRestAndLogClients(analyticJob), DEFAULT_TIMEOUT)
    }.recover {
      case e => {
        logger.warn("Exception fetching data. Will make another attempt with backup fetcher instead.", e)
        Await.result(doFetchDataUsingBackupFetcher(analyticJob), DEFAULT_TIMEOUT)
      }
    }.transform(
      data => {
        logger.info(s"Succeeded fetching data for ${appId}")
        Success(data)
      },
      e => {
        logger.error(s"Failed fetching data for ${appId}", e)
        Failure(e)
      }
    )
  }

  private def doFetchDataUsingRestAndLogClients(analyticJob: AnalyticJob): Future[SparkApplicationData] = async {
    val appId = analyticJob.getAppId
    val restDerivedData = await(sparkRestClient.fetchData(appId))
    val lastAttemptId = restDerivedData.applicationInfo.attempts.maxBy { _.startTime }.attemptId

    // Would use .map but await doesn't like that construction.
    val logDerivedData = sparkLogClient match {
      case Some(sparkLogClient) => Some(await(sparkLogClient.fetchData(appId, lastAttemptId)))
      case None => None
    }

    SparkApplicationData(appId, restDerivedData, logDerivedData)
  }

  private def doFetchDataUsingBackupFetcher(analyticJob: AnalyticJob): Future[SparkApplicationData] = async {
    backupFetcher.fetchData(analyticJob)
  }
}

object SparkFetcher {
  import org.apache.spark.deploy.history.SparkFSFetcher
  import com.linkedin.drelephant.spark.legacydata.LegacyDataConverters

  private[fetchers] class LegacyFetcher(fetcherConfigurationData: FetcherConfigurationData)
      extends ElephantFetcher[SparkApplicationData] {
    lazy val legacyFetcher = new SparkFSFetcher(fetcherConfigurationData)

    override def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
      val legacyData = legacyFetcher.fetchData(analyticJob)
      LegacyDataConverters.convert(legacyData)
    }
  }

  val SPARK_EVENT_LOG_ENABLED_KEY = "spark.eventLog.enabled"
  val DEFAULT_TIMEOUT = Duration(60, SECONDS)
}
