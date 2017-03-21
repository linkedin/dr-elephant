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

import java.io.InputStream
import java.nio.file.Files
import java.util.Date

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, Future}

import com.linkedin.drelephant.analysis.{AnalyticJob, ApplicationType}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.spark.data.{SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfo, ApplicationInfo}
import com.linkedin.drelephant.spark.legacydata.{MockSparkApplicationData, SparkGeneralData}
import com.linkedin.drelephant.util.{SparkUtils, HadoopUtils}
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.SparkFSFetcher
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.mockito.Mockito
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

class SparkFetcherTest extends FunSpec with Matchers with MockitoSugar {
  import SparkFetcherTest._

  describe("SparkFetcher") {
    import ExecutionContext.Implicits.global

    val fetcherConfigurationData = newFakeFetcherConfigurationData()

    val appId = "application_1"

    val t2 = System.currentTimeMillis
    val t1 = t2 - 1
    val duration = 8000000L

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfo(
        appId,
        "app",
        Seq(
          newFakeApplicationAttemptInfo(Some("2"), startTime = new Date(t2 - duration), endTime = new Date(t2)),
          newFakeApplicationAttemptInfo(Some("1"), startTime = new Date(t1 - duration), endTime = new Date(t1))
        )
      ),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty
    )

    val logDerivedData = SparkLogDerivedData(SparkListenerEnvironmentUpdate(Map.empty))

    val analyticJob = new AnalyticJob().setAppId(appId)

    it("returns data") {
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkConf = new SparkConf()
        override lazy val sparkRestClient = newFakeSparkRestClient(appId, Future(restDerivedData))
        override lazy val sparkLogClient = Some(newFakeSparkLogClient(appId, Some("2"), Future(logDerivedData)))
      }
      val data = sparkFetcher.fetchData(analyticJob)
      data.appId should be(appId)
    }

    it("returns data from the legacy fetchers if the REST client fails") {
      val sharedSparkConf = new SparkConf()
      sharedSparkConf.set("spark.eventLog.dir", "/logs/spark")

      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkConf = sharedSparkConf
        override lazy val sparkRestClient = newFakeSparkRestClient(appId, Future { throw new Exception() })
        override lazy val sparkLogClient = Some(newFakeSparkLogClient(appId, Some("2"), Future { throw new Exception() }))
        override lazy val backupFetcher = new SparkFetcher.LegacyFetcher(fetcherConfigurationData) {
          override lazy val legacyFetcher = new SparkFSFetcher(fetcherConfigurationData) {
            override lazy val sparkConf = sharedSparkConf
            override def fetchData(analyticJob: AnalyticJob) = new MockSparkApplicationData() {
              val generalData = {
                val generalData = new SparkGeneralData()
                generalData.setApplicationId("application_1")
                generalData.setApplicationName("app")
                generalData.setStartTime(1000L)
                generalData.setEndTime(2000L)
                generalData.setSparkUser("foo")
                generalData
              }

              override def getGeneralData(): SparkGeneralData = generalData
            }
          }
        }
      }

      val data = sparkFetcher.fetchData(analyticJob)
      data.appId should be("application_1")
    }

    it("throws an exception if the REST client fails") {
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkConf = new SparkConf()
        override lazy val sparkRestClient = newFakeSparkRestClient(appId, Future { throw new Exception() })
        override lazy val sparkLogClient = Some(newFakeSparkLogClient(appId, Some("2"), Future(logDerivedData)))
      }

      an[Exception] should be thrownBy { sparkFetcher.fetchData(analyticJob) }
    }

    it("throws an exception if the log client fails") {
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkConf = new SparkConf()
        override lazy val sparkRestClient = newFakeSparkRestClient(appId, Future(restDerivedData))
        override lazy val sparkLogClient = Some(newFakeSparkLogClient(appId, Some("2"), Future { throw new Exception() }))
      }

      an[Exception] should be thrownBy { sparkFetcher.fetchData(analyticJob) }
    }

    it("gets its SparkConf when SPARK_CONF_DIR is set") {
      val tempDir = Files.createTempDirectory(null)

      val testResourceIn = getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
      val testResourceFile = tempDir.resolve("spark-defaults.conf")
      Files.copy(testResourceIn, testResourceFile)

      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map("SPARK_CONF_DIR" -> tempDir.toString)
        }
      }
      val sparkConf = sparkFetcher.sparkConf

      testResourceIn.close()
      Files.delete(testResourceFile)
      Files.delete(tempDir)

      sparkConf.get("spark.yarn.historyServer.address") should be("jh1.grid.example.com:18080")
      sparkConf.get("spark.eventLog.enabled") should be("true")
      sparkConf.get("spark.eventLog.compress") should be("true")
      sparkConf.get("spark.eventLog.dir") should be("hdfs://nn1.grid.example.com:9000/logs/spark")
    }

    it("gets its SparkConf when SPARK_HOME is set") {
      val tempDir = Files.createTempDirectory(null)
      val tempConfDir = Files.createDirectory(tempDir.resolve("conf"))

      val testResourceIn = getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
      val testResourceFile = tempConfDir.resolve("spark-defaults.conf")
      Files.copy(testResourceIn, testResourceFile)

      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map("SPARK_HOME" -> tempDir.toString)
        }
      }
      val sparkConf = sparkFetcher.sparkConf

      testResourceIn.close()
      Files.delete(testResourceFile)
      Files.delete(tempConfDir)
      Files.delete(tempDir)

      sparkConf.get("spark.yarn.historyServer.address") should be("jh1.grid.example.com:18080")
      sparkConf.get("spark.eventLog.enabled") should be("true")
      sparkConf.get("spark.eventLog.compress") should be("true")
      sparkConf.get("spark.eventLog.dir") should be("hdfs://nn1.grid.example.com:9000/logs/spark")
    }

    it("throws an exception if neither SPARK_CONF_DIR nor SPARK_HOME are set") {
      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map.empty[String, String]
        }
      }
      an[IllegalStateException] should be thrownBy { sparkFetcher.sparkConf }
    }
  }
}

object SparkFetcherTest {
  import JavaConverters._

  def newFakeFetcherConfigurationData(): FetcherConfigurationData =
    new FetcherConfigurationData(classOf[SparkFetcher].getName, new ApplicationType("SPARK"), Map.empty.asJava)

  def newFakeApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Date,
    endTime: Date
  ): ApplicationAttemptInfo = new ApplicationAttemptInfo(
    attemptId,
    startTime,
    endTime,
    sparkUser = "foo",
    completed = true
  )

  def newFakeSparkRestClient(
    appId: String,
    restDerivedData: Future[SparkRestDerivedData]
  )(
    implicit ec: ExecutionContext
  ): SparkRestClient = {
    val sparkRestClient = Mockito.mock(classOf[SparkRestClient])
    Mockito.when(sparkRestClient.fetchData(appId)).thenReturn(restDerivedData)
    sparkRestClient
  }

  def newFakeSparkLogClient(
    appId: String,
    attemptId: Option[String],
    logDerivedData: Future[SparkLogDerivedData]
  )(
    implicit ec: ExecutionContext
  ): SparkLogClient = {
    val sparkLogClient = Mockito.mock(classOf[SparkLogClient])
    Mockito.when(sparkLogClient.fetchData(appId, attemptId)).thenReturn(logDerivedData)
    sparkLogClient
  }
}
