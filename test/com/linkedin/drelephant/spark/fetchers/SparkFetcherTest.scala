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

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.util.Date

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, Future}

import com.google.common.io.Files
import com.linkedin.drelephant.analysis.{AnalyticJob, ApplicationType}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.spark.data.{SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfo, ApplicationInfo}
import com.linkedin.drelephant.util.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.mockito.Mockito
import org.scalatest.{FunSpec, Matchers}

class SparkFetcherTest extends FunSpec with Matchers {
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
      val tempDir = Files.createTempDir()

      val testResourceIn = getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
      val testResourceFile = new File(tempDir, "spark-defaults.conf")
      val testResourceOut = new FileOutputStream(testResourceFile)
      managedCopyInputStreamToOutputStream(testResourceIn, testResourceOut)

      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() {
          override val defaultEnv = Map("SPARK_CONF_DIR" -> tempDir.toString)
        }
      }
      val sparkConf = sparkFetcher.sparkConf

      tempDir.delete()

      sparkConf.get("spark.yarn.historyServer.address") should be("jh1.grid.example.com:18080")
      sparkConf.get("spark.eventLog.enabled") should be("true")
      sparkConf.get("spark.eventLog.compress") should be("true")
      sparkConf.get("spark.eventLog.dir") should be("hdfs://nn1.grid.example.com:9000/logs/spark")
    }

    it("gets its SparkConf when SPARK_HOME is set") {
      val tempDir = Files.createTempDir()
      val tempConfDir = new File(tempDir, "conf")
      tempConfDir.mkdir()

      val testResourceIn = getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
      val testResourceFile = new File(tempConfDir, "spark-defaults.conf")
      val testResourceOut = new FileOutputStream(testResourceFile)
      managedCopyInputStreamToOutputStream(testResourceIn, testResourceOut)

      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() {
          override val defaultEnv = Map("SPARK_HOME" -> tempDir.toString)
        }
      }
      val sparkConf = sparkFetcher.sparkConf

      tempDir.delete()

      sparkConf.get("spark.yarn.historyServer.address") should be("jh1.grid.example.com:18080")
      sparkConf.get("spark.eventLog.enabled") should be("true")
      sparkConf.get("spark.eventLog.compress") should be("true")
      sparkConf.get("spark.eventLog.dir") should be("hdfs://nn1.grid.example.com:9000/logs/spark")
    }

    it("throws an exception if neither SPARK_CONF_DIR nor SPARK_HOME are set") {
      val fetcherConfigurationData = newFakeFetcherConfigurationData()
      val sparkFetcher = new SparkFetcher(fetcherConfigurationData) {
        override lazy val sparkUtils = new SparkUtils() { override val defaultEnv = Map.empty[String, String] }
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

  def managedCopyInputStreamToOutputStream(in: => InputStream, out: => OutputStream): Unit = {
    for {
      input <- resource.managed(in)
      output <- resource.managed(out)
    } {
      val buffer = new Array[Byte](512)
      def read(): Unit = input.read(buffer) match {
        case -1 => ()
        case bytesRead => {
          output.write(buffer, 0, bytesRead)
          read()
        }
      }
      read()
    }
  }
}
