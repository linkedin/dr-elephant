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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI

import com.linkedin.drelephant.analysis.AnalyticJob
import com.linkedin.drelephant.configurations.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.linkedin.drelephant.util.{SparkUtils, SparkUtilsTest}
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import org.w3c.dom.Document
import org.xerial.snappy.SnappyOutputStream

class SparkFsFetcherTest extends FunSpec with Matchers with MockitoSugar {
  import SparkFsFetcherTest._

  describe("SparkFsFetcher") {
    describe("constructor") {
      it("handles fetcher configurations with supplied values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest5.xml")
        fetcher.eventLogSizeLimitMb should be(50)
      }

      it("handles fetcher configurations with empty values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest6.xml")
        fetcher.eventLogSizeLimitMb should be(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB)
      }

      it("handles fetcher configurations with missing values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest7.xml")
        fetcher.eventLogSizeLimitMb should be(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB)
      }
    }

    describe(".fetchData") {
      it("returns the data collected from the Spark event log for the given analytic job") {
        val eventLogBytes = {
          val eventLog =
            """{"Event":"SparkListenerApplicationStart","App Name":"app","App ID":"application_1","Timestamp":1,"User":"foo"}"""
          val bout = new ByteArrayOutputStream()
          for {
            in <- resource.managed(new ByteArrayInputStream(eventLog.getBytes("UTF-8")))
            out <- resource.managed(new SnappyOutputStream(bout))
          } {
            IOUtils.copy(in, out)
          }
          bout.toByteArray
        }

        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest7.xml")
        val fetcher = new SparkFSFetcher(fetcherConfigurationData) {
          override lazy val hadoopConfiguration = new Configuration(false)

          override lazy val sparkConf =
            new SparkConf()
              .set("spark.eventLog.dir", "webhdfs://nn1.grid.example.com:50070/logs/spark")
              .set("spark.eventLog.compress", "true")
              .set("spark.io.compression.codec", "snappy")

          override lazy val sparkUtils = SparkUtilsTest.newFakeSparkUtilsForEventLog(
            new URI("webhdfs://nn1.grid.example.com:50070"),
            new Path("/logs/spark"),
            new Path("application_1_1.snappy"),
            eventLogBytes
          )

          override protected def doAsPrivilegedAction[T](action: () => T): T = action()
        }
        val analyticJob = new AnalyticJob().setAppId("application_1")

        val data = fetcher.fetchData(analyticJob)
        data.getAppId should be("application_1")

        val generalData = data.getGeneralData
        generalData.getApplicationId should be("application_1")
        generalData.getApplicationName should be("app")
        generalData.getSparkUser should be("foo")
      }
    }
  }
}

object SparkFsFetcherTest {
  def newFetcher(confResourcePath: String): SparkFSFetcher = {
    val fetcherConfData = newFetcherConfigurationData(confResourcePath)
    val fetcherClass = getClass.getClassLoader.loadClass(fetcherConfData.getClassName)
    fetcherClass.getConstructor(classOf[FetcherConfigurationData]).newInstance(fetcherConfData).asInstanceOf[SparkFSFetcher]
  }

  def newFetcherConfigurationData(confResourcePath: String): FetcherConfigurationData = {
    val document = parseDocument(confResourcePath)
    val fetcherConf = new FetcherConfiguration(document.getDocumentElement())
    fetcherConf.getFetchersConfigurationData().get(0)
  }

  def parseDocument(resourcePath: String): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    val builder = factory.newDocumentBuilder()
    builder.parse(getClass.getClassLoader.getResourceAsStream(resourcePath))
  }
}
