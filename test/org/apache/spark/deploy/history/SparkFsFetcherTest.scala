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

import java.nio.file.{Files, StandardCopyOption}

import com.linkedin.drelephant.analysis.AnalyticJob
import com.linkedin.drelephant.configurations.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.linkedin.drelephant.util.HadoopUtilsTest
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FunSpec, Matchers}
import org.w3c.dom.Document

class SparkFsFetcherTest extends FunSpec with Matchers {
  import SparkFsFetcherTest._

  describe("SparkFsFetcher") {
    describe("constructor") {
      it("handles fetcher configurations with supplied values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest5.xml")
        fetcher.eventLogSizeLimitMb should be(50)
        fetcher.eventLogDir should be("/custom/configured")
      }

      it("handles fetcher configurations with empty values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest6.xml")
        fetcher.eventLogSizeLimitMb should be(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB)
        fetcher.eventLogDir should be(SparkFSFetcher.DEFAULT_EVENT_LOG_DIR)
      }

      it("handles fetcher configurations with missing values") {
        val fetcher = newFetcher("configurations/fetcher/FetcherConfTest7.xml")
        fetcher.eventLogSizeLimitMb should be(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB)
        fetcher.eventLogDir should be(SparkFSFetcher.DEFAULT_EVENT_LOG_DIR)
      }
    }

    describe(".nameNodeAddress") {
      it("returns the first fetcher configuration's name node that is provided and active") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest8.xml")
        val conf = HadoopUtilsTest.newConfiguration(loadDefaults = true)
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active")),
          ("sample-ha3.grid.example.com", ("sample-ha3.grid.example.com", "standby")),
          ("sample-ha4.grid.example.com", ("sample-ha4.grid.example.com", "active"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample-ha4.grid.example.com:50070"))
      }

      it("returns the first active Hadoop configuration's HA name node if the fetcher configuration provides no active") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest8.xml")
        val conf = HadoopUtilsTest.newConfiguration(loadDefaults = true)
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active")),
          ("sample-ha3.grid.example.com", ("sample-ha3.grid.example.com", "standby")),
          ("sample-ha4.grid.example.com", ("sample-ha4.grid.example.com", "standby"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample-ha2.grid.example.com:50070"))
      }

      it("returns the first active Hadoop configuration's HA name node if the fetcher configuration provides nothing") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest6.xml")
        val conf = HadoopUtilsTest.newConfiguration(loadDefaults = true)
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample-ha2.grid.example.com:50070"))
      }

      it("returns the Hadoop configuration's default name node as a last resort") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest6.xml")
        val conf = HadoopUtilsTest.newConfiguration(loadDefaults = true)
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "standby"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample.grid.example.com:50070"))
      }
    }

    describe(".fetchData") {
      it("returns the data collected from the Spark event log for the given analytic job") {
        val in = getClass.getClassLoader.getResourceAsStream("spark_event_logs/event_log_1")
        val tempPath = Files.createTempFile("event_log_1", "")
        Files.copy(in, tempPath, StandardCopyOption.REPLACE_EXISTING)
        in.close()

        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest7.xml")
        val fetcher = new SparkFSFetcher(fetcherConfigurationData) {
          override protected lazy val _fs = FileSystem.getLocal(new Configuration())

          override protected def doAsPrivilegedAction[T](action: () => T): T = action()

          override protected def eventLogContext(appId: String): SparkFSFetcher.EventLogContext =
            SparkFSFetcher.EventLogContext(new Path(tempPath.toString), usingDefaultEventLog = true)
        }
        val analyticJob = new AnalyticJob().setAppId("foo")

        val dataCollection = fetcher.fetchData(analyticJob)
        val jobProgressData = Option(dataCollection.getJobProgressData())
        jobProgressData should not be None
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
