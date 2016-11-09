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

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileOutputStream}

import com.linkedin.drelephant.analysis.AnalyticJob
import com.linkedin.drelephant.configurations.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.linkedin.drelephant.util.HadoopUtilsTest
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.mockito.BDDMockito
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.w3c.dom.Document

class SparkFsFetcherTest extends FunSpec with Matchers with MockitoSugar {
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
        val tempFile = copyResourceToTempFile("spark_event_logs/event_log_1")

        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest7.xml")
        val fetcher = new SparkFSFetcher(fetcherConfigurationData) {
          override protected lazy val _fs = FileSystem.getLocal(new Configuration())

          override protected def doAsPrivilegedAction[T](action: () => T): T = action()

          override private[history] def eventLogContext(appId: String): SparkFSFetcher.EventLogContext =
            SparkFSFetcher.EventLogContext(new Path(tempFile.getPath), usingDefaultEventLog = true)
        }
        val analyticJob = new AnalyticJob().setAppId("foo")

        val dataCollection = fetcher.fetchData(analyticJob)
        val jobProgressData = Option(dataCollection.getJobProgressData())
        jobProgressData should not be None
      }
    }

    describe(".eventLogContext") {
      it("returns the default log path and true when the log path exists") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest7.xml")
        val fetcher = new SparkFSFetcher(fetcherConfigurationData) {
          override protected lazy val _logDir = "/logs"

          override protected lazy val _fs = {
            val fs = mock[FileSystem]
            BDDMockito.given(fs.exists(new Path(_logDir, "foo_1.snappy"))).willReturn(true)
            fs
          }

          override protected def doAsPrivilegedAction[T](action: () => T): T = action()
        }

        fetcher.eventLogContext("foo") should be(SparkFSFetcher.EventLogContext(new Path("/logs/foo_1.snappy"), true))
      }

      it("returns the legacy log path and false when the default log path doesn't exist") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest7.xml")
        val fetcher = new SparkFSFetcher(fetcherConfigurationData) {
          override protected lazy val _logDir = "/logs"

          override protected lazy val _fs = {
            val fs = mock[FileSystem]
            BDDMockito.given(fs.exists(new Path(_logDir, "foo_1.snappy"))).willReturn(false)
            fs
          }

          override protected def doAsPrivilegedAction[T](action: () => T): T = action()
        }

        fetcher.eventLogContext("foo") should be(SparkFSFetcher.EventLogContext(new Path("/logs/foo"), false))
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

  def copyResourceToTempFile(resourcePath: String): File = {
    // Unfortunately we can't use java.nio to do any of this, since we want to preserve compatibility with Java 6.
    val in = new BufferedInputStream(getClass.getClassLoader.getResourceAsStream(resourcePath))
    val tempPath = File.createTempFile("temp", "")
    val out = new BufferedOutputStream(new FileOutputStream(tempPath))
    val buf = new Array[Byte](4096)
    var done = false
    var bytesRead = 0
    while (!done) {
      bytesRead = in.read(buf)
      if (bytesRead < 0) {
        done = true
      } else {
        out.write(buf, 0, bytesRead)
      }
    }
    out.close()
    in.close()
    tempPath
  }
}
