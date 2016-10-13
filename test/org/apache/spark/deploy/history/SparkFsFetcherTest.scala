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

import com.linkedin.drelephant.configurations.fetcher.{FetcherConfiguration, FetcherConfigurationData}
import com.linkedin.drelephant.util.HadoopUtilsTest
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.hadoop.conf.Configuration
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
      it("returns the Hadoop-configured name node address by default") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest6.xml")
        val conf = new Configuration()
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample-ha2.grid.example.com:50070"))
      }

      it("returns the fetcher configuration name node address when supplied") {
        val fetcherConfigurationData = newFetcherConfigurationData("configurations/fetcher/FetcherConfTest8.xml")
        val conf = new Configuration()
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active")),
          ("sample-ha3.grid.example.com", ("sample-ha3.grid.example.com", "standby")),
          ("sample-ha4.grid.example.com", ("sample-ha4.grid.example.com", "active"))
        )
        val nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfigurationData, conf, hadoopUtils)
        nameNode should be(Some("sample-ha4.grid.example.com:50070"))
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
