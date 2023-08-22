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

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URI

import scala.concurrent.ExecutionContext

import com.linkedin.drelephant.util.{SparkUtils, SparkUtilsTest}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.scalatest.{AsyncFunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar
import org.xerial.snappy.SnappyOutputStream

class SparkLogClientTest extends AsyncFunSpec with Matchers with MockitoSugar {
  describe("SparkLogClient") {
    it("returns log-derived data") {
      val hadoopConfiguration = new Configuration(false)

      val sparkConf =
        new SparkConf()
          .set("spark.eventLog.dir", "webhdfs://nn1.grid.example.com:50070/logs/spark")
          .set("spark.eventLog.compress", "true")
          .set("spark.io.compression.codec", "snappy")

      val appId = "application_1"
      val attemptId = Some("1")

      val eventLogBytes = {
        val bout = new ByteArrayOutputStream()
        for {
          in <- resource.managed(getClass.getClassLoader.getResourceAsStream("spark_event_logs/event_log_2"))
          out <- resource.managed(new SnappyOutputStream(bout))
        } {
          IOUtils.copy(in, out)
        }
        bout.toByteArray
      }

      val sparkLogClient = new SparkLogClient(hadoopConfiguration, sparkConf, None) {
        override lazy val sparkUtils = SparkUtilsTest.newFakeSparkUtilsForEventLog(
          new URI("webhdfs://nn1.grid.example.com:50070"),
          new Path("/logs/spark"),
          new Path("application_1_1.snappy"),
          eventLogBytes
        )

        override protected def doAsPrivilegedAction[T](action: () => T): T = action()
      }

      sparkLogClient.fetchData(appId, attemptId).map { logDerivedData =>
        val expectedProperties = Map(
          "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.storage.memoryFraction" -> "0.3",
          "spark.driver.memory" -> "2G",
          "spark.executor.instances" -> "900",
          "spark.executor.memory" -> "1g",
          "spark.shuffle.memoryFraction" -> "0.5"
        )
        val actualProperties = logDerivedData.appConfigurationProperties
        actualProperties should be(expectedProperties)
      }
    }
  }
}
