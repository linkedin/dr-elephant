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

package com.linkedin.drelephant.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path, PathFilter, PositionedReadable}
import org.apache.hadoop.io.compress.CompressionInputStream
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.io.LZ4CompressionCodec
import org.mockito.BDDMockito
import org.scalatest.{FunSpec, OptionValues}
import org.scalatest.mockito.MockitoSugar
import org.xerial.snappy.SnappyOutputStream


class SparkUtilsTest extends FunSpec with org.scalatest.Matchers with OptionValues with MockitoSugar {
  describe("SparkUtils") {
    describe(".fileSystemAndPathForEventLogDir") {
      it("returns a filesystem + path based on uri from fetcherConfg") {
        val hadoopConfiguration = new Configuration(false)
        val sparkConf = new SparkConf()
        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration,
          sparkConf,
          Some("webhdfs://nn1.grid.example.com:50070/logs/spark"))
        fs.getUri.toString should be("webhdfs://nn1.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("returns a webhdfs filesystem + path based on spark.eventLog.dir when it is a webhdfs URL") {
        val hadoopConfiguration = new Configuration(false)
        val sparkConf = new SparkConf().set("spark.eventLog.dir", "webhdfs://nn1.grid.example.com:50070/logs/spark")
        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)
        fs.getUri.toString should be("webhdfs://nn1.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("returns a webhdfs filesystem + path based on spark.eventLog.dir when it is an hdfs URL") {
        val hadoopConfiguration = new Configuration(false)
        val sparkConf = new SparkConf().set("spark.eventLog.dir", "hdfs://nn1.grid.example.com:9000/logs/spark")
        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)
        fs.getUri.toString should be("webhdfs://nn1.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("returns a webhdfs filesystem + path based on dfs.nameservices and spark.eventLog.dir when the latter is a path and the dfs.nameservices is configured and available") {
        val hadoopConfiguration = new Configuration(false)
        hadoopConfiguration.set("dfs.nameservices", "sample")
        hadoopConfiguration.set("dfs.ha.namenodes.sample", "ha1,ha2")
        hadoopConfiguration.set("dfs.namenode.http-address.sample.ha1", "sample-ha1.grid.example.com:50070")
        hadoopConfiguration.set("dfs.namenode.http-address.sample.ha2", "sample-ha2.grid.example.com:50070")

        val sparkConf = new SparkConf().set("spark.eventLog.dir", "/logs/spark")

        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]

          override lazy val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
            ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
            ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
          )

          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)
        fs.getUri.toString should be("webhdfs://sample-ha2.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("returns a webhdfs filesystem + path based on dfs.nameservices and spark.eventLog.dir when the latter is a path and the dfs.nameservices is configured but unavailable") {
        val hadoopConfiguration = new Configuration(false)
        hadoopConfiguration.set("dfs.nameservices", "sample")
        hadoopConfiguration.set("dfs.ha.namenodes.sample", "ha1,ha2")
        hadoopConfiguration.set("dfs.namenode.http-address.sample.ha1", "sample-ha1.grid.example.com:50070")
        hadoopConfiguration.set("dfs.namenode.http-address.sample.ha2", "sample-ha2.grid.example.com:50070")
        hadoopConfiguration.set("dfs.namenode.http-address", "sample.grid.example.com:50070")

        val sparkConf = new SparkConf().set("spark.eventLog.dir", "/logs/spark")

        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]

          override lazy val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
            ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
            ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "standby"))
          )

          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)
        fs.getUri.toString should be("webhdfs://sample.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("returns a webhdfs filesystem + path based on dfs.namenode.http-address and spark.eventLog.dir when the latter is a path and dfs.nameservices is not configured") {
        val hadoopConfiguration = new Configuration(false)
        hadoopConfiguration.set("dfs.namenode.http-address", "sample.grid.example.com:50070")

        val sparkConf = new SparkConf().set("spark.eventLog.dir", "/logs/spark")

        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]

          override lazy val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
            ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
            ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
          )

          override lazy val defaultEnv = Map.empty[String, String]
        }

        val (fs, path) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)
        fs.getUri.toString should be("webhdfs://sample.grid.example.com:50070")
        path should be(new Path("/logs/spark"))
      }

      it("throws an exception when spark.eventLog.dir is a path and no namenode is configured at all") {
        val hadoopConfiguration = new Configuration(false)

        val sparkConf = new SparkConf().set("spark.eventLog.dir", "/logs/spark")

        val sparkUtils = new SparkUtils {
          override lazy val logger = mock[Logger]
          override lazy val hadoopUtils = mock[HadoopUtils]
          override lazy val defaultEnv = Map.empty[String, String]
        }

        an[Exception] should be thrownBy { sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None) }
      }
    }

    describe(".pathAndCodecforEventLog") {
      it("returns the path and codec for the event log, given the base path and app/attempt information") {
        val hadoopConfiguration = new Configuration(false)

        val sparkConf =
          new SparkConf()
            .set("spark.eventLog.dir", "/logs/spark")
            .set("spark.eventLog.compress", "true")

        val sparkUtils = SparkUtilsTest.newFakeSparkUtilsForEventLog(
          new URI("webhdfs://nn1.grid.example.com:50070"),
          new Path("/logs/spark"),
          new Path("application_1_1.lz4"),
          Array.empty[Byte]
        )

        val (fs, basePath) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)

        val (path, codec) =
          sparkUtils.pathAndCodecforEventLog(sparkConf: SparkConf, fs: FileSystem, basePath: Path, "application_1", Some("1"))

        path should be(new Path("webhdfs://nn1.grid.example.com:50070/logs/spark/application_1_1.lz4"))
        codec.value should be(a[LZ4CompressionCodec])
      }
      it("returns the path and codec for the event log, given the base path and appid. Extracts attempt and codec from path") {
        val hadoopConfiguration = new Configuration(false)

        val sparkConf =
          new SparkConf()
            .set("spark.eventLog.dir", "/logs/spark")
            .set("spark.eventLog.compress", "true")

        val sparkUtils = SparkUtilsTest.newFakeSparkUtilsForEventLog(
          new URI("webhdfs://nn1.grid.example.com:50070"),
          new Path("/logs/spark"),
          new Path("application_1_1.lz4"),
          Array.empty[Byte]
        )

        val (fs, basePath) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)

        val (path, codec) =
          sparkUtils.pathAndCodecforEventLog(sparkConf: SparkConf, fs: FileSystem, basePath: Path, "application_1", None)

        path should be(new Path("webhdfs://nn1.grid.example.com:50070/logs/spark/application_1_1.lz4"))
        codec.value should be(a[LZ4CompressionCodec])
      }
    }

    describe(".withEventLog") {
      it("loans the input stream for the event log") {
        val expectedLog =
          """{"Event":"SparkListenerApplicationStart","App Name":"app","App ID":"application_1","Timestamp":1,"User":"foo"}"""

        val eventLogBytes = {
          val bout = new ByteArrayOutputStream()
          for {
            in <- resource.managed(new ByteArrayInputStream(expectedLog.getBytes("UTF-8")))
            out <- resource.managed(new SnappyOutputStream(bout))
          } {
            IOUtils.copy(in, out)
          }
          bout.toByteArray
        }

        val hadoopConfiguration = new Configuration(false)

        val sparkConf =
          new SparkConf()
            .set("spark.eventLog.dir", "/logs/spark")
            .set("spark.eventLog.compress", "true")

        val sparkUtils = SparkUtilsTest.newFakeSparkUtilsForEventLog(
          new URI("webhdfs://nn1.grid.example.com:50070"),
          new Path("/logs/spark"),
          new Path("application_1_1.snappy"),
          eventLogBytes
        )

        val (fs, basePath) = sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, None)

        val (path, codec) =
          sparkUtils.pathAndCodecforEventLog(sparkConf: SparkConf, fs: FileSystem, basePath: Path, "application_1", None)

        sparkUtils.withEventLog(fs, path, codec) { in =>
          val bout = new ByteArrayOutputStream()
          IOUtils.copy(in, bout)

          val actualLog = new String(bout.toByteArray, "UTF-8")
          actualLog should be(expectedLog)
        }
      }
    }
  }
}

object SparkUtilsTest extends MockitoSugar {
  def newFakeSparkUtilsForEventLog(
    fileSystemUri: URI,
    basePath: Path,
    filename: Path,
    bytes: Array[Byte]
  ): SparkUtils = new SparkUtils() {
    override lazy val logger = mock[Logger]
    override lazy val hadoopUtils = mock[HadoopUtils]
    override lazy val defaultEnv = Map.empty[String, String]

    override def fileSystemAndPathForEventLogDir(
      hadoopConfiguration: Configuration,
      sparkConf: SparkConf,
      uriFromFetcherConf: Option[String]
    ): (FileSystem, Path) = {
      val fs = mock[FileSystem]
      val expectedPath = new Path(new Path(fileSystemUri), new Path(basePath, filename))
      val expectedFileStatus = {
        val fileStatus = mock[FileStatus]
        BDDMockito.given(fileStatus.getLen).willReturn(bytes.length.toLong)
        BDDMockito.given(fileStatus.getPath()).willReturn(expectedPath)
        fileStatus
      }
      val expectedStatusArray = Array(expectedFileStatus)

      val filter = new PathFilter() {
        override def accept(file: Path): Boolean = {
          file.getName().startsWith("mockAppId");
        }
      }

      BDDMockito.given(fs.getUri).willReturn(fileSystemUri)
      BDDMockito.given(fs.exists(expectedPath)).willReturn(true)
      BDDMockito.given(fs.getFileStatus(expectedPath)).willReturn(expectedFileStatus)
      BDDMockito.given(fs.listStatus(org.mockito.Matchers.refEq(new Path(new Path(fileSystemUri), basePath)),
        org.mockito.Matchers.any(filter.getClass))).
        willReturn(expectedStatusArray)
      BDDMockito.given(fs.open(expectedPath)).willReturn(
        new FSDataInputStream(new FakeCompressionInputStream(new ByteArrayInputStream(bytes)))
      )
      (fs, basePath)
    }
  }

  class FakeCompressionInputStream(in: InputStream) extends CompressionInputStream(in) with PositionedReadable {
    override def read(): Int = in.read()
    override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
    override def read(pos: Long, buffer: Array[Byte], off: Int, len: Int): Int = ???
    override def readFully(pos: Long, buffer: Array[Byte], off: Int, len: Int): Unit = ???
    override def readFully(pos: Long, buffer: Array[Byte]): Unit = ???
    override def resetState(): Unit = ???
  }
}
