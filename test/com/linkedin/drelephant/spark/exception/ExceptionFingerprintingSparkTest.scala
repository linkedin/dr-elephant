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

package com.linkedin.drelephant.spark.exception

import java.io.{BufferedReader, File, FileReader, StringReader}
import java.text.SimpleDateFormat

import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import com.linkedin.drelephant.spark.exception.ExceptionFingerprintingSparkUtilities._
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.apache.hadoop.conf.Configuration
import java.util

import com.linkedin.drelephant.ElephantContext
import com.linkedin.drelephant.exceptions.core.{ExceptionFingerprintingFactory, ExceptionFingerprintingSpark, RegexRule}
import common.TestConstants._
import play.Application
import play.GlobalSettings
import play.test.FakeApplication
import org.apache.hadoop.conf.Configuration
import play.test.Helpers._
import com.linkedin.drelephant.exceptions.util.Constant._
import com.linkedin.drelephant.exceptions.util.{Constant, ExceptionInfo}
import com.linkedin.drelephant.exceptions.util.ExceptionUtils._

class ExceptionFingerprintingSparkTest extends FunSpec with Matchers with BeforeAndAfter {
  private val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  val dbConn = new util.HashMap[String, String]
  dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE)
  dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE)
  dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE)
  dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE)

  before {
    ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf);
  }

  val gs = new GlobalSettings() {
    override def onStart(app: Application): Unit = {
      //LOGGER.info("Starting FakeApplication")
    }
  }
  val fakeApp = fakeApplication(dbConn, gs)

  describe(".apply") {
    it("check for user enabled exception") {
      val stage = createStage(1, StageStatus.FAILED, Some("array issues"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExecutionEngineType.SPARK, data)
      val className = checkTye(exceptionFingerprinting)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042", "Mock diagnostic message")
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      val classificationValue = exceptionFingerprinting.classifyException(exceptionInfoList)
      // Test to check for log source Information
      val logSourceInformation = exceptionFingerprinting.getExceptionLogSourceInformation
      className should be("ExceptionFingerprintingSpark")
      exceptionInfoList.size() should be(2)
      classificationValue.name() should be("USER_ENABLED")
      exceptionInfoList.get(0).getExceptionStackTrace should be("Mock diagnostic message")
      logSourceInformation.containsKey("DRIVER") should be(true)
      logSourceInformation.get("DRIVER") should be("http://0.0.0.0:19888/jobhistory/nmlogs/hostname:0" +
        "/container_e24_1547063162911_185371_01_000001/container_e24_1547063162911_185371_01_000001/dssadmin/stderr/?start=0")
    }
    it("check for auto tuning  enabled exception") {
      val stage = createStage(1, StageStatus.FAILED, Some("java.lang.OutOfMemoryError: Exception thrown in " +
        "awaitResult: \n  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)\n  " +
        "at org.apache.spark.deploy.yarn.ApplicationMaster.runDriver(ApplicationMaster.scala:401)"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExecutionEngineType.SPARK, data)
      val className = checkTye(exceptionFingerprinting)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042")
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      val classificationValue = exceptionFingerprinting.classifyException(exceptionInfoList)
      className should be("ExceptionFingerprintingSpark")
      exceptionInfoList.size() should be(2)
      classificationValue.name() should be("AUTOTUNING_ENABLED")
    }
    it("check for build URL for query driver logs ") {
      val sparkExceptionFingerPrinting = new ExceptionFingerprintingSpark()
      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042")
      val exceptionInfoList = sparkExceptionFingerPrinting.processRawData(analyticJob)
      val queryURL = sparkExceptionFingerPrinting.buildURLtoQuery()
      queryURL should be("http://0.0.0.0:19888/jobhistory/nmlogs/hostname:0/container_e24_1547063162911_185371_01_000001" +
        "/container_e24_1547063162911_185371_01_000001/dssadmin")
    }
    it("check for eligibilty of applying exception fingerprinting ") {
      val analyticJob = getAnalyticalJob(true,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042")
      analyticJob.getAppType().getName should be("SPARK")
      val isApplicationApplied = analyticJob.applyExceptionFingerprinting(null, null)
      isApplicationApplied should be(false)
      analyticJob.setSucceeded(false)
      val isApplicationAppliedNext = analyticJob.applyExceptionFingerprinting(null, null)
      isApplicationAppliedNext should be(true)
    }
    it("check for complete exception fingerprinting ") {
      val stage = createStage(1, StageStatus.FAILED, Some("java.lang.OutOfMemoryError: Exception thrown in " +
        "awaitResult: \n  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)\n  " +
        "at org.apache.spark.deploy.yarn.ApplicationMaster.runDriver(ApplicationMaster.scala:401)"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042")
      running(testServer(TEST_SERVER_PORT, fakeApp), new ExceptionFingerprintingRunnerTest(data, analyticJob))
    }

    it("check for exception regex ") {
      val dataContainsException = Array("java.io.FileNotFoundException: File /jobs/emailopt/",
        "java.lang.OutOfMemoryError: Java heap space",
        "Reason: Container killed by YARN for", "java.lang.OutOfMemoryError: Exception thrown in awaitResult:"
          + "  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)")
      val dataContainsNoException = Array("SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]")
      for (data <- dataContainsException) {
        isExceptionContains(data) should be(true)
      }
      for (data <- dataContainsNoException) {
        isExceptionContains(data) should be(false)
      }

      val exceptionOutOfMemory = new ExceptionInfo(1,
        "java.lang.OutOfMemoryError: Exception thrown in awaitResult:",
        "  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)",
        ExceptionInfo.ExceptionSource.EXECUTOR, 1, "")

      val exceptionVirtualMemory = new ExceptionInfo(1,
        "[pid=116086,containerID=container_1535113754342_0003_01_000002] " +
          "is running beyond virtual memory limits. Current usage: 106.2 MB of 1 GB " +
          "physical memory used; 5.8 GB of 2.1 GB virtual memory used. Killing container",
        "",
        ExceptionInfo.ExceptionSource.EXECUTOR, 1, "")

      val exceptionExitCode = new ExceptionInfo(1,
        "Container killed by the ApplicationMaster. " +
          "Container killed on request. Exit code is " +
          "103 Container exited with a non-zero exit code 103",
        "",
        ExceptionInfo.ExceptionSource.EXECUTOR, 1, "")

      val exceptionNonAutoTuningFault = new ExceptionInfo(1,
        "java.io.FileNotFoundException: File webhdfs://nn1.grid.example.com:50070/logs/spark/application_1.lz4 does not exist.",
        "at com.linkedin.drelephant.util.SparkUtils$class.com$linkedin$drelephant$util$SparkUtils$$openEventLog(SparkUtils.scala:313)",
        ExceptionInfo.ExceptionSource.EXECUTOR, 1, "")

      val exceptionList = new java.util.ArrayList[ExceptionInfo]()
      val rule = new RegexRule()
      exceptionList.add(exceptionOutOfMemory)
      rule.logic(exceptionList) should be(Constant.LogClass.AUTOTUNING_ENABLED)
      exceptionList.clear()

      exceptionList.add(exceptionVirtualMemory)
      rule.logic(exceptionList) should be(Constant.LogClass.AUTOTUNING_ENABLED)
      exceptionList.clear()

      exceptionList.add(exceptionVirtualMemory)
      rule.logic(exceptionList) should be(Constant.LogClass.AUTOTUNING_ENABLED)
      exceptionList.clear()

      exceptionList.add(exceptionExitCode)
      rule.logic(exceptionList) should be(Constant.LogClass.AUTOTUNING_ENABLED)
      exceptionList.clear()

      exceptionList.add(exceptionNonAutoTuningFault)
      rule.logic(exceptionList) should be(Constant.LogClass.USER_ENABLED)
      exceptionList.clear()
    }

    it("check for removal of black listed exceptions") {
      val stage1 = createStage(1, StageStatus.FAILED, Some("java.lang.OutOfMemoryError: Exception thrown in awaitResult:"), "details")
      val stage2 = createStage(2, StageStatus.FAILED, Some("-XX:OnOutOfMemoryError='kill %p'"), "details")
      val stages = Seq(stage1, stage2)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExecutionEngineType.SPARK, data)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "hostname:8042")
      val blackListedPatterns = Array("ABCD")
      ConfigurationBuilder.BLACK_LISTED_EXCEPTION_PATTERN.setValue(blackListedPatterns)
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      exceptionInfoList.size() should be(3)
      exceptionInfoList.get(0).getExceptionStackTrace should be("Not Available")
      val blackListedPatternsEnhanced = blackListedPatterns :+ "-XX:OnOutOfMemoryError='kill %p'"
      ConfigurationBuilder.BLACK_LISTED_EXCEPTION_PATTERN.setValue(blackListedPatternsEnhanced)
      exceptionFingerprinting.processRawData(analyticJob).size() should be(2)
    }

    it("check for driver log processing for Exception fingerprinting spark") {
      val exceptionFingerPrintingSpark = new ExceptionFingerprintingSpark()

      var driverLogs = " Showing 50000 bytes of 77821039 total. Click \n            <a href=\"/jobhistory/logs/" +
        "ltx1-hcl11887.grid.linkedin.com:8041/container_e99_1574361432315_150107_01_000005" +
        "/container_e99_1574361432315_150107_01_000005/metrics/stderr/?start=0\">" +
        "here</a>\n             for the full log.\n          <pre>re\n\n)\n19/11/22 05:21:40 INFO scheduler." +
      "\n20/02/07 06:10:23 ERROR yarn.ApplicationMaster: User class threw exception: org.apa" +
        "che.spark.sql.AnalysisException: cannot resolve '`src_session_tokenized`' given " +
        "input columns: [value, id, target, count];;\n'Project [value#6, id#8L, target#34, count#35L, " +
        "rank() windowspecdefinition('src_session_tokenized, count#35L DESC NULLS LAST, " +
        "specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#61]\n+- AnalysisBarrier\n      " +
        "+- Project [value#6, id#8L, suggestionInfo#27._1 AS target#34, suggestionInfo#27._2 AS count#35L]\n         +- " +
        "Project [value#6, id#8L, src_session_tokenized#0, suggestions#1, counts#2, suggestionInfo#27]\n            +- " +
        "Generate explode(UDF(suggestions#1, counts#2)), false, [suggestionInfo#27]\n               +- Join Inner, " +
        "(src_session_tokenized#0 = value#6)\n                  :- Project [value#6, monotonically_" +
        "increasing_id() AS id#8L]\n                  :  +- Relation[value#6] text\n                  +- " +
        "Relation[src_session_tokenized#0,suggestions#1,counts#2] avro\n\norg.apache.spark.sql." +
        "AnalysisException: cannot resolve '`src_session_tokenized`' given input columns: " +
        "[value, id, target, count];;\n'Project [value#6, id#8L, target#34, count#35L, rank() " +
        "windowspecdefinition('src_session_tokenized, count#35L DESC NULLS LAST, " +
        "specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#61]\n+- AnalysisBarrier\n      +- " +
        "Project [value#6, id#8L, suggestionInfo#27._1 AS target#34, suggestionInfo#27._2 AS count#35L]\n         +- Project " +
        "[value#6, id#8L, src_session_tokenized#0, suggestions#1, counts#2, suggestionInfo#27]\n            +- Generate " +
        "explode(UDF(suggestions#1, counts#2)), false, [suggestionInfo#27]\n               +- Join " +
        "Inner, (src_session_tokenized#0 = value#6)\n                  :- Project " +
        "[value#6, monotonically_increasing_id() AS id#8L]\n                  :  +- Relation[value#6] " +
        "text\n                  +- Relation[src_session_tokenized#0,suggestions#1,counts#2] avro\n\n\tat " +
        "org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)\n\tat " +
        "org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2." +
        "applyOrElse(CheckAnalysis.scala:88)\n\tat org.apache.spark.sql.catalyst.analysis." +
        "CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:85)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:286)\n\tat " +
        "org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4$$anonfun$apply$11.apply(TreeNode.scala:335)\n\tat " +
        "scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)\n\tat " +
        "scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)\n\tat " +
        "scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)\n\tat " +
        "scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)\n\tat " +
        "scala.collection.TraversableLike$class.map(TraversableLike.scala:234)\n\tat " +
        "scala.collection.AbstractTraversable.map(Traversable.scala:104)"
      val exceptionInfo = new util.ArrayList[ExceptionInfo]();
      var reader = new BufferedReader(new StringReader(driverLogs))
      exceptionFingerPrintingSpark.convertInputStreamToExceptionList(reader, exceptionInfo, "http://localhost/metrics/stderr/?start=77771039")
      exceptionInfo.size() should be(2)
      val exception = exceptionInfo.get(0)
      exceptionInfo.get(1).getExceptionName should be("org.apache.spark.sql.AnalysisException: " +
        "cannot resolve '`src_session_tokenized`' given input columns: [value, id, target, count];;")
      exceptionInfo.get(0).getExceptionName should be("20/02/07 06:10:23 ERROR yarn.ApplicationMaster: " +
        "User class threw exception: org.apache.spark.sql.AnalysisException: cannot resolve '`src_session_tokenized`' given input columns: [value, id, target, count];;")

      exceptionInfo.get(1).getExceptionStackTrace should startWith("org.apache.spark.sql.AnalysisException: cannot resolve")
      exceptionInfo.get(0).getExceptionStackTrace should startWith("20/02/07 06:10:23 ERROR yarn.ApplicationMaster")
      exceptionInfo.get(0).getExceptionTrackingURL should be("http://localhost/metrics/stderr/?start=40")
      exceptionInfo.get(1).getExceptionTrackingURL should be("http://localhost/metrics/stderr/?start=1051")

      exceptionInfo.get(1).getWeightOfException should be(2)
      reader.close()



      driverLogs = "exception"
      reader = new BufferedReader(new StringReader(driverLogs))
      exceptionInfo.clear()
      exceptionFingerPrintingSpark.convertInputStreamToExceptionList(reader, exceptionInfo, "http://localhost/metrics/stderr/?start=77771039")
      exceptionInfo.size() should be(0)

    }


    it("check for start index based on log length") {
      val exceptionFingerPrintingSpark = new ExceptionFingerprintingSpark()

      /**
        * If value is less than first threshold .
        */
      exceptionFingerPrintingSpark.getStartIndexOfDriverLogs(260059L) should be(3000L)

      /**
        * If value is greater than all the threshold values
        */
      exceptionFingerPrintingSpark.getStartIndexOfDriverLogs(1111111111111L) should be(1111111111111L - 50000L)

      /**
        * If unable to parse log length then use default approach
        */
      exceptionFingerPrintingSpark.getStartIndexOfDriverLogs(0) should be(0)

      /**
        * If the log length is greater than second threshold , then have 95 % of it.
        */
      exceptionFingerPrintingSpark.getStartIndexOfDriverLogs(260070) should be(247066)
    }
    it("check for exception utils functionality of configuration builder") {
      /**
        * Check for default values
        */
      ConfigurationBuilder.FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue should be(260059L)
      ConfigurationBuilder.LAST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue should be(1000000L)
      ConfigurationBuilder.MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES.getValue should be(3000L)
      ConfigurationBuilder.THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES.getValue should be(50000L)
      ConfigurationBuilder.THRESHOLD_PERCENTAGE_OF_LOG_TO_READ.getValue should be(0.95F)
      ConfigurationBuilder.THRESHOLD_LOG_LINE_LENGTH.getValue should be(1000)
      ConfigurationBuilder.JHS_TIME_OUT.getValue should be(150000)
      ConfigurationBuilder.NUMBER_OF_STACKTRACE_LINE.getValue should be(5)
      val configuration = new Configuration()

      /**
        * Check for overriden values
        */
      configuration.setLong(ConfigurationBuilder.FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES.getConfigurationName, 26005)
      configuration.setLong(ConfigurationBuilder.LAST_THRESHOLD_LOG_LENGTH_IN_BYTES.getConfigurationName, 4353)
      configuration.setLong(ConfigurationBuilder.MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES.getConfigurationName, 1000)
      configuration.setFloat(ConfigurationBuilder.THRESHOLD_PERCENTAGE_OF_LOG_TO_READ.getConfigurationName, 0.85F)
      configuration.setLong(ConfigurationBuilder.THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES.getConfigurationName, 10000)
      configuration.setInt(ConfigurationBuilder.JHS_TIME_OUT.getConfigurationName, 11234)
      configuration.setInt(ConfigurationBuilder.NUMBER_OF_STACKTRACE_LINE.getConfigurationName, 5)
      configuration.setInt(ConfigurationBuilder.THRESHOLD_LOG_LINE_LENGTH.getConfigurationName, 100)
      ConfigurationBuilder.buildConfigurations(configuration)

      ConfigurationBuilder.FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue should be(26005L)
      ConfigurationBuilder.LAST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue should be(4353)
      ConfigurationBuilder.MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES.getValue should be(1000L)
      ConfigurationBuilder.THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES.getValue should be(10000L)
      ConfigurationBuilder.THRESHOLD_PERCENTAGE_OF_LOG_TO_READ.getValue should be(0.85F)
      ConfigurationBuilder.THRESHOLD_LOG_LINE_LENGTH.getValue should be(100)
      ConfigurationBuilder.JHS_TIME_OUT.getValue should be(11234)
      ConfigurationBuilder.NUMBER_OF_STACKTRACE_LINE.getValue should be(5)
    }


  }
}
