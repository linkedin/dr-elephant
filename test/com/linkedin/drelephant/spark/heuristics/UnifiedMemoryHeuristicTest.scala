package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import com.linkedin.drelephant.spark.heuristics.UnifiedMemoryHeuristic.Evaluator
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters

/**
  * Test class for Unified Memory Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class UnifiedMemoryHeuristicTest extends FunSpec with Matchers {

  import UnifiedMemoryHeuristicTest._

  val heuristicConfigurationData = newFakeHeuristicConfigurationData()
  val unifiedMemoryHeuristic = new UnifiedMemoryHeuristic(heuristicConfigurationData)
  val appConfigurationProperties = Map("spark.executor.memory"->"3147483647")
  val appConfigurationProperties1 = Map("spark.executor.memory"->"214567874847")
  val appConfigurationProperties2 = Map("spark.executor.memory"->"214567874847", "spark.memory.fraction"->"0.06")
  val appConfigurationProperties3 = Map("spark.executor.memory"->"5368709120")

  val executorData = Seq(
    newDummyExecutorData("1", 999999999, Map.empty, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 400000, Map.empty, Map("executionMemory" -> 200000, "storageMemory" -> 34568)),
    newDummyExecutorData("3", 400000, Map.empty, Map("executionMemory" -> 300000, "storageMemory" -> 34569)),
    newDummyExecutorData("4", 400000, Map.empty, Map("executionMemory" -> 20000, "storageMemory" -> 3456)),
    newDummyExecutorData("5", 400000, Map.empty,Map("executionMemory" -> 200000, "storageMemory" -> 34564)),
    newDummyExecutorData("6", 400000, Map.empty,Map("executionMemory" -> 300000, "storageMemory" -> 94561))
  )

  val executorData1 = Seq(
    newDummyExecutorData("driver", 400000, Map.empty,Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 999999999, Map.empty,Map("executionMemory" -> 200, "storageMemory" -> 200))
  )

  val executorData2 = Seq(
    newDummyExecutorData("driver", 400000, Map.empty, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 999999999, Map.empty, Map("executionMemory" -> 999999990, "storageMemory" -> 9))
  )

  val executorData3 = Seq(
    newDummyExecutorData("1", 400000, Map.empty, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 500000, Map.empty, Map("executionMemory" -> 5000, "storageMemory" -> 9))
  )

  val executorData4 = Seq(
    newDummyExecutorData("1", 268435460L, Map.empty, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 268435460L, Map.empty, Map("executionMemory" -> 900000, "storageMemory" -> 500000))
  )

  val executorData5 = Seq(
    newDummyExecutorData("1",1073741824 , Map("jvmUsedMemory" -> 147483000), Map("executionMemory" -> 429496729, "storageMemory" -> 10737418)))

  val executorData6 = Seq(
    newDummyExecutorData("1",1610612736 , Map("jvmUsedMemory" -> 3847483648L), Map("executionMemory" -> 838860800, "storageMemory" -> 10737418)),
    newDummyExecutorData("2",1610612736 , Map("jvmUsedMemory" -> 2147480000L), Map("executionMemory" -> 209715200, "storageMemory" -> 10737418))
  )


  describe(".apply") {
    val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
    val data1 = newFakeSparkApplicationData(appConfigurationProperties1, executorData1)
    val data2 = newFakeSparkApplicationData(appConfigurationProperties1, executorData2)
    val data3 = newFakeSparkApplicationData(appConfigurationProperties1, executorData3)
    val data4 = newFakeSparkApplicationData(appConfigurationProperties2, executorData4)
    val data5 = newFakeSparkApplicationData(appConfigurationProperties3, executorData5)
    val data6 = newFakeSparkApplicationData(appConfigurationProperties3, executorData6)
    val heuristicResult = unifiedMemoryHeuristic.apply(data)
    val heuristicResult1 = unifiedMemoryHeuristic.apply(data1)
    val heuristicResult2 = unifiedMemoryHeuristic.apply(data2)
    val heuristicResult3 = unifiedMemoryHeuristic.apply(data3)
    val heuristicResult4 = unifiedMemoryHeuristic.apply(data4)
    val heuristicResult5 = unifiedMemoryHeuristic.apply(data5)
    val heuristicResult6 = unifiedMemoryHeuristic.apply(data6)
    val evaluator = new Evaluator(unifiedMemoryHeuristic, data1)
    it("has severity") {
      heuristicResult.getSeverity should be(Severity.CRITICAL)
    }

    it("has max value") {
      val details = heuristicResult.getHeuristicResultDetails.get(2)
      details.getName should be("Max peak unified memory")
      details.getValue should be("385.32 KB")
    }

    it("has mean value") {
      val details = heuristicResult.getHeuristicResultDetails.get(1)
      details.getName should be("Mean peak unified memory")
      details.getValue should be("263.07 KB")
    }

    it("data1 has severity") {
      heuristicResult1.getSeverity should be(Severity.CRITICAL)
    }

    it("data1 has maxMemory") {
      evaluator.maxMemory should be(999999999)
    }

    it("data1 has max memory") {
      evaluator.maxUnifiedMemory should be(400)
    }

    it("data1 has mean memory") {
      evaluator.meanUnifiedMemory should be(400)
    }

    it("has no severity when max and allocated memory are the same") {
      heuristicResult2.getSeverity should be(Severity.NONE)
      //no severity so there will be no suggestion either for unified or executor memory
      heuristicResult2.getHeuristicResultDetails.size() should be(5)
    }

    it("has no severity when maxMemory is less than 256Mb") {
      heuristicResult3.getSeverity should be(Severity.NONE)
    }

    it("has critical severity when maxMemory is greater than 256Mb and spark memory fraction is greater than 0.05") {
      heuristicResult4.getSeverity should be(Severity.CRITICAL)
    }

    it("has suggestion for executor memory") {
      heuristicResult5.getHeuristicResultDetails.size() should be(6)
      heuristicResult5.getHeuristicResultDetails.get(5).getName should be("Executor Memory")
    }

    it("has suggestion for spark.memory.fraction") {
      heuristicResult6.getHeuristicResultDetails.size() should be(6)
      heuristicResult6.getHeuristicResultDetails.get(5).getName should be("Unified Memory")
    }

    it("has no severity when maxMemory is less than 256Mb") {
      heuristicResult3.getSeverity should be(Severity.NONE)
    }

    it("has critical severity when maxMemory is greater than 256Mb and spark memory fraction is greater than 0.05") {
      heuristicResult4.getSeverity should be(Severity.CRITICAL)
    }
  }
}

object UnifiedMemoryHeuristicTest {

  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newDummyExecutorData(
    id: String,
    maxMemory: Long,
    peakJvmUsedMemory: Map[String, Long],
    peakUnifiedMemory: Map[String, Long]
  ): ExecutorSummaryImpl = new ExecutorSummaryImpl(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed = 0,
    diskUsed = 0,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks = 0,
    maxTasks = 0,
    totalDuration = 0,
    totalInputBytes = 0,
    totalShuffleRead = 0,
    totalShuffleWrite = 0,
    maxMemory,
    totalGCTime = 0,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory,
    peakUnifiedMemory
  )

  def newFakeSparkApplicationData(
    appConfigurationProperties: Map[String, String],
    executorSummaries: Seq[ExecutorSummaryImpl]): SparkApplicationData =
  {
    val appId = "application_1"
    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = Seq.empty
    )
    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))
    )
    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
