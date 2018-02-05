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

  val executorData = Seq(
    newDummyExecutorData("1", 999999999, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34568)),
    newDummyExecutorData("3", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 34569)),
    newDummyExecutorData("4", 400000, Map("executionMemory" -> 20000, "storageMemory" -> 3456)),
    newDummyExecutorData("5", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34564)),
    newDummyExecutorData("6", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94561))
  )

  val executorData1 = Seq(
    newDummyExecutorData("driver", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 999999999, Map("executionMemory" -> 200, "storageMemory" -> 200))
  )

  val executorData2 = Seq(
    newDummyExecutorData("driver", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 999999999, Map("executionMemory" -> 999999990, "storageMemory" -> 9))
  )

  describe(".apply") {
    val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
    val data1 = newFakeSparkApplicationData(appConfigurationProperties1, executorData1)
    val data2 = newFakeSparkApplicationData(appConfigurationProperties1, executorData2)
    val heuristicResult = unifiedMemoryHeuristic.apply(data)
    val heuristicResult1 = unifiedMemoryHeuristic.apply(data1)
    val heuristicResult2 = unifiedMemoryHeuristic.apply(data2)
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
    peakJvmUsedMemory = Map.empty,
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
