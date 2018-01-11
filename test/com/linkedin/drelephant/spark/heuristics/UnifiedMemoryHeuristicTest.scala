package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters

class UnifiedMemoryHeuristicTest extends FunSpec with Matchers {

  import UnifiedMemoryHeuristicTest._

  val heuristicConfigurationData = newFakeHeuristicConfigurationData()

  val memoryFractionHeuristic = new UnifiedMemoryHeuristic(heuristicConfigurationData)

  val executorData = Seq(
    newDummyExecutorData("1", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94567)),
    newDummyExecutorData("2", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34568)),
    newDummyExecutorData("3", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 34569)),
    newDummyExecutorData("4", 400000, Map("executionMemory" -> 20000, "storageMemory" -> 3456)),
    newDummyExecutorData("5", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34564)),
    newDummyExecutorData("6", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94561))
  )
  describe(".apply") {
    val data = newFakeSparkApplicationData(executorData)
    val heuristicResult = memoryFractionHeuristic.apply(data)
    val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

    it("has severity") {
      heuristicResult.getSeverity should be(Severity.CRITICAL)
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

  def newFakeSparkApplicationData(executorSummaries: Seq[ExecutorSummaryImpl]): SparkApplicationData = {
    val appId = "application_1"
    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = Seq.empty
    )

    SparkApplicationData(appId, restDerivedData, logDerivedData = None)
  }
}
