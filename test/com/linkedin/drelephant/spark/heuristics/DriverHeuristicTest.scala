package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import com.linkedin.drelephant.spark.heuristics.DriverHeuristic.Evaluator
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters
import scala.concurrent.duration.Duration

/**
  * Test class for Driver Metrics Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class DriverHeuristicTest extends FunSpec with Matchers {

  import DriverHeuristicTest._

  val heuristicConfigurationData = newFakeHeuristicConfigurationData()

  val driverHeuristic = new DriverHeuristic(heuristicConfigurationData)

  val executorData = Seq(
    newDummyExecutorData("1", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94567), null, 0, 0),
    newDummyExecutorData("2", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34568), null, 0, 0),
    newDummyExecutorData("3", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 34569), null, 0, 0),
    newDummyExecutorData("4", 400000, Map("executionMemory" -> 20000, "storageMemory" -> 3456), null, 0, 0),
    newDummyExecutorData("5", 400000, Map("executionMemory" -> 200000, "storageMemory" -> 34564), null, 0, 0),
    newDummyExecutorData("driver", 400000, Map("executionMemory" -> 300000, "storageMemory" -> 94561), Map("jvmUsedMemory" -> 394567123),
      totalGCTime = Duration("2min").toMillis, totalDuration = Duration("15min").toMillis)
  )
  describe(".apply") {
    val data = newFakeSparkApplicationData(executorData)
    val heuristicResult = driverHeuristic.apply(data)
    val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

    it("has severity") {
      heuristicResult.getSeverity should be(Severity.SEVERE)
    }

    describe("Evaluator") {
      val evaluator = new Evaluator(driverHeuristic, data)
      it("has max driver peak JVM memory") {
        evaluator.maxDriverPeakJvmUsedMemory should be(394567123)
      }
      it("ratio of time spend in Gc to total duration") {
        evaluator.ratio should be(0.13333333333333333)
      }
    }
  }
}

object DriverHeuristicTest {

  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newDummyExecutorData(
    id: String,
    maxMemory: Long,
    peakUnifiedMemory: Map[String, Long],
    peakJvmUsedMemory: Map[String, Long],
    totalGCTime: Long,
    totalDuration: Long
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
    totalDuration,
    totalInputBytes = 0,
    totalShuffleRead = 0,
    totalShuffleWrite = 0,
    maxMemory,
    totalGCTime,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory,
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

