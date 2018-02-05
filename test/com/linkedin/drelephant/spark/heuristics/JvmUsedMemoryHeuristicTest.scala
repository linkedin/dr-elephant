package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters

/**
  * Test class for JVM Used memory. It checks whether all the values used in the heuristic are calculated correctly.
  */
class JvmUsedMemoryHeuristicTest extends FunSpec with Matchers {

  import JvmUsedMemoryHeuristicTest._

  val heuristicConfigurationData = newFakeHeuristicConfigurationData()

  val peakJvmUsedMemoryHeuristic = new JvmUsedMemoryHeuristic(heuristicConfigurationData)

  val appConfigurationProperties = Map("spark.driver.memory"->"40000000000", "spark.executor.memory"->"50000000000")

  val executorData = Seq(
    newDummyExecutorData("1", Map("jvmUsedMemory" -> 394567123)),
    newDummyExecutorData("2", Map("jvmUsedMemory" -> 2834)),
    newDummyExecutorData("3", Map("jvmUsedMemory" -> 3945667)),
    newDummyExecutorData("4", Map("jvmUsedMemory" -> 16367890)),
    newDummyExecutorData("5", Map("jvmUsedMemory" -> 2345647)),
    newDummyExecutorData("driver", Map("jvmUsedMemory" -> 394561))
  )
  describe(".apply") {
    val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
    val heuristicResult = peakJvmUsedMemoryHeuristic.apply(data)
    val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

    it("has severity") {
      heuristicResult.getSeverity should be(Severity.CRITICAL)
    }

    it("has all the details") {
      heuristicResultDetails.size() should be(4)
    }

    describe(".Evaluator") {
      import JvmUsedMemoryHeuristic.Evaluator

      val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
      val heuristicResult = peakJvmUsedMemoryHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails
      val evaluator = new Evaluator(peakJvmUsedMemoryHeuristic, data)

      it("has severity executor") {
        evaluator.severity should be(Severity.CRITICAL)
      }

      it("has max peak jvm memory") {
        evaluator.maxExecutorPeakJvmUsedMemory should be (394567123)
      }

      it("has reasonable size") {
        val details = heuristicResultDetails.get(3)
        details.getName should be ("Suggested spark.executor.memory")
        details.getValue should be ("452 MB")
      }
    }
  }
}

object JvmUsedMemoryHeuristicTest {

  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newDummyExecutorData(
    id: String,
    peakJvmUsedMemory: Map[String, Long]
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
    maxMemory = 0,
    totalGCTime = 0,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory,
    peakUnifiedMemory = Map.empty
  )

  def newFakeSparkApplicationData(
    appConfigurationProperties: Map[String, String],
    executorSummaries: Seq[ExecutorSummaryImpl]
  ): SparkApplicationData = {

    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))
    )
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = Seq.empty
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
