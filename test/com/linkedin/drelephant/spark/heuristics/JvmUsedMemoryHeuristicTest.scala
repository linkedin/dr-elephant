package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters

class JvmUsedMemoryHeuristicTest extends FunSpec with Matchers {

  import JvmUsedMemoryHeuristicTest._

  val heuristicConfigurationData = newFakeHeuristicConfigurationData()

  val peakJvmUsedMemoryHeuristic = new JvmUsedMemoryHeuristic(heuristicConfigurationData)

  val appConfigurationProperties = Map("spark.driver.memory"->"40000000000", "spark.executor.memory"->"500000000")

  val executorData = Seq(
    newDummyExecutorData("1", Map("jvmUsedMemory" -> 394567123)),
    newDummyExecutorData("2", Map("jvmUsedMemory" -> 23456834)),
    newDummyExecutorData("3", Map("jvmUsedMemory" -> 334569)),
    newDummyExecutorData("4", Map("jvmUsedMemory" -> 134563)),
    newDummyExecutorData("5", Map("jvmUsedMemory" -> 234564)),
    newDummyExecutorData("driver", Map("jvmUsedMemory" -> 394561))
  )
  describe(".apply") {
    val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
    val heuristicResult = peakJvmUsedMemoryHeuristic.apply(data)

    it("has severity") {
      heuristicResult.getSeverity should be(Severity.CRITICAL)
    }

    describe(".Evaluator") {
      import JvmUsedMemoryHeuristic.Evaluator

      val data = newFakeSparkApplicationData(appConfigurationProperties, executorData)
      val evaluator = new Evaluator(peakJvmUsedMemoryHeuristic, data)

      it("has severity executor") {
        evaluator.severityExecutor should be(Severity.NONE)
      }

      it("has severity driver") {
        evaluator.severityDriver should be(Severity.CRITICAL)
      }

      it("has max peak jvm memory") {
        evaluator.maxExecutorPeakJvmUsedMemory should be (394567123)
      }

      it("has max driver peak jvm memory") {
        evaluator.maxDriverPeakJvmUsedMemory should be (394561)
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
    totalDuration = 0,
    totalInputBytes = 0,
    totalShuffleRead = 0,
    totalShuffleWrite = 0,
    maxMemory = 0,
    totalGCTime = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory
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
      executorSummaries = executorSummaries
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
