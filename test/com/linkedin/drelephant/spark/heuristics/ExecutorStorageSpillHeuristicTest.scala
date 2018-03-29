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

package com.linkedin.drelephant.spark.heuristics

import scala.collection.JavaConverters
import com.linkedin.drelephant.analysis.{ApplicationType, Severity, SeverityThresholds}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl, StageDataImpl}
import com.linkedin.drelephant.spark.heuristics.ExecutorStorageSpillHeuristic.Evaluator
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

/**
  * Test class for Executor Storage Spill Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class ExecutorStorageSpillHeuristicTest extends FunSpec with Matchers {
  import ExecutorStorageSpillHeuristicTest._

  describe("ExecutorStorageSpillHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map.empty
    )
    val executorStorageSpillHeuristic = new ExecutorStorageSpillHeuristic(heuristicConfigurationData)

    val appConfigurationProperties = Map("spark.executor.memory" -> "4g", "spark.executor.cores"->"4", "spark.executor.instances"->"4")

    val executorSummaries = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalMemoryBytesSpilled = 200000L
      ),
      newFakeExecutorSummary(
        id = "2",
        totalMemoryBytesSpilled = 100000L
      ),
      newFakeExecutorSummary(
        id = "3",
        totalMemoryBytesSpilled = 300000L
      ),
      newFakeExecutorSummary(
        id = "4",
        totalMemoryBytesSpilled = 200000L
      )
    )

    describe(".apply") {
      val data1 = newFakeSparkApplicationData(executorSummaries, appConfigurationProperties)
      val heuristicResult = executorStorageSpillHeuristic.apply(data1)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails
      val evaluator = new Evaluator(executorStorageSpillHeuristic, data1)

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.SEVERE)
      }

      it("returns the total memory spilled") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("Total memory spilled")
        details.getValue should be("781.25 KB")
      }

      it("returns the max memory spilled") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("Max memory spilled")
        details.getValue should be("292.97 KB")
      }

      it("returns the mean memory spilled") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("Mean memory spilled")
        details.getValue should be("195.31 KB")
      }

      it("has the memory spilled per task") {
        evaluator.totalMemorySpilledPerTask should be(800000)
      }
    }
  }
}

object ExecutorStorageSpillHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeExecutorSummary(
    id: String,
    totalMemoryBytesSpilled: Long
  ): ExecutorSummaryImpl = new ExecutorSummaryImpl(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed=0,
    diskUsed = 0,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks = 0,
    maxTasks = 10,
    totalDuration=0,
    totalInputBytes=0,
    totalShuffleRead=0,
    totalShuffleWrite= 0,
    maxMemory= 2000,
    totalGCTime = 0,
    totalMemoryBytesSpilled,
    executorLogs = Map.empty,
    peakJvmUsedMemory = Map.empty,
    peakUnifiedMemory = Map.empty
  )

  def newFakeSparkApplicationData(
    executorSummaries: Seq[ExecutorSummaryImpl],
    appConfigurationProperties: Map[String, String]
  ): SparkApplicationData = {
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
