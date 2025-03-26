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
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration.Duration


class ExecutorGcHeuristicTest extends FunSpec with Matchers {
  import ExecutorGcHeuristicTest._

  describe("ExecutorGcHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map(
        "max_to_median_ratio_severity_thresholds" -> "1.414,2,4,16",
        "ignore_max_bytes_less_than_threshold" -> "4000000",
        "ignore_max_millis_less_than_threshold" -> "4000001"
      )
    )
    val executorGcHeuristic = new ExecutorGcHeuristic(heuristicConfigurationData)

    val executorSummaries = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalGCTime = Duration("2min").toMillis,
        totalDuration = Duration("15min").toMillis
      ),
      newFakeExecutorSummary(
        id = "2",
        totalGCTime = Duration("6min").toMillis,
        totalDuration = Duration("14min").toMillis
      ),
      newFakeExecutorSummary(
        id = "3",
        totalGCTime = Duration("4min").toMillis,
        totalDuration = Duration("20min").toMillis
      ),
      newFakeExecutorSummary(
        id = "4",
        totalGCTime = Duration("8min").toMillis,
        totalDuration = Duration("30min").toMillis
      )
    )

    describe(".apply") {
      val data1 = newFakeSparkApplicationData(executorSummaries)
      val heuristicResult = executorGcHeuristic.apply(data1)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.CRITICAL)
      }

      it("returns the JVM GC time to Executor Run time duration") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("GC time to Executor Run time ratio")
        details.getValue should include("0.2531")
      }

      it("returns the total GC time") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("Total GC time")
        details.getValue should be("1200000")
      }

      it("returns the executor's run time") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("Total Executor Runtime")
        details.getValue should be("4740000")
      }
    }
  }
}

object ExecutorGcHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeExecutorSummary(
    id: String,
    totalGCTime: Long,
    totalDuration: Long
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
    totalDuration,
    totalInputBytes=0,
    totalShuffleRead=0,
    totalShuffleWrite= 0,
    maxMemory= 0,
    totalGCTime,
    executorLogs = Map.empty
  )

  def newFakeSparkApplicationData(
    executorSummaries: Seq[ExecutorSummaryImpl]
  ): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = executorSummaries
    )
    SparkApplicationData(appId, restDerivedData, None)
  }
}
