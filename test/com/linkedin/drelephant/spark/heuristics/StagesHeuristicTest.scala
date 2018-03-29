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
import scala.concurrent.duration.Duration

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, StageDataImpl}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

/**
  * Test class for Stages Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class StagesHeuristicTest extends FunSpec with Matchers {
  import StagesHeuristicTest._

  describe("StagesHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map(
        "stage_failure_rate_severity_thresholds" -> "0.2,0.4,0.6,0.8",
        "stage_task_failure_rate_severity_thresholds" -> "0.2,0.4,0.6,0.8",
        "stage_runtime_minutes_severity_thresholds" -> "15,30,45,60"
      )
    )
    val stagesHeuristic = new StagesHeuristic(heuristicConfigurationData)
    val stageDatas = Seq(
      newFakeStageData(StageStatus.COMPLETE, 0, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("2min").toMillis, "foo"),
      newFakeStageData(StageStatus.COMPLETE, 1, numCompleteTasks = 8, numFailedTasks = 2, executorRunTime = Duration("2min").toMillis, "bar"),
      newFakeStageData(StageStatus.COMPLETE, 2, numCompleteTasks = 6, numFailedTasks = 4, executorRunTime = Duration("2min").toMillis, "baz"),
      newFakeStageData(StageStatus.FAILED, 3, numCompleteTasks = 4, numFailedTasks = 6, executorRunTime = Duration("2min").toMillis, "aaa"),
      newFakeStageData(StageStatus.FAILED, 4, numCompleteTasks = 2, numFailedTasks = 8, executorRunTime = Duration("2min").toMillis, "zzz"),
      newFakeStageData(StageStatus.COMPLETE, 5, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("0min").toMillis, "bbb"),
      newFakeStageData(StageStatus.COMPLETE, 6, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("30min").toMillis, "ccc"),
      newFakeStageData(StageStatus.COMPLETE, 7, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("60min").toMillis, "ddd"),
      newFakeStageData(StageStatus.COMPLETE, 8, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("90min").toMillis, "eee"),
      newFakeStageData(StageStatus.COMPLETE, 9, numCompleteTasks = 10, numFailedTasks = 0, executorRunTime = Duration("120min").toMillis, "fff")
    )

    val appConfigurationProperties = Map("spark.executor.instances" -> "2")

    describe(".apply") {
      val data = newFakeSparkApplicationData(stageDatas, appConfigurationProperties)
      val heuristicResult = stagesHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.CRITICAL)
      }

      it("returns the number of completed stages") {
        heuristicResultDetails.get(0).getValue should be("8")
      }

      it("returns the number of failed stages") {
        heuristicResultDetails.get(1).getValue should be("2")
      }

      it("returns the stage failure rate") {
        heuristicResultDetails.get(2).getValue should be("0.200")
      }

      it("returns the list of stages with high task failure rates") {
        heuristicResultDetails.get(3).getValue should be(
          s"""|stage 3, attempt 0 (task failure rate: 0.600)
              |stage 4, attempt 0 (task failure rate: 0.800)""".stripMargin
        )
      }
    }

    describe(".Evaluator") {
      import StagesHeuristic.Evaluator

      val data = newFakeSparkApplicationData(stageDatas, appConfigurationProperties)
      val evaluator = new Evaluator(stagesHeuristic, data)

      it("has the number of completed stages") {
        evaluator.numCompletedStages should be(8)
      }

      it("has the number of failed stages") {
        evaluator.numFailedStages should be(2)
      }

      it("has the stage failure rate") {
        evaluator.stageFailureRate should be(Some(0.2D))
      }

      it("has the list of stages with high task failure rates") {
        val stageIdsAndTaskFailureRates =
          evaluator.stagesWithHighTaskFailureRates.map { case (stageData, taskFailureRate) => (stageData.stageId, taskFailureRate) }
        stageIdsAndTaskFailureRates should contain theSameElementsInOrderAs(Seq((3, 0.6D), (4, 0.8D)))
      }
      it("computes the overall severity") {
        evaluator.severity should be(Severity.CRITICAL)
      }
    }
  }
}

object StagesHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeStageData(
    status: StageStatus,
    stageId: Int,
    numCompleteTasks: Int,
    numFailedTasks: Int,
    executorRunTime: Long,
    name: String
  ): StageDataImpl = new StageDataImpl(
    status,
    stageId,
    attemptId = 0,
    numActiveTasks = numCompleteTasks + numFailedTasks,
    numCompleteTasks,
    numFailedTasks,
    executorRunTime,
    inputBytes = 0,
    inputRecords = 0,
    outputBytes = 0,
    outputRecords = 0,
    shuffleReadBytes = 0,
    shuffleReadRecords = 0,
    shuffleWriteBytes = 0,
    shuffleWriteRecords = 0,
    memoryBytesSpilled = 0,
    diskBytesSpilled = 0,
    name,
    details = "",
    schedulingPool = "",
    accumulatorUpdates = Seq.empty,
    tasks = None,
    executorSummary = None
  )

  def newFakeSparkApplicationData(
    stageDatas: Seq[StageDataImpl],
    appConfigurationProperties: Map[String, String]
  ): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = stageDatas,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = Seq.empty
    )

    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
