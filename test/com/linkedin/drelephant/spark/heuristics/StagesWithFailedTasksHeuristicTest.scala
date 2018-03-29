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

import java.util.Date
import scala.collection.JavaConverters
import scala.concurrent.duration.Duration
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, StageDataImpl, TaskDataImpl}

/**
  * Test class for Stages With Failed Tasks Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class StagesWithFailedTasksHeuristicTest extends FunSpec with Matchers {
  import StagesWithFailedTasksHeuristicTest._

  val OOM_ERROR = "java.lang.OutOfMemoryError"
  val OVERHEAD_MEMORY_ERROR = "killed by YARN for exceeding memory limits"

  describe("StagesHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData()
    val stagesWithFailedTasksHeuristic = new StagesWithFailedTasksHeuristic(heuristicConfigurationData)
    val failedTaskData = Seq(
      newFakeStageData(StageStatus.COMPLETE, 0, numCompleteTasks = 10, OOM_ERROR, OVERHEAD_MEMORY_ERROR),
      newFakeStageData(StageStatus.COMPLETE, 1, numCompleteTasks = 100, OOM_ERROR, OVERHEAD_MEMORY_ERROR),
      newFakeStageData(StageStatus.COMPLETE, 2, numCompleteTasks = 5, OOM_ERROR, ""),
      newFakeStageData(StageStatus.FAILED, 3, numCompleteTasks = 3, OVERHEAD_MEMORY_ERROR, OVERHEAD_MEMORY_ERROR),
      newFakeStageData(StageStatus.FAILED, 4, numCompleteTasks = 102, OVERHEAD_MEMORY_ERROR, OVERHEAD_MEMORY_ERROR),
      newFakeStageData(StageStatus.COMPLETE, 5, numCompleteTasks = 100, OOM_ERROR, OOM_ERROR),
      newFakeStageData(StageStatus.COMPLETE, 6, numCompleteTasks = 10, OOM_ERROR, ""),
      newFakeStageData(StageStatus.COMPLETE, 7, numCompleteTasks = 10, OOM_ERROR, OVERHEAD_MEMORY_ERROR),
      newFakeStageData(StageStatus.COMPLETE, 8, numCompleteTasks = 10, "", ""),
      newFakeStageData(StageStatus.COMPLETE, 9, numCompleteTasks = 20, OOM_ERROR, OOM_ERROR)
    )
    val appConfigurationProperties = Map.empty

    describe(".apply") {
      val data = newFakeSparkApplicationData(failedTaskData)
      val heuristicResult = stagesWithFailedTasksHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.SEVERE)
      }
    }

    describe(".Evaluator") {
      import StagesWithFailedTasksHeuristic.Evaluator
      val data = newFakeSparkApplicationData(failedTaskData)
      val evaluator = new Evaluator(stagesWithFailedTasksHeuristic, data)

      it("has OOM and Overhead severity") {
        evaluator.severityOOMStages should be(Severity.SEVERE)
        evaluator.severityOverheadStages should be (Severity.SEVERE)
      }
      it("has correct number of stages having error") {
        evaluator.stagesWithOOMError should be (7)
        evaluator.stagesWithOverheadError should be (5)
      }
    }
  }
}

object StagesWithFailedTasksHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeStageData(
    status: StageStatus,
    stageId: Int,
    numCompleteTasks: Int,
    error1: String,
    error2: String
   ): StageDataImpl = new StageDataImpl(
    status,
    stageId,
    attemptId = 0,
    numActiveTasks = numCompleteTasks,
    numCompleteTasks,
    numFailedTasks = 3,
    executorRunTime = 0,
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
    name = "foo",
    details = "",
    schedulingPool = "",
    accumulatorUpdates = Seq.empty,
    tasks = new Some(Map(0.toLong -> new TaskDataImpl(
      taskId = 0,
      index = 1,
      attempt = 0,
      launchTime = new Date(),
      executorId = "1",
      host = "SomeHost",
      taskLocality = "ANY",
      speculative = false,
      accumulatorUpdates = Seq(),
      errorMessage = Some(error1),
      taskMetrics = None), 1.toLong -> new TaskDataImpl(
      taskId = 1,
      index = 1,
      attempt = 0,
      launchTime = new Date(),
      executorId = "1",
      host = "SomeHost",
      taskLocality = "ANY",
      speculative = false,
      accumulatorUpdates = Seq(),
      errorMessage = Some(error2),
      taskMetrics = None), 2.toLong -> new TaskDataImpl(
      taskId = 1,
      index = 1,
      attempt = 0,
      launchTime = new Date(),
      executorId = "1",
      host = "SomeHost",
      taskLocality = "ANY",
      speculative = false,
      accumulatorUpdates = Seq(),
      errorMessage = None,
      taskMetrics = None)
    )),
    executorSummary = None
  )

  def newFakeSparkApplicationData
  (stagesWithFailedTasks: Seq[StageDataImpl]): SparkApplicationData = {
    val appId = "application_1"
    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = stagesWithFailedTasks
    )
    SparkApplicationData(appId, restDerivedData, None)
  }
}