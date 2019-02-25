/*
 * Copyright 2019 LinkedIn Corp.
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

import com.linkedin.drelephant.analysis.{ ApplicationType, Severity }
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{ SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData }
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ ApplicationInfoImpl, StageDataImpl }
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{ FunSpec, Matchers }
import java.util.Date
import com.linkedin.drelephant.spark.fetchers.statusapiv1.TaskMetricDistributions
import com.linkedin.drelephant.spark.fetchers.statusapiv1.TaskMetricDistributionsImpl
import java.util.Calendar
import org.apache.commons.lang3.time.DateUtils
import com.linkedin.drelephant.spark.fetchers.statusapiv1.TaskMetricDistributionsImpl
import com.linkedin.drelephant.spark.fetchers.statusapiv1.TaskMetricDistributionsImpl
import com.linkedin.drelephant.AutoTuner

/**
 * Test class for Stages Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
 */
class SparkApplicationMetricsHeuristicTest extends FunSpec with Matchers {
  import SparkApplicationMetricsHeuristicTest._

  describe("SparkApplicationMetricsHeuristics") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map())
    val sparkApplicationMetricsHeuristics = new SparkApplicationMetricsHeuristic(heuristicConfigurationData)

    val stageDatas = Seq(
      newFakeStageData(StageStatus.COMPLETE, 0, 10, "stage1", 1000990929L, 100000L, 2000990929L,
        200000L, 4000990929L, 600000L, 24570990929L, 245990929L, 34570990929L, 34570990929L),
      newFakeStageData(StageStatus.COMPLETE, 1, 11, "stage2", 2000990929L, 200000L, 3000990929L,
        300000L, 5000990929L, 700000L, 23570990929L, 345990929L, 44570990929L, 44570990929L),
      newFakeStageData(StageStatus.COMPLETE, 2, 12, "stage3", 3000990929L, 300000L, 4000990929L,
        400000L, 6000990929L, 800000L, 22570990929L, 445990929L, 54570990929L, 4570990929L),
      newFakeStageData(StageStatus.COMPLETE, 3, 13, "stage4", 4000990929L, 400000L, 5000990929L,
        500000L, 7000990929L, 900000L, 21570990929L, 545990929L, 64570990929L, 64570990929L),
      newFakeStageData(StageStatus.COMPLETE, 4, 14, "stage5", 5000990929L, 500000L, 6000990929L,
        600000L, 8000990929L, 2000000L, 20570990929L, 645990929L, 74570990929L, 4570990929L),
      newFakeStageData(StageStatus.COMPLETE, 5, 15, "stage6", 6000990929L, 600000L, 7000990929L,
        700000L, 9000990929L, 2100000L, 27570990929L, 845990929L, 84570990929L, 84570990929L))

    val appConfigurationProperties = Map("spark.executor.instances" -> "2")

    describe(".apply") {
      val data = newFakeSparkApplicationData(stageDatas, appConfigurationProperties)
      val heuristicResult = sparkApplicationMetricsHeuristics.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.NONE)
      }
      it("returns the Total Input Size") {
        heuristicResultDetails.get(0).getValue should be("20032.8308")
      }
      it("returns the Total Input Records") {
        heuristicResultDetails.get(1).getValue should be("2100000")
      }
      it("returns the Total Output Size") {
        heuristicResultDetails.get(2).getValue should be("25 GB")
      }
      it("returns the Total Shuffle Read Size") {
        heuristicResultDetails.get(3).getValue should be("36 GB")
      }
      it("returns the Total Shuffle Read Records") {
        heuristicResultDetails.get(4).getValue should be("7100000")
      }
      it("returns the Total Shuffle Write Size") {
        heuristicResultDetails.get(5).getValue should be("130 GB")
      }
      it("returns the Total Shuffle Write Records") {
        heuristicResultDetails.get(6).getValue should be("3075945574")
      }
      it("returns the Total Memory Spill Size") {
        heuristicResultDetails.get(7).getValue should be("332 GB")
      }
      it("returns the Total Disk Spill Size") {
        heuristicResultDetails.get(8).getValue should be("221 GB")
      }
    }

  }
}

object SparkApplicationMetricsHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeStageData(
    status: StageStatus,
    stageId: Int,
    numCompleteTasks: Int,
    name: String,
    inputBytes: Long,
    inputRecords: Long,
    outputBytes: Long,
    outputRecords: Long,
    shuffleReadBytes: Long,
    shuffleReadRecords: Long,
    shuffleWriteBytes: Long,
    shuffleWriteRecords: Long,
    memoryBytesSpilled: Long,
    diskBytesSpilled: Long): StageDataImpl = new StageDataImpl(
    status,
    stageId,
    attemptId = 0,
    numTasks = numCompleteTasks + 0,
    numActiveTasks = numCompleteTasks + 0,
    numCompleteTasks,
    numFailedTasks = 0,
    executorRunTime = 0L,
    executorCpuTime = 0,
    submissionTime = None,
    firstTaskLaunchedTime = None,
    completionTime = None,
    failureReason = None,
    inputBytes,
    inputRecords,
    outputBytes,
    outputRecords,
    shuffleReadBytes,
    shuffleReadRecords,
    shuffleWriteBytes,
    shuffleWriteRecords,
    memoryBytesSpilled,
    diskBytesSpilled,
    name,
    details = "",
    schedulingPool = "",
    accumulatorUpdates = Seq.empty,
    tasks = None,
    executorSummary = None,
    peakJvmUsedMemory = None,
    peakExecutionMemory = None,
    peakStorageMemory = None,
    peakUnifiedMemory = None,
    taskSummary = None,
    executorMetricsSummary = None)

  def newFakeSparkApplicationData(
    stageDatas: Seq[StageDataImpl],
    appConfigurationProperties: Map[String, String]): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = stageDatas,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = Seq.empty)

    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq)))

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
