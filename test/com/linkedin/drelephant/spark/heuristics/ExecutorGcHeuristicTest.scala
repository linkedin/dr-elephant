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
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfoImpl, ApplicationInfoImpl, ExecutorSummaryImpl, StageDataImpl}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}
import java.util.{Calendar,Date}
import scala.concurrent.duration.Duration

/**
  * Test class for Executor GC Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class ExecutorGcHeuristicTest extends FunSpec with Matchers {
  import ExecutorGcHeuristicTest._

  describe("ExecutorGcHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData()
    val executorGcHeuristic = new ExecutorGcHeuristic(heuristicConfigurationData)
    val dateArray : Array[Date] = new Array[Date](20)
    val cal: Calendar = Calendar.getInstance()
    dateArray(0) = cal.getTime
    cal.add(Calendar.MILLISECOND,700)
    dateArray(1) = cal.getTime
    cal.add(Calendar.MILLISECOND,300)
    for(i <- 2 to 11)
    {
    dateArray(i) = cal.getTime
    cal.add(Calendar.MINUTE,1)
    }
    for(i <- 12 to 19)
    {
     dateArray(i) = cal.getTime
     cal.add(Calendar.MINUTE,5)
    }
    val appAttemptInfo = Seq(
    newFakeApplicationAttemptInfo(endTime = dateArray(14))
    )

    val appAttemptInfo1 = Seq(
    newFakeApplicationAttemptInfo(endTime = dateArray(1))
    )

   val appAttemptInfo2 = Seq(
    newFakeApplicationAttemptInfo(endTime = dateArray(6))
   )

   val appAttemptInfo3 = Seq(
   newFakeApplicationAttemptInfo(endTime = dateArray(9))
    )
    val executorSummaries = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalGCTime = Duration("2min").toMillis,
        totalDuration = Duration("15min").toMillis,
        addTime = dateArray(12),
        endTime = Option(dateArray(15))
      ),
      newFakeExecutorSummary(
        id = "2",
        totalGCTime = Duration("6min").toMillis,
        totalDuration = Duration("14min").toMillis,
        addTime = dateArray(8),
        endTime = None
      ),
      newFakeExecutorSummary(
        id = "3",
        totalGCTime = Duration("4min").toMillis,
        totalDuration = Duration("20min").toMillis,
        addTime = dateArray(2),
        endTime = None
        ),
      newFakeExecutorSummary(
        id = "4",
        totalGCTime = Duration("8min").toMillis,
        totalDuration = Duration("30min").toMillis,
        addTime = dateArray(13),
        endTime = Option(dateArray(19))
      )
    )

    val executorSummaries1 = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalGCTime = 500,
        totalDuration = 700,
        addTime = dateArray(0),
        endTime = Option(dateArray(1))
      )
    )

    val executorSummaries2 = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalGCTime = 12000,
        totalDuration = Duration("4min").toMillis,
        addTime = dateArray(2),
        endTime = None
      ),
      newFakeExecutorSummary(
        id = "2",
        totalGCTime = 13000,
        totalDuration = Duration("1min").toMillis,
        addTime = dateArray(5),
        endTime = None
      )
    )

    val executorSummaries3 = Seq(
      newFakeExecutorSummary(
        id = "1",
        totalGCTime = 9000,
        totalDuration = Duration("2min").toMillis,
        addTime = dateArray(7),
        endTime = None
      )
    )

    describe(".apply") {
      val data = newFakeSparkApplicationData(executorSummaries, appAttemptInfo)
      val data1 = newFakeSparkApplicationData(executorSummaries1, appAttemptInfo1)
      val data2 = newFakeSparkApplicationData(executorSummaries2, appAttemptInfo2)
      val data3 = newFakeSparkApplicationData(executorSummaries3, appAttemptInfo3)
      val heuristicResult = executorGcHeuristic.apply(data)
      val heuristicResult1 = executorGcHeuristic.apply(data1)
      val heuristicResult2 = executorGcHeuristic.apply(data2)
      val heuristicResult3 = executorGcHeuristic.apply(data3)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails
      val heuristicResultDetails1 = heuristicResult1.getHeuristicResultDetails
      val heuristicResultDetails2 = heuristicResult2.getHeuristicResultDetails
      val heuristicResultDetails3 = heuristicResult3.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.CRITICAL)
      }

      it("returns non-zero score") {
        heuristicResult.getScore should be(Severity.CRITICAL.getValue * data.executorSummaries
          .filterNot(_.id.equals("driver")).size)
      }

      it("return the low severity") {
        heuristicResult2.getSeverity should be(Severity.LOW)
      }

      it("return the 0 score") {
        heuristicResult2.getScore should be(0)
      }

      it("return NONE severity for runtime less than 5 min") {
        heuristicResult2.getSeverity should be(Severity.LOW)
      }

      it("return none severity") {
        heuristicResult3.getSeverity should be(Severity.NONE)
      }

      it("returns the JVM GC time to Executor Run time duration") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("GC time to Executor Run time ratio")
        details.getValue should include("0.2531")
      }

      it("returns the total GC time") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("Total GC time")
        details.getValue should be("20 Minutes")
      }

      it("returns the executor's run time") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("Total Executor Runtime")
        details.getValue should be("1 Hours 19 Minutes")
      }

      it("returns total Gc Time in millisec") {
        val details = heuristicResultDetails1.get(1)
        details.getName should include("Total GC time")
        details.getValue should be("500 msec")
      }

      it("returns executor run Time in millisec") {
        val details = heuristicResultDetails1.get(2)
        details.getName should include("Total Executor Runtime")
        details.getValue should be("700 msec")
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
    totalDuration: Long,
    addTime: Date,
    endTime: Option[Date]
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
    maxTasks = 0,
    totalDuration,
    addTime,
    endTime,
    totalInputBytes=0,
    totalShuffleRead=0,
    totalShuffleWrite= 0,
    maxMemory = 0,
    totalGCTime,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory = Map.empty,
    peakUnifiedMemory = Map.empty
  )
  def newFakeApplicationAttemptInfo(
      endTime: Date
    ): ApplicationAttemptInfoImpl = new ApplicationAttemptInfoImpl(
      attemptId = Option("attemptId_1"),
      startTime= Calendar.getInstance().getTime,
      endTime,
      sparkUser = "",
      completed = true
    )

  def newFakeSparkApplicationData(
    executorSummaries: Seq[ExecutorSummaryImpl] , appAttemptInfo: Seq[ApplicationAttemptInfoImpl]
  ): SparkApplicationData = {
    val appId = "application_1"

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", appAttemptInfo),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = Seq.empty
    )
    SparkApplicationData(appId, restDerivedData, None)
  }
}
