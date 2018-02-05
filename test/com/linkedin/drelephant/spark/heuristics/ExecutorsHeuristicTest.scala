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
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfoImpl, ExecutorSummaryImpl}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

/**
  * Test class for Executors Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
class ExecutorsHeuristicTest extends FunSpec with Matchers {
  import ExecutorsHeuristicTest._

  describe("ExecutorsHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map(
        "max_to_median_ratio_severity_thresholds" -> "1.414,2,4,16",
        "ignore_max_bytes_less_than_threshold" -> "4000000",
        "ignore_max_millis_less_than_threshold" -> "4000001"
      )
    )
    val executorsHeuristic = new ExecutorsHeuristic(heuristicConfigurationData)

    val maxMemory = 5000000L

    val executorSummaries = Seq(
      newFakeExecutorSummary(
        id = "1",
        memoryUsed = 1000000L,
        totalDuration = 1000001L,
        totalInputBytes = 1000002L,
        totalShuffleRead = 1000003L,
        totalShuffleWrite = 1000004L,
        maxMemory
      ),
      newFakeExecutorSummary(
        id = "2",
        memoryUsed = 2000000L,
        totalDuration = 2000001L,
        totalInputBytes = 2000002L,
        totalShuffleRead = 2000003L,
        totalShuffleWrite = 2000004L,
        maxMemory
      ),
      newFakeExecutorSummary(
        id = "3",
        memoryUsed = 3000000L,
        totalDuration = 3000001L,
        totalInputBytes = 3000002L,
        totalShuffleRead = 3000003L,
        totalShuffleWrite = 3000004L,
        maxMemory
      ),
      newFakeExecutorSummary(
        id = "4",
        memoryUsed = 4000000L,
        totalDuration = 4000001L,
        totalInputBytes = 4000002L,
        totalShuffleRead = 4000003L,
        totalShuffleWrite = 4000004L,
        maxMemory
      )
    )

    describe(".apply") {
      val data = newFakeSparkApplicationData(executorSummaries)
      val heuristicResult = executorsHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.LOW)
      }

      it("returns the total storage memory allocated") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("storage memory allocated")
        details.getValue should be("19.07 MB")
      }

      it("returns the total storage memory used") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("storage memory used")
        details.getValue should be("9.54 MB")
      }

      it("returns the storage memory utilization rate") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("storage memory utilization rate")
        details.getValue should be("0.500")
      }

      it("returns the distribution of storage memory used among executors") {
        val details = heuristicResultDetails.get(3)
        details.getName should include("storage memory used")
        details.getValue should include regex("976.56 KB.*976.56 KB.*2.38 MB.*2.86 MB.*3.81 MB")
      }

      it("returns the distribution of task time among executors") {
        val details = heuristicResultDetails.get(4)
        details.getName should include("task time")
        details.getValue should include regex("16 min 40 sec.*16 min 40 sec.*41 min 40 sec.*50 min.*1 hr 6 min 40 sec")
      }

      it("returns the total sum of task time among executors") {
        val details = heuristicResultDetails.get(5)
        details.getName should include("task time sum")
        details.getValue should include regex("10000")
      }

      it("returns the distribution of input bytes among executors") {
        val details = heuristicResultDetails.get(6)
        details.getName should include("input bytes")
        details.getValue should include regex("976.56 KB.*976.56 KB.*2.38 MB.*2.86 MB.*3.81 MB")
      }

      it("returns the distribution of shuffle read bytes among executors") {
        val details = heuristicResultDetails.get(7)
        details.getName should include("shuffle read bytes")
        details.getValue should include regex("976.57 KB.*976.57 KB.*2.38 MB.*2.86 MB.*3.81 MB")
      }

      it("returns the distribution of shuffle write bytes among executors") {
        val details = heuristicResultDetails.get(8)
        details.getName should include("shuffle write bytes")
        details.getValue should include regex("976.57 KB.*976.57 KB.*2.38 MB.*2.86 MB.*3.81 MB")
      }
    }

    describe(".Evaluator") {
      import ExecutorsHeuristic.Evaluator
      import ExecutorsHeuristic.Distribution

      val data = newFakeSparkApplicationData(executorSummaries)
      val evaluator = new Evaluator(executorsHeuristic, data)

      it("has the total storage memory allocated") {
        evaluator.totalStorageMemoryAllocated should be(20000000L)
      }

      it("has the total storage memory used") {
        evaluator.totalStorageMemoryUsed should be(10000000L)
      }

      it("has the storage memory utilization rate") {
        evaluator.storageMemoryUtilizationRate should be(0.5D)
      }

      it("has the distribution of storage memory used among executors") {
        evaluator.storageMemoryUsedDistribution should be(
          Distribution(1000000L, 1000000L, 2500000L, 3000000L, 4000000L)
        )
      }

      it("has the distribution of task time among executors") {
        evaluator.taskTimeDistribution should be(
          Distribution(1000001L, 1000001L, 2500001L, 3000001L, 4000001L)
        )
      }

      it("has the distribution of input bytes among executors") {
        evaluator.inputBytesDistribution should be(
          Distribution(1000002L, 1000002L, 2500002L, 3000002L, 4000002L)
        )
      }

      it("has the distribution of shuffle read among executors") {
        evaluator.shuffleReadBytesDistribution should be(
          Distribution(1000003L, 1000003L, 2500003L, 3000003L, 4000003L)
        )
      }

      it("has the distribution of shuffle write among executors") {
        evaluator.shuffleWriteBytesDistribution should be(
          Distribution(1000004L, 1000004L, 2500004L, 3000004L, 4000004L)
        )
      }

      it("computes the overall severity") {
        evaluator.severity should be(Severity.LOW)
      }

      it("computes the severity of a given distribution, when the max is large enough") {
        val distribution = Distribution(min = 0L, p25 = 1000L, median = 1000L, p75 = 1000L, max = 16000L)
        evaluator.severityOfDistribution(distribution, ignoreMaxLessThanThreshold = 16000L) should be(Severity.CRITICAL)
      }

      it("computes the severity of a given distribution, when the max is not large enough") {
        val distribution = Distribution(min = 0L, p25 = 1000L, median = 1000L, p75 = 1000L, max = 16000L)
        evaluator.severityOfDistribution(distribution, ignoreMaxLessThanThreshold = 16001L) should be(Severity.NONE)
      }

      it("computes the severity of a given distribution, when the median is zero and the max is large enough") {
        val distribution = Distribution(min = 0L, p25 = 0L, median = 0L, p75 = 0L, max = 16000L)
        evaluator.severityOfDistribution(distribution, ignoreMaxLessThanThreshold = 16000L) should be(Severity.CRITICAL)
      }

      it("computes the severity of a given distribution, when the median is zero and the max is not large enough") {
        val distribution = Distribution(min = 0L, p25 = 0L, median = 0L, p75 = 0L, max = 16000L)
        evaluator.severityOfDistribution(distribution, ignoreMaxLessThanThreshold = 16001L) should be(Severity.NONE)
      }
    }
  }
}

object ExecutorsHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeExecutorSummary(
    id: String,
    memoryUsed: Long,
    totalDuration: Long,
    totalInputBytes: Long,
    totalShuffleRead: Long,
    totalShuffleWrite: Long,
    maxMemory: Long
  ): ExecutorSummaryImpl = new ExecutorSummaryImpl(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed,
    diskUsed = 0,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks = 0,
    maxTasks = 0,
    totalDuration,
    totalInputBytes,
    totalShuffleRead,
    totalShuffleWrite,
    maxMemory,
    totalGCTime = 0,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory = Map.empty,
    peakUnifiedMemory = Map.empty
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
