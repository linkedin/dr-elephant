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

package com.linkedin.drelephant.spark

import java.util.Date

import scala.collection.JavaConverters

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfoImpl, ApplicationInfoImpl, ExecutorSummaryImpl}
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.commons.io.FileUtils
import org.scalatest.{FunSpec, Matchers}

class SparkMetricsAggregatorTest extends FunSpec with Matchers {
  import SparkMetricsAggregatorTest._

  describe("SparkMetricsAggregator") {
    val aggregatorConfigurationData = newFakeAggregatorConfigurationData(
      Map("allocated_memory_waste_buffer_percentage" -> "0.5")
    )

    val appId = "application_1"

    val applicationInfo = {
      val applicationAttemptInfo = {
        val now = System.currentTimeMillis
        val duration = 8000000L
        newFakeApplicationAttemptInfo(Some("1"), startTime = new Date(now - duration), endTime = new Date(now))
      }
      new ApplicationInfoImpl(appId, name = "app", Seq(applicationAttemptInfo))
    }

    val executorSummaries = Seq(
      newFakeExecutorSummary(id = "1", totalDuration = 1000000L, Map("jvmUsedMemory" -> 394567123)),
      newFakeExecutorSummary(id = "2", totalDuration = 3000000L, Map("jvmUsedMemory" -> 23456834))

    )
    val restDerivedData = {
      SparkRestDerivedData(
        applicationInfo,
        jobDatas = Seq.empty,
        stageDatas = Seq.empty,
        executorSummaries = executorSummaries,
        stagesWithFailedTasks = Seq.empty
      )
    }

    describe("when it has data") {
      val logDerivedData = {
        val environmentUpdate = newFakeSparkListenerEnvironmentUpdate(
          Map(
            "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
            "spark.storage.memoryFraction" -> "0.3",
            "spark.driver.memory" -> "2G",
            "spark.executor.instances" -> "2",
            "spark.executor.memory" -> "4g",
            "spark.shuffle.memoryFraction" -> "0.5"
          )
        )
        SparkLogDerivedData(environmentUpdate)
      }

      val data = SparkApplicationData(appId, restDerivedData, Some(logDerivedData))

      val aggregator = new SparkMetricsAggregator(aggregatorConfigurationData)
      aggregator.aggregate(data)

      val result = aggregator.getResult

      it("calculates resources used (allocated)") {
        result.getResourceUsed should be(4096000+12288000)
      }

      it("calculates resources wasted") {
        val resourceAllocated = 4096000+12288000
        val resourceUsed = 676288+967110
        result.getResourceWasted should be(resourceAllocated.toDouble - resourceUsed.toDouble * 1.5)
      }

      it("doesn't calculate total delay") {
        result.getTotalDelay should be(0L)
      }
      it("sets resource used as 0 when duration is negative") {
        //make the duration negative
        val applicationInfo = {
          val applicationAttemptInfo = {
            val now = System.currentTimeMillis
            val duration = -8000000L
            newFakeApplicationAttemptInfo(Some("1"), startTime = new Date(now - duration), endTime = new Date(now))
          }
          new ApplicationInfoImpl(appId, name = "app", Seq(applicationAttemptInfo))
        }
        val restDerivedData = SparkRestDerivedData(
            applicationInfo,
            jobDatas = Seq.empty,
            stageDatas = Seq.empty,
            executorSummaries = executorSummaries,
            stagesWithFailedTasks = Seq.empty
          )

        val data = SparkApplicationData(appId, restDerivedData, Some(logDerivedData))

        val aggregator = new SparkMetricsAggregator(aggregatorConfigurationData)
        aggregator.aggregate(data)

        val result = aggregator.getResult
        result.getResourceUsed should be(0L)
      }
    }

    describe("when it doesn't have log-derived data") {
      val data = SparkApplicationData(appId, restDerivedData, logDerivedData = None)

      val aggregator = new SparkMetricsAggregator(aggregatorConfigurationData)
      aggregator.aggregate(data)

      val result = aggregator.getResult

      it("doesn't calculate resources used") {
        result.getResourceUsed should be(0L)
      }

      it("doesn't calculate resources wasted") {
        result.getResourceWasted should be(0L)
      }

      it("doesn't calculate total delay") {
        result.getTotalDelay should be(0L)
      }
    }
  }
}

object SparkMetricsAggregatorTest {
  import JavaConverters._

  def newFakeAggregatorConfigurationData(params: Map[String, String] = Map.empty): AggregatorConfigurationData =
      new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", new ApplicationType("SPARK"), params.asJava)

  def newFakeSparkListenerEnvironmentUpdate(appConfigurationProperties: Map[String, String]): SparkListenerEnvironmentUpdate =
    SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))

  def newFakeApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Date,
    endTime: Date
  ): ApplicationAttemptInfoImpl = new ApplicationAttemptInfoImpl(
    attemptId,
    startTime,
    endTime,
    sparkUser = "foo",
    completed = true
  )

  def newFakeExecutorSummary(
    id: String,
    totalDuration: Long,
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
    totalDuration,
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
}
