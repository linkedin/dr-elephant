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

package com.linkedin.drelephant.spark.exception

import java.text.SimpleDateFormat
import java.util.Date

import com.linkedin.drelephant.ElephantContext
import com.linkedin.drelephant.analysis.{AnalyticJob, ApplicationType}
import com.linkedin.drelephant.exceptions.core.ExceptionFingerprintingSpark
import com.linkedin.drelephant.spark.fetchers.SparkFetcher
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{StageDataImpl, StageStatus}
import com.linkedin.drelephant.spark.heuristics.ConfigurationHeuristicsConstants._
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities.createExecutorSummary
import common.TestConstants._

private [spark] object ExceptionFingerprintingSparkUtilities {
  private val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  def createStage (stageID : Int, status: StageStatus, failureReason : Option[String], details : String )= {
    new StageDataImpl(
      status,
      stageID,
      attemptId = 0,
      numTasks = 10,
      numActiveTasks = 4,
      numCompleteTasks = 6,
      numFailedTasks = 0,
      executorRunTime = 0,
      executorCpuTime = 0,
      submissionTime = Some(sdf.parse("09/09/2018 12:00:00")),
      firstTaskLaunchedTime = None,
      completionTime = Some(sdf.parse("09/09/2018 12:05:00")),
      failureReason,
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
      name = "test",
      details,
      schedulingPool = "",
      accumulatorUpdates = Seq.empty,
      tasks = None,
      executorSummary = None,
      peakJvmUsedMemory = None,
      peakExecutionMemory = None,
      peakStorageMemory = None,
      peakUnifiedMemory = None,
      taskSummary = None,
      executorMetricsSummary = None
    )
  }

  def getProperties ()={
     Map(
      SPARK_EXECUTOR_MEMORY -> "4G",
      SPARK_DRIVER_MEMORY -> "6G",
      SPARK_EXECUTOR_MEMORY_OVERHEAD -> "1G",
      SPARK_DRIVER_MEMORY_OVERHEAD -> "2G",
      SPARK_EXECUTOR_CORES -> "1",
      SPARK_DRIVER_CORES -> "1",
      SPARK_EXECUTOR_INSTANCES -> "200",
      SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
      SPARK_MEMORY_FRACTION -> "0.1"
    )
  }

  def getExecutorSummary () ={
     Seq(
      createExecutorSummary("driver", 200, 3, 300),
      createExecutorSummary("1", 800, 3, 300)
    )
  }

  def checkTye(instance : AnyRef) ={
    instance match {
      case s : ExceptionFingerprintingSpark => "ExceptionFingerprintingSpark"
      case _  => "ClassNotFound"
    }
  }

  def checkException(instance: AnyRef) ={
    instance match {
      case s : Exception => "Exception"
      case _ => "unexpected"
    }
  }

  def getAnalyticalJob(isSucceeded : Boolean, amContainerLogsURL : String, amHostHttpAddress : String)={
    new AnalyticJob()
      .setAppId(TEST_JOB_ID1)
      .setFinishTime(1462178403)
      .setStartTime(1462178412)
      .setName(TEST_JOB_NAME)
      .setQueueName(TEST_DEFAULT_QUEUE_NAME)
      .setUser(TEST_USERNAME)
      .setTrackingUrl(TEST_TRACKING_URL)
      .setSucceeded(isSucceeded)
      .setAmContainerLogsURL(amContainerLogsURL)
      .setAmHostHttpAddress(amHostHttpAddress)
      .setAppType(new ApplicationType("spark"))
  }
}
