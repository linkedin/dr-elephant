/*
 * Originally from
 * https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/status/api/v1/api.scala
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright 2016 LinkedIn Corp.
 *
 * To keep up to date, please copy
 * https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/status/api/v1/api.scala
 * and maintain in this package.
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
package com.linkedin.drelephant.spark.fetchers.statusapiv1

import java.util.Date

import scala.collection.Map

import org.apache.spark.JobExecutionStatus
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

trait ApplicationInfo {
  def id: String
  def name: String
  def attempts: Seq[ApplicationAttemptInfo]
}

trait ApplicationAttemptInfo{
  def attemptId: Option[String]
  def startTime: Date
  def endTime: Date
  def sparkUser: String
  def completed: Boolean
}

trait ExecutorStageSummary{
  def taskTime : Long
  def failedTasks : Int
  def succeededTasks : Int
  def inputBytes : Long
  def outputBytes : Long
  def shuffleRead : Long
  def shuffleWrite : Long
  def memoryBytesSpilled : Long
  def diskBytesSpilled : Long
}

trait ExecutorSummary{
  def id: String
  def hostPort: String
  def rddBlocks: Int
  def memoryUsed: Long
  def diskUsed: Long
  def activeTasks: Int
  def failedTasks: Int
  def completedTasks: Int
  def totalTasks: Int
  def maxTasks: Int
  def totalDuration: Long
  def totalInputBytes: Long
  def totalShuffleRead: Long
  def totalShuffleWrite: Long
  def maxMemory: Long
  def totalGCTime: Long
  def totalMemoryBytesSpilled: Long
  def executorLogs: Map[String, String]
  def peakJvmUsedMemory: Map[String, Long]
  def peakUnifiedMemory: Map[String, Long]
}

trait JobData{
  def jobId: Int
  def name: String
  def description: Option[String]
  def submissionTime: Option[Date]
  def completionTime: Option[Date]
  def stageIds: Seq[Int]
  def jobGroup: Option[String]
  def status: JobExecutionStatus
  def numTasks: Int
  def numActiveTasks: Int
  def numCompletedTasks: Int
  def numSkippedTasks: Int
  def numFailedTasks: Int
  def numActiveStages: Int
  def numCompletedStages: Int
  def numSkippedStages: Int
  def numFailedStages: Int}

// Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage
// page ... does anybody pay attention to it?
trait RDDStorageInfo{
  def id: Int
  def name: String
  def numPartitions: Int
  def numCachedPartitions: Int
  def storageLevel: String
  def memoryUsed: Long
  def diskUsed: Long
  def dataDistribution: Option[Seq[RDDDataDistribution]]
  def partitions: Option[Seq[RDDPartitionInfo]]}

trait RDDDataDistribution{
  def address: String
  def memoryUsed: Long
  def memoryRemaining: Long
  def diskUsed: Long}

trait RDDPartitionInfo{
  def blockName: String
  def storageLevel: String
  def memoryUsed: Long
  def diskUsed: Long
  def executors: Seq[String]}

trait StageData{
  def status: StageStatus
  def stageId: Int
  def attemptId: Int
  def numActiveTasks: Int
  def numCompleteTasks: Int
  def numFailedTasks: Int

  def executorRunTime: Long

  def inputBytes: Long
  def inputRecords: Long
  def outputBytes: Long
  def outputRecords: Long
  def shuffleReadBytes: Long
  def shuffleReadRecords: Long
  def shuffleWriteBytes: Long
  def shuffleWriteRecords: Long
  def memoryBytesSpilled: Long
  def diskBytesSpilled: Long

  def name: String
  def details: String
  def schedulingPool: String

  def accumulatorUpdates: Seq[AccumulableInfo]
  def tasks: Option[Map[Long, TaskDataImpl]]
  def executorSummary: Option[Map[String, ExecutorStageSummary]]}

trait TaskData{
  def taskId: Long
  def index: Int
  def attempt: Int
  def launchTime: Date
  def executorId: String
  def host: String
  def taskLocality: String
  def speculative: Boolean
  def accumulatorUpdates: Seq[AccumulableInfo]
  def errorMessage: Option[String]
  def taskMetrics: Option[TaskMetrics]}

trait TaskMetrics{
  def executorDeserializeTime: Long
  def executorRunTime: Long
  def resultSize: Long
  def jvmGcTime: Long
  def resultSerializationTime: Long
  def memoryBytesSpilled: Long
  def diskBytesSpilled: Long
  def inputMetrics: Option[InputMetrics]
  def outputMetrics: Option[OutputMetrics]
  def shuffleReadMetrics: Option[ShuffleReadMetrics]
  def shuffleWriteMetrics: Option[ShuffleWriteMetrics]}

trait InputMetrics{
  def bytesRead: Long
  def recordsRead: Long}

trait OutputMetrics{
  def bytesWritten: Long
  def recordsWritten: Long}

trait ShuffleReadMetrics{
  def remoteBlocksFetched: Int
  def localBlocksFetched: Int
  def fetchWaitTime: Long
  def remoteBytesRead: Long
  def totalBlocksFetched: Int
  def recordsRead: Long}

trait ShuffleWriteMetrics{
  def bytesWritten: Long
  def writeTime: Long
  def recordsWritten: Long}

trait TaskMetricDistributions{
  def quantiles: IndexedSeq[Double]

  def executorDeserializeTime: IndexedSeq[Double]
  def executorRunTime: IndexedSeq[Double]
  def resultSize: IndexedSeq[Double]
  def jvmGcTime: IndexedSeq[Double]
  def resultSerializationTime: IndexedSeq[Double]
  def memoryBytesSpilled: IndexedSeq[Double]
  def diskBytesSpilled: IndexedSeq[Double]

  def inputMetrics: Option[InputMetricDistributions]
  def outputMetrics: Option[OutputMetricDistributions]
  def shuffleReadMetrics: Option[ShuffleReadMetricDistributions]
  def shuffleWriteMetrics: Option[ShuffleWriteMetricDistributions]}

trait InputMetricDistributions{
  def bytesRead: IndexedSeq[Double]
  def recordsRead: IndexedSeq[Double]}

trait OutputMetricDistributions{
  def bytesWritten: IndexedSeq[Double]
  def recordsWritten: IndexedSeq[Double]}

trait ShuffleReadMetricDistributions{
  def readBytes: IndexedSeq[Double]
  def readRecords: IndexedSeq[Double]
  def remoteBlocksFetched: IndexedSeq[Double]
  def localBlocksFetched: IndexedSeq[Double]
  def fetchWaitTime: IndexedSeq[Double]
  def remoteBytesRead: IndexedSeq[Double]
  def totalBlocksFetched: IndexedSeq[Double]}

trait ShuffleWriteMetricDistributions{
  def writeBytes: IndexedSeq[Double]
  def writeRecords: IndexedSeq[Double]
  def writeTime: IndexedSeq[Double]}

trait AccumulableInfo{
  def id: Long
  def name: String
  def update: Option[String]
  def value: String}

class ApplicationInfoImpl(
  var id: String,
  var name: String,
  var attempts: Seq[ApplicationAttemptInfoImpl]) extends ApplicationInfo

class ApplicationAttemptInfoImpl(
  var attemptId: Option[String],
  var startTime: Date,
  var endTime: Date,
  var sparkUser: String,
  var completed: Boolean = false) extends ApplicationAttemptInfo

class ExecutorStageSummaryImpl(
  var taskTime : Long,
  var failedTasks : Int,
  var succeededTasks : Int,
  var inputBytes : Long,
  var outputBytes : Long,
  var shuffleRead : Long,
  var shuffleWrite : Long,
  var memoryBytesSpilled : Long,
  var diskBytesSpilled : Long) extends ExecutorStageSummary

class ExecutorSummaryImpl(
  var id: String,
  var hostPort: String,
  var rddBlocks: Int,
  var memoryUsed: Long,
  var diskUsed: Long,
  var activeTasks: Int,
  var failedTasks: Int,
  var completedTasks: Int,
  var totalTasks: Int,
  var maxTasks: Int,
  var totalDuration: Long,
  var totalInputBytes: Long,
  var totalShuffleRead: Long,
  var totalShuffleWrite: Long,
  var maxMemory: Long,
  var totalGCTime: Long,
  var totalMemoryBytesSpilled: Long,
  var executorLogs: Map[String, String],
  var peakJvmUsedMemory: Map[String, Long],
  var peakUnifiedMemory: Map[String, Long]) extends ExecutorSummary

class JobDataImpl(
  var jobId: Int,
  var name: String,
  var description: Option[String],
  var submissionTime: Option[Date],
  var completionTime: Option[Date],
  var stageIds: Seq[Int],
  var jobGroup: Option[String],
  var status: JobExecutionStatus,
  var numTasks: Int,
  var numActiveTasks: Int,
  var numCompletedTasks: Int,
  var numSkippedTasks: Int,
  var numFailedTasks: Int,
  var numActiveStages: Int,
  var numCompletedStages: Int,
  var numSkippedStages: Int,
  var numFailedStages: Int) extends JobData

// Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage
// page ... does anybody pay attention to it?
class RDDStorageInfoImpl(
  var id: Int,
  var name: String,
  var numPartitions: Int,
  var numCachedPartitions: Int,
  var storageLevel: String,
  var memoryUsed: Long,
  var diskUsed: Long,
  var dataDistribution: Option[Seq[RDDDataDistributionImpl]],
  var partitions: Option[Seq[RDDPartitionInfoImpl]]) extends RDDStorageInfo

class RDDDataDistributionImpl(
  var address: String,
  var memoryUsed: Long,
  var memoryRemaining: Long,
  var diskUsed: Long) extends RDDDataDistribution

class RDDPartitionInfoImpl(
  var blockName: String,
  var storageLevel: String,
  var memoryUsed: Long,
  var diskUsed: Long,
  var executors: Seq[String]) extends RDDPartitionInfo

class StageDataImpl(
  var status: StageStatus,
  var stageId: Int,
  var attemptId: Int,
  var numActiveTasks: Int ,
  var numCompleteTasks: Int,
  var numFailedTasks: Int,

  var executorRunTime: Long,

  var inputBytes: Long,
  var inputRecords: Long,
  var outputBytes: Long,
  var outputRecords: Long,
  var shuffleReadBytes: Long,
  var shuffleReadRecords: Long,
  var shuffleWriteBytes: Long,
  var shuffleWriteRecords: Long,
  var memoryBytesSpilled: Long,
  var diskBytesSpilled: Long,

  var name: String,
  var details: String,
  var schedulingPool: String,

  var accumulatorUpdates: Seq[AccumulableInfoImpl],
  var tasks: Option[Map[Long, TaskDataImpl]],
  var executorSummary: Option[Map[String, ExecutorStageSummaryImpl]]) extends StageData

class TaskDataImpl(
  var taskId: Long,
  var index: Int,
  var attempt: Int,
  var launchTime: Date,
  var executorId: String,
  var host: String,
  var taskLocality: String,
  var speculative: Boolean,
  var accumulatorUpdates: Seq[AccumulableInfoImpl],
  var errorMessage: Option[String] = None,
  var taskMetrics: Option[TaskMetricsImpl] = None) extends TaskData

class TaskMetricsImpl(
  var executorDeserializeTime: Long,
  var executorRunTime: Long,
  var resultSize: Long,
  var jvmGcTime: Long,
  var resultSerializationTime: Long,
  var memoryBytesSpilled: Long,
  var diskBytesSpilled: Long,
  var inputMetrics: Option[InputMetricsImpl],
  var outputMetrics: Option[OutputMetricsImpl],
  var shuffleReadMetrics: Option[ShuffleReadMetricsImpl],
  var shuffleWriteMetrics: Option[ShuffleWriteMetricsImpl]) extends TaskMetrics

class InputMetricsImpl(
  var bytesRead: Long,
  var recordsRead: Long) extends InputMetrics

class OutputMetricsImpl(
  var bytesWritten: Long,
  var recordsWritten: Long) extends OutputMetrics

class ShuffleReadMetricsImpl(
  var remoteBlocksFetched: Int,
  var localBlocksFetched: Int,
  var fetchWaitTime: Long,
  var remoteBytesRead: Long,
  var totalBlocksFetched: Int,
  var recordsRead: Long) extends ShuffleReadMetrics

class ShuffleWriteMetricsImpl(
  var bytesWritten: Long,
  var writeTime: Long,
  var recordsWritten: Long) extends ShuffleWriteMetrics

class TaskMetricDistributionsImpl(
  var quantiles: IndexedSeq[Double],

  var executorDeserializeTime: IndexedSeq[Double],
  var executorRunTime: IndexedSeq[Double],
  var resultSize: IndexedSeq[Double],
  var jvmGcTime: IndexedSeq[Double],
  var resultSerializationTime: IndexedSeq[Double],
  var memoryBytesSpilled: IndexedSeq[Double],
  var diskBytesSpilled: IndexedSeq[Double],

  var inputMetrics: Option[InputMetricDistributionsImpl],
  var outputMetrics: Option[OutputMetricDistributionsImpl],
  var shuffleReadMetrics: Option[ShuffleReadMetricDistributionsImpl],
  var shuffleWriteMetrics: Option[ShuffleWriteMetricDistributionsImpl]) extends TaskMetricDistributions

class InputMetricDistributionsImpl(
  var bytesRead: IndexedSeq[Double],
  var recordsRead: IndexedSeq[Double]) extends InputMetricDistributions

class OutputMetricDistributionsImpl(
  var bytesWritten: IndexedSeq[Double],
  var recordsWritten: IndexedSeq[Double]) extends OutputMetricDistributions

class ShuffleReadMetricDistributionsImpl(
  var readBytes: IndexedSeq[Double],
  var readRecords: IndexedSeq[Double],
  var remoteBlocksFetched: IndexedSeq[Double],
  var localBlocksFetched: IndexedSeq[Double],
  var fetchWaitTime: IndexedSeq[Double],
  var remoteBytesRead: IndexedSeq[Double],
  var totalBlocksFetched: IndexedSeq[Double]) extends ShuffleReadMetricDistributions

class ShuffleWriteMetricDistributionsImpl(
  var writeBytes: IndexedSeq[Double],
  var writeRecords: IndexedSeq[Double],
  var writeTime: IndexedSeq[Double]) extends ShuffleWriteMetricDistributions

class AccumulableInfoImpl(
  var id: Long,
  var name: String,
  var update: Option[String],
  var value: String) extends AccumulableInfo
