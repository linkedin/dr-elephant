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

package com.linkedin.drelephant.spark.legacydata

import java.util.Date

import scala.collection.JavaConverters
import scala.util.Try

import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.spark.JobExecutionStatus
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus

/**
  * Converters for legacy SparkApplicationData to current SparkApplicationData.
  *
  * The converters make a best effort, providing default values for attributes the legacy data doesn't provide.
  * In practice, the Dr. Elephant Spark heuristics end up using a relatively small subset of the converted data.
  */
object LegacyDataConverters {
  import JavaConverters._

  //Returns a default object. This default object is returned if logs are processed locally i.e. in case FS fetcher is being used.
  def extractStagesWithFailedTasks(legacyData: SparkApplicationData): scala.Seq[StageData] = {
    Seq(new StageData {
      override def numCompleteTasks: Int = 0
      override def inputRecords: Long = 0
      override def shuffleReadBytes: Long = 0
      override def shuffleWriteBytes: Long = 0
      override def schedulingPool: String = ""
      override def outputRecords: Long = 0
      override def shuffleWriteRecords: Long = 0
      override def inputBytes: Long = 0
      override def details: String = ""
      override def tasks = None
      override def attemptId: Int = 0
      override def stageId: Int = 0
      override def memoryBytesSpilled: Long = 0
      override def executorRunTime: Long = 0
      override def shuffleReadRecords: Long = 0
      override def outputBytes: Long = 0
      override def numActiveTasks: Int = 0
      override def diskBytesSpilled: Long = 0
      override def numFailedTasks: Int = 0
      override def accumulatorUpdates: Seq[AccumulableInfo] = Seq.empty
      override def name: String = ""
      override def executorSummary: Option[collection.Map[String, ExecutorStageSummary]] = None
      override def status = StageStatus.COMPLETE
    })
  }

  def convert(legacyData: SparkApplicationData): com.linkedin.drelephant.spark.data.SparkApplicationData = {
    com.linkedin.drelephant.spark.data.SparkApplicationData(
      legacyData.getAppId,
      extractAppConfigurationProperties(legacyData),
      extractApplicationInfo(legacyData),
      extractJobDatas(legacyData),
      extractStageDatas(legacyData),
      extractExecutorSummaries(legacyData),
      extractStagesWithFailedTasks(legacyData)
    )
  }

  def extractAppConfigurationProperties(legacyData: SparkApplicationData): Map[String, String] =
    legacyData.getEnvironmentData.getSparkProperties.asScala.toMap

  def extractApplicationInfo(legacyData: SparkApplicationData): ApplicationInfoImpl = {
    val generalData = legacyData.getGeneralData
    new ApplicationInfoImpl(
      generalData.getApplicationId,
      generalData.getApplicationName,
      Seq(
        new ApplicationAttemptInfoImpl(
          Some("1"),
          new Date(generalData.getStartTime),
          new Date(generalData.getEndTime),
          generalData.getSparkUser,
          completed = true
        )
      )
    )
  }

  def extractJobDatas(legacyData: SparkApplicationData): Seq[JobDataImpl] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractJobData(jobId: Int): JobDataImpl = {
      val jobInfo = jobProgressData.getJobInfo(jobId)
      new JobDataImpl(
        jobInfo.jobId,
        jobInfo.jobId.toString,
        description = None,
        submissionTime = None,
        completionTime = None,
        jobInfo.stageIds.asScala.map { _.toInt },
        Option(jobInfo.jobGroup),
        extractJobExecutionStatus(jobId),
        jobInfo.numTasks,
        jobInfo.numActiveTasks,
        jobInfo.numCompletedTasks,
        jobInfo.numSkippedTasks,
        jobInfo.numFailedTasks,
        jobInfo.numActiveStages,
        jobInfo.completedStageIndices.size(),
        jobInfo.numSkippedStages,
        jobInfo.numFailedStages
      )
    }

    def extractJobExecutionStatus(jobId: Int): JobExecutionStatus = {
      if (jobProgressData.getCompletedJobs.contains(jobId)) {
        JobExecutionStatus.SUCCEEDED
      } else if (jobProgressData.getFailedJobs.contains(jobId)) {
        JobExecutionStatus.FAILED
      } else {
        JobExecutionStatus.UNKNOWN
      }
    }

    val sortedJobIds = jobProgressData.getJobIds.asScala.toSeq.sorted
    sortedJobIds.map { jobId => extractJobData(jobId) }
  }

  def extractStageDatas(legacyData: SparkApplicationData): Seq[StageData] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractStageData(stageAttemptId: SparkJobProgressData.StageAttemptId): StageDataImpl = {
      val stageInfo = jobProgressData.getStageInfo(stageAttemptId.stageId, stageAttemptId.attemptId)
      new StageDataImpl(
        extractStageStatus(stageAttemptId),
        stageAttemptId.stageId,
        stageAttemptId.attemptId,
        stageInfo.numActiveTasks,
        stageInfo.numCompleteTasks,
        stageInfo.numFailedTasks,
        stageInfo.executorRunTime,
        stageInfo.inputBytes,
        inputRecords = 0,
        stageInfo.outputBytes,
        outputRecords = 0,
        stageInfo.shuffleReadBytes,
        shuffleReadRecords = 0,
        stageInfo.shuffleWriteBytes,
        shuffleWriteRecords = 0,
        stageInfo.memoryBytesSpilled,
        stageInfo.diskBytesSpilled,
        stageInfo.name,
        stageInfo.description,
        schedulingPool = "",
        accumulatorUpdates = Seq.empty,
        tasks = None,
        executorSummary = None
      )
    }

    def extractStageStatus(stageAttemptId: SparkJobProgressData.StageAttemptId): StageStatus = {
      if (jobProgressData.getCompletedStages.contains(stageAttemptId)) {
        StageStatus.COMPLETE
      } else if (jobProgressData.getFailedStages.contains(stageAttemptId)) {
        StageStatus.FAILED
      } else {
        StageStatus.PENDING
      }
    }

    val sortedStageAttemptIds = jobProgressData.getStageAttemptIds.asScala.toSeq.sortBy { stageAttemptId =>
      (stageAttemptId.stageId, stageAttemptId.attemptId)
    }
    sortedStageAttemptIds.map { stageAttemptId => extractStageData(stageAttemptId) }
  }

  def extractExecutorSummaries(legacyData: SparkApplicationData): Seq[ExecutorSummaryImpl] = {
    val executorData = legacyData.getExecutorData

    def extractExecutorSummary(executorId: String): ExecutorSummaryImpl = {
      val executorInfo = executorData.getExecutorInfo(executorId)
      new ExecutorSummaryImpl(
        executorInfo.execId,
        executorInfo.hostPort,
        executorInfo.rddBlocks,
        executorInfo.memUsed,
        executorInfo.diskUsed,
        executorInfo.activeTasks,
        executorInfo.failedTasks,
        executorInfo.completedTasks,
        executorInfo.totalTasks,
        executorInfo.maxTasks,
        executorInfo.duration,
        executorInfo.inputBytes,
        executorInfo.shuffleRead,
        executorInfo.shuffleWrite,
        executorInfo.maxMem,
        executorInfo.totalGCTime,
        executorInfo.totalMemoryBytesSpilled,
        executorLogs = Map.empty,
        peakJvmUsedMemory = Map.empty,
        peakUnifiedMemory = Map.empty
      )
    }

    val sortedExecutorIds = {
      val executorIds = executorData.getExecutors.asScala.toSeq
      Try(executorIds.sortBy { _.toInt }).getOrElse(executorIds.sorted)
    }
    sortedExecutorIds.map { executorId => extractExecutorSummary(executorId) }
  }
}
