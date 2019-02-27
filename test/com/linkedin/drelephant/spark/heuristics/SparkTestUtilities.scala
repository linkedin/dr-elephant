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

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate

import scala.collection.JavaConverters

private [spark] object SparkTestUtilities {
  import JavaConverters._
  import java.text.SimpleDateFormat

  val OOM_ERROR = "java.lang.OutOfMemoryError"
  val OVERHEAD_MEMORY_ERROR = "killed by YARN for exceeding memory limits"

  private val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  /** Create a ample heuristics configuration data. */
  def createHeuristicConfigurationData(
      params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  /**
    * Builder for creating a StageAnalysis.
    *
    * @param stageId stage ID.
    * @param numTasks total number of tasks for the stage.
    */
  case class StageAnalysisBuilder(stageId: Int, numTasks: Int) {
    var rawSpillSeverity = Severity.NONE
    var executionSpillSeverity = Severity.NONE
    var longTaskSeverity = Severity.NONE
    var rawSkewSeverity = Severity.NONE
    var taskSkewSeverity = Severity.NONE
    var failedWithOOMSeverity = Severity.NONE
    var failedWithContainerKilledSeverity = Severity.NONE
    var gcSeverity = Severity.NONE
    var taskFailureSeverity = Severity.NONE
    var stageFailureSeverity = Severity.NONE
    var spillScore = 0
    var longTaskScore = 0
    var taskSkewScore = 0
    var taskFailureScore = 0
    var stageFailureScore = 0
    var gcScore = 0
    var medianRunTime: Option[Double] = None
    var maxRunTime: Option[Double] = None
    var memoryBytesSpilled = 0L
    var maxTaskBytesSpilled = 0L
    var inputBytes: Long = 0L
    var outputBytes: Long = 0L
    var shuffleReadBytes: Long = 0L
    var shuffleWriteBytes: Long = 0L
    var numFailedTasks = 0
    var numTasksWithOOM = 0
    var numTasksWithContainerKilled = 0
    var stageDuration = Some((5 * 60 * 1000).toLong)
    var spillDetails: Seq[String] = Seq()
    var longTaskDetails: Seq[String] = Seq()
    var taskSkewDetails: Seq[String] = Seq()
    var taskFailureDetails: Seq[String] = Seq()
    var stageFailureDetails: Seq[String] = Seq()
    var gcDetails: Seq[String] = Seq()

    /**
      * Configure execution memory spill related parameters.
      *
      * @param raw the raw execution memory spill severity.
      * @param severity the reported execution memory spill severity.
      * @param maxTaskSpillMb maximum amount (MB) of execution memory spill for a task.
      * @param bytesSpilledMb total amount (MB) of execution memory spill.
      * @return this StageAnalysisBuilder.
      */
    def spill(
        raw: Severity,
        severity: Severity,
        score: Int,
        maxTaskSpillMb: Long,
        bytesSpilledMb: Long,
        details: Seq[String]): StageAnalysisBuilder = {
      rawSpillSeverity = raw
      executionSpillSeverity = severity
      spillScore = score
      maxTaskBytesSpilled = maxTaskSpillMb << 20
      memoryBytesSpilled = bytesSpilledMb << 20
      spillDetails = details
      this
    }

    /** Set the amount of input data in MB. */
    def input(inputMb: Long): StageAnalysisBuilder = {
      inputBytes = inputMb << 20
      this
    }

    /** Set the amount of output data in MB. */
    def output(outputMb: Long): StageAnalysisBuilder = {
      outputBytes = outputMb << 20
      this
    }

    /** Set the amount of shuffle read data in MB. */
    def shuffleRead(shuffleReadMb: Long): StageAnalysisBuilder = {
      shuffleReadBytes = shuffleReadMb << 20
      this
    }

    /** Set the amount of shuffle write data in MB. */
    def shuffleWrite(shuffleWriteMb: Long): StageAnalysisBuilder = {
      shuffleWriteBytes = shuffleWriteMb << 20
      this
    }

    /** Set the stage duration. */
    def duration(sec: Long): StageAnalysisBuilder = {
      stageDuration = Some(sec * 1000)
      this
    }

    /** Set the median and max task runtimes in seconds */
    def taskRuntime(median: Double, maximum: Double): StageAnalysisBuilder = {
      medianRunTime = Some(median * 1000)
      maxRunTime = Some(maximum * 1000)
      this
    }

    /** set the long task analysis information */
    def longTask(severity: Severity, score: Int, details: Seq[String]): StageAnalysisBuilder = {
      longTaskSeverity = severity
      longTaskScore = score
      longTaskDetails = details
      this
    }

    /** set the raw and reported task skew severity and details */
    def skew(
        raw: Severity,
        severity: Severity,
        score: Int,
        details: Seq[String]): StageAnalysisBuilder = {
      rawSkewSeverity = raw
      taskSkewSeverity = severity
      taskSkewScore = score
      taskSkewDetails = details
      this
    }

    /**
      * Configure stage failure information.
      *
      * @param severity severity of stage failure.
      * @param score score for stage failure analysis
      * @param details information and recommendations
      * @return
      */
    def stageFailure(severity: Severity,
                     score: Int,
                     details: Seq[String]): StageAnalysisBuilder = {
      stageFailureSeverity = severity
      stageFailureScore = score
      stageFailureDetails = details
      this
    }

    /**
      * Configure task failure information.
      *
      * @param taskSeverity severity of all task failures.
      * @param oomSeverity severity of task failures due to OutOfMemory errors.
      * @param containerKilledSeverity severity of failures due to containers killed by YARN.
      * @param score score from task failure analysis.
      * @param numFailures total number of task failures.
      * @param numOOM total number of tasks failed with OutOfMemory errors.
      * @param numContainerKilled total number of tasks failed due to container killed by YARN.
      * @param details information and recommendations for task failures
      * @return this StageAnalysisBuilder.
      */
    def taskFailures(
        taskSeverity: Severity,
        oomSeverity: Severity,
        containerKilledSeverity: Severity,
        score: Int,
        numFailures: Int,
        numOOM: Int,
        numContainerKilled: Int,
        details: Seq[String]): StageAnalysisBuilder = {
      taskFailureSeverity = taskSeverity
      failedWithOOMSeverity = oomSeverity
      failedWithContainerKilledSeverity = containerKilledSeverity
      taskFailureScore = score
      numFailedTasks = numFailures
      numTasksWithOOM = numOOM
      numTasksWithContainerKilled = numContainerKilled
      taskFailureDetails = details
      this
    }

    /** Create the StageAnalysis. */
    def create(): StageAnalysis = {
      StageAnalysis(
        stageId,
        ExecutionMemorySpillResult(executionSpillSeverity, spillScore, spillDetails,
          rawSpillSeverity, memoryBytesSpilled, maxTaskBytesSpilled),
        SimpleStageAnalysisResult(longTaskSeverity, longTaskScore, longTaskDetails),
        TaskSkewResult(taskSkewSeverity, taskSkewScore, taskSkewDetails, rawSkewSeverity),
        TaskFailureResult(taskFailureSeverity, taskFailureScore, taskFailureDetails,
          failedWithOOMSeverity, failedWithContainerKilledSeverity, numFailedTasks,
          numTasksWithOOM, numTasksWithContainerKilled),
        SimpleStageAnalysisResult(stageFailureSeverity, stageFailureScore, stageFailureDetails),
        numTasks, medianRunTime, maxRunTime, stageDuration, inputBytes, outputBytes,
        shuffleReadBytes, shuffleWriteBytes)
    }
  }

  /**
    * Builder for creating StageData.
    *
    * @param stageId stage ID
    * @param numTasks total number of tasks for the stage.
    */
  case class StageBuilder(stageId: Int, numTasks: Int) {
    val stage = new StageDataImpl(
      StageStatus.COMPLETE,
      stageId,
      attemptId = 0,
      numTasks = numTasks,
      numActiveTasks = numTasks,
      numCompleteTasks = numTasks,
      numFailedTasks = 0,
      executorRunTime = 0,
      executorCpuTime = 0,
      submissionTime = Some(sdf.parse("09/09/2018 12:00:00")),
      firstTaskLaunchedTime = None,
      completionTime = Some(sdf.parse("09/09/2018 12:05:00")),
      failureReason = None,

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
      details = "stage details",
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

    /** Create the specified number of tasks for the stage. */
    private def createTasks(numTasks: Int): Map[Long, TaskDataImpl] = {
      (1 until (numTasks + 1)).map { i =>
        (i.toLong, new TaskDataImpl(
          taskId = i.toLong,
          index = 1,
          attempt = 0,
          launchTime = new Date(),
          executorId = "1",
          host = "SomeHost",
          taskLocality = "ANY",
          speculative = false,
          accumulatorUpdates = Seq(),
          errorMessage = None,
          taskMetrics = None))
      }.toMap
    }

    /** Set the stage status. */
    def status(stageStatus: StageStatus, reason: Option[String]): StageBuilder = {
      stage.status = stageStatus
      stage.failureReason = reason
      this
    }

    /**
      * Set the run times.
      *
      * @param medianS median task run time in seconds.
      * @param maxS maximum task runtime in seconds.
      * @param totalS total runtime for all tasks.
      * @return this StageBuilder.
      */
    def taskRuntime(medianS: Int, maxS: Int, totalS: Int): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianMs = (medianS  * 1000).toDouble
      val maxMs = (maxS * 1000).toDouble
      taskMetricDistributions.executorRunTime =
        IndexedSeq(medianMs/2, medianMs, medianMs, medianMs, maxMs)
      stage.executorRunTime = totalS * 1000
      this
    }

    /**
      * Set the input information.
      *
      * @param medianMB median amount of input read for a task in MB.
      * @param maxMB maximum amount of input read for a task in MB.
      * @param totalMB total amount of input read for the stage in MB.
      * @return this StageBuilder.
      */
    def input(medianMB: Long, maxMB: Long, totalMB: Long): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianBytes = (medianMB << 20).toDouble
      val maxBytes = (maxMB << 20).toDouble
      taskMetricDistributions.inputMetrics =
        Some(new InputMetricDistributionsImpl(
          IndexedSeq(medianBytes/2, medianBytes, medianBytes, medianBytes, maxBytes),
          IndexedSeq(1000.0, 2000.0, 2000.0, 2000.0, 3000.0)))
      stage.inputBytes = totalMB << 20
      this
    }

    /**
      * Set the output information.
      *
      * @param medianMB median amount of output written for a task in MB.
      * @param maxMB maximum amount of output written for a task in MB.
      * @param totalMB total amount of output written for the stage in MB.
      * @return this StageBuilder.
      */
    def output(medianMB: Long, maxMB: Long, totalMB: Long): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianBytes = (medianMB << 20).toDouble
      val maxBytes = (maxMB << 20).toDouble
      taskMetricDistributions.outputMetrics =
        Some(new OutputMetricDistributionsImpl(
          IndexedSeq(medianBytes/2, medianBytes, medianBytes, medianBytes, maxBytes),
          IndexedSeq(1000.0, 2000.0, 2000.0, 2000.0, 3000.0)))
      stage.outputBytes = totalMB << 20
      this
    }

    /**
      * Set the shuffle read information.
      *
      * @param medianMB median amount of shuffle read for a task in MB.
      * @param maxMB maximum amount of shuffle read for a task in MB.
      * @param totalMB total amount of shuffle read for the stage in MB.
      * @return this StageBuilder.
      */
    def shuffleRead(medianMB: Long, maxMB: Long, totalMB: Long): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianBytes = (medianMB << 20).toDouble
      val maxBytes = (maxMB << 20).toDouble
      taskMetricDistributions.shuffleReadMetrics =
        Some(new ShuffleReadMetricDistributionsImpl(
          IndexedSeq(medianBytes/2, medianBytes, medianBytes, medianBytes, maxBytes),
          IndexedSeq(1000.0, 2000.0, 2000.0, 2000.0, 3000.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0)))
      stage.shuffleReadBytes = totalMB << 20
      this
    }

    /**
      * Set the shuffle write information.
      *
      * @param medianMB median amount of shuffle write for a task in MB.
      * @param maxMB maximum amount of shuffle write for a task in MB.
      * @param totalMB total amount of shuffle write for the stage in MB.
      * @return this StageBuilder.
      */
    def shuffleWrite(medianMB: Long, maxMB: Long, totalMB: Long): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianBytes = (medianMB << 20).toDouble
      val maxBytes = (maxMB << 20).toDouble
      taskMetricDistributions.shuffleWriteMetrics =
        Some(new ShuffleWriteMetricDistributionsImpl(
          IndexedSeq(medianBytes/2, medianBytes, medianBytes, medianBytes, maxBytes),
          IndexedSeq(1000.0, 2000.0, 2000.0, 2000.0, 3000.0),
          IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0)))
      stage.shuffleWriteBytes = totalMB << 20
      this
    }

    /**
      * Set the execution memory spill information.
      *
      * @param medianMB median amount of execution memory spill for a task in MB.
      * @param maxMB maximum amount of iexecution memory spill for a task in MB.
      * @param totalMB total amount of execution memory spill for the stage in MB.
      * @return this StageBuilder.
      */
    def spill(medianMB: Long, maxMB: Long, totalMB: Long): StageBuilder = {
      val taskMetricDistributions = getTaskMetricDistributions()
      val medianBytes = (medianMB << 20).toDouble
      val maxBytes = (maxMB << 20).toDouble
      taskMetricDistributions.memoryBytesSpilled =
        IndexedSeq(medianBytes/2, medianBytes, medianBytes, medianBytes, maxBytes)
      stage.memoryBytesSpilled = totalMB << 20
      this
    }

    /**
      * Set the failure information.
      *
      * @param numFailed total number of tasks failed.
      * @param numOOM total number of tasks which failed due to OutOfMemory.
      * @param numContainerKilled total number of ask which failed due to container killed by YARN.
      * @return this StageBuilder.
      */
    def failures(numFailed: Int, numOOM: Int, numContainerKilled: Int): StageBuilder = {
      stage.tasks = Some(createTasks(numFailed))
      (1 until (numOOM + 1)).map { i =>
        stage.tasks.get(i.toLong).errorMessage = Some(OOM_ERROR)
      }
      ((numOOM + 1) until (numOOM + numContainerKilled + 1)).map { i =>
        stage.tasks.get(i.toLong).errorMessage = Some(OVERHEAD_MEMORY_ERROR)
      }
      ((numOOM + numContainerKilled + 1) until numFailed + 1).map { i =>
        stage.tasks.get(i.toLong).errorMessage = Some("ArrayIndexOutOfBoundsException")
      }
      stage.numFailedTasks = numFailed
      this
    }

    /** Set the stage submission and completion times. */
    def times(submissionTime: String, completionTime: String): StageBuilder = {
      stage.submissionTime = Some(sdf.parse(submissionTime))
      stage.completionTime = Some(sdf.parse(completionTime))
      this
    }

    /** Create the StageDataImpl. */
    def create(): StageDataImpl = stage

    /** @return a askMetricDistributionsImpl for the StageData, creating it if needed. */
    private def getTaskMetricDistributions(): TaskMetricDistributionsImpl = {
      stage.taskSummary match {
        case None =>
          val taskMetricDistributions =
            new TaskMetricDistributionsImpl(
              quantiles = IndexedSeq(0.0, 0.25, 0.5, 0.75, 1.0),
              executorDeserializeTime = IndexedSeq(0.0, 0.0, 0.1, 0.1, 0.2),
              executorDeserializeCpuTime = IndexedSeq(0.0, 0.0, 0.1, 0.1, 0.2),
              executorRunTime = IndexedSeq(1000.0, 5000.0, 6000.0, 6500.0, 7000.0),
              executorCpuTime = IndexedSeq(1000.0, 5000.0, 6000.0, 6500.0, 7000.0),
              resultSize = IndexedSeq(0.0, 0.0, 0.0, 0.0),
              jvmGcTime = IndexedSeq(0.0, 0.0, 0.0, 0.0),
              resultSerializationTime = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              gettingResultTime = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              schedulerDelay = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              peakExecutionMemory = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              memoryBytesSpilled = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              diskBytesSpilled = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0),
              inputMetrics = None,
              outputMetrics = None,
              shuffleReadMetrics = None,
              shuffleWriteMetrics = None)
          stage.taskSummary = Some(taskMetricDistributions)
          taskMetricDistributions
        case Some(taskMetricDistributions) =>
          taskMetricDistributions
      }
    }
  }

  /**
    * Create an  executor metrics summary.
    *
    * @param id executor ID
    * @param jvmUsedMemoryMb peak JVM used memory for the executor.
    * @param totalGCTimeSec total time spent in GC by the executor.
    * @param totalDurationSec total task runtime for the executor.
    * @return executor summary.
    */
  private[spark] def createExecutorSummary(
      id: String,
      jvmUsedMemoryMb: Long,
      totalGCTimeSec: Long,
      totalDurationSec: Long): ExecutorSummaryImpl = new ExecutorSummaryImpl(
    id,
    hostPort = "",
    rddBlocks = 0,
    memoryUsed=0,
    diskUsed = 0,
    totalCores = 1,
    activeTasks = 0,
    failedTasks = 0,
    completedTasks = 0,
    totalTasks = 0,
    maxTasks = 0,
    totalDurationSec * 1000,
    totalInputBytes=0,
    totalShuffleRead=0,
    totalShuffleWrite= 0,
    maxMemory = 0,
    totalGCTimeSec * 1000,
    totalMemoryBytesSpilled = 0,
    executorLogs = Map.empty,
    peakJvmUsedMemory = Map("jvmUsedMemory" -> (jvmUsedMemoryMb << 20)),
    peakUnifiedMemory = Map.empty
  )

  /**
    * Create the Spark application data.
    *
    * @param stages list of stage data
    * @param executorSummaries list of executor summaries.
    * @param properties configuration properties for the Spark application.
    * @return Spark application data.
    */
  def createSparkApplicationData
  (stages: Seq[StageDataImpl],
   executorSummaries: Seq[ExecutorSummaryImpl],
   properties: Option[Map[String, String]]): SparkApplicationData = {
    val appId = "application_1"

    val logDerivedData = properties.map { props =>
      SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> props.toSeq))
      )}

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", Seq.empty),
      jobDatas = Seq.empty,
      stageDatas = stages,
      executorSummaries = executorSummaries,
      stagesWithFailedTasks = stages
    )
    SparkApplicationData(appId, restDerivedData, logDerivedData)
  }
}
