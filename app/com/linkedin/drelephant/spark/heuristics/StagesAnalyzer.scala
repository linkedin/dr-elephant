package com.linkedin.drelephant.spark.heuristics

import scala.collection.mutable.ArrayBuffer
import com.linkedin.drelephant.analysis.{Severity, SeverityThresholds}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{StageData, StageStatus, TaskDataImpl}
import com.linkedin.drelephant.util.{MemoryFormatUtils, Utils}

/**
  * Analysis results for a stage.
  *
  * @param stageId the stage ID.
  * @param executionMemorySpillResult stage analysis result for examining the stage for execution
  *                                   memory spill.
  * @param longTaskResult stage analysis result for examining the stage for long tasks.
  * @param taskSkewResult stage analysis result for examining the stage for task skew.
  * @param taskFailureResult stage analysis result for examining the stage for task failures.
  * @param stageFailureResult stage analysis result for examining the stage for stage failure.
  * @param stageGCResult stage analysis result for examining the stage for GC.
  */
private[heuristics] case class StageAnalysis(
    stageId: Int,
    executionMemorySpillResult: ExecutionMemorySpillResult,
    longTaskResult: LongTaskResult,
    taskSkewResult: TaskSkewResult,
    taskFailureResult: TaskFailureResult,
    stageFailureResult: StageFailureResult,
    stageGCResult: StageGCResult)

/**
  * Analyzes the stage level metrics for the given application.
  *
  * @param heuristicConfigurationData heuristic configuration data
  * @param data Spark application data
  */
private[heuristics] class StagesAnalyzer(
    private val heuristicConfigurationData: HeuristicConfigurationData,
    private val data: SparkApplicationData) {

  import ConfigurationUtils._

  // serverity thresholds for execution memory spill
  private val executionMemorySpillThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap
      .get(SPARK_STAGE_EXECUTION_MEMORY_SPILL_THRESHOLD_KEY), ascending = true)
      .getOrElse(DEFAULT_EXECUTION_MEMORY_SPILL_THRESHOLDS)

  // severity thresholds for task skew
  private val taskSkewThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap
      .get(SPARK_STAGE_TASK_SKEW_THRESHOLD_KEY), ascending = true)
      .getOrElse(DEFAULT_TASK_SKEW_THRESHOLDS)

  // severity thresholds for task duration (long running tasks)
  private val taskDurationThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap
      .get(SPARK_STAGE_TASK_DURATION_THRESHOLD_KEY), ascending = true)
      .getOrElse(DEFAULT_TASK_DURATION_THRESHOLDS)

  // severity thresholds for task failures
  private val taskFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap
      .get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS)

  // execution memory spill: threshold for processed data, above which some spill is expected
  private val maxDataProcessedThreshold = MemoryFormatUtils.stringToBytes(
    heuristicConfigurationData.getParamMap
    .getOrDefault(MAX_DATA_PROCESSED_THRESHOLD_KEY, DEFAULT_MAX_DATA_PROCESSED_THRESHOLD))

  // threshold for ratio of max task duration to stage duration, for flagging task skew
  private val longTaskToStageDurationRatio = heuristicConfigurationData.getParamMap
    .getOrDefault(LONG_TASK_TO_STAGE_DURATION_RATIO_KEY, DEFAULT_LONG_TASK_TO_STAGE_DURATION_RATIO).toDouble

  // min threshold for median task duration, for flagging task skew
  private val taskDurationMinThreshold = heuristicConfigurationData.getParamMap
    .getOrDefault(TASK_SKEW_TASK_DURATION_MIN_THRESHOLD_KEY, DEFAULT_TASK_SKEW_TASK_DURATION_MIN_THRESHOLD).toLong

  // the maximum number of recommended partitions
  private val maxRecommendedPartitions = heuristicConfigurationData.getParamMap
    .getOrDefault(MAX_RECOMMENDED_PARTITIONS_KEY, DEFAULT_MAX_RECOMMENDED_PARTITIONS).toInt

  /**
    * Get the analysis for each stage of the application.
    *
    * @param curNumPartitions the configured number of partitions for the application
    *                         (value of spark.sql.shuffle.partitions).
    * @return list of analysis results of stages.
    */
  def getStageAnalysis(curNumPartitions: Int): Seq[StageAnalysis] = {
    val failedTasksStageMap = data.stagesWithFailedTasks.flatMap { stageData =>
      stageData.tasks.map(tasks => (stageData.stageId, tasks.values))
    }.toMap

    data.stageDatas.map { stageData =>
      val medianTime = stageData.taskSummary.collect {
        case distribution => distribution.executorRunTime(DISTRIBUTION_MEDIAN_IDX)
      }
      val maxTime = stageData.taskSummary.collect {
        case distribution => distribution.executorRunTime(DISTRIBUTION_MAX_IDX)
      }
      val stageDuration = (stageData.submissionTime, stageData.completionTime) match {
        case (Some(submissionTime), Some(completionTime)) =>
          Some(completionTime.getTime() - submissionTime.getTime())
        case _ => None
      }
      val stageId = stageData.stageId

      val executionMemorySpillResult = checkForExecutionMemorySpill(stageId, stageData)
      val longTaskResult = checkForLongTasks(stageId, stageData, medianTime, curNumPartitions)
      val taskSkewResult = checkForTaskSkew(stageId, stageData, medianTime, maxTime, stageDuration,
          executionMemorySpillResult.severity)
      val stageFailureResult = checkForStageFailure(stageId, stageData)
      val taskFailureResult = checkForTaskFailure(stageId, stageData, failedTasksStageMap)
      val gcResult = checkForGC(stageId, stageData)

      new StageAnalysis(stageData.stageId, executionMemorySpillResult, longTaskResult,
        taskSkewResult, taskFailureResult, stageFailureResult, gcResult)
    }
  }

  /**
    * Check stage for execution memory spill.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @return results of execution memory spill analysis for the stage.
    */
  private def checkForExecutionMemorySpill(
      stageId: Int,
      stageData: StageData): ExecutionMemorySpillResult = {
    val maxData = Seq(stageData.inputBytes, stageData.shuffleReadBytes,
      stageData.shuffleWriteBytes, stageData.outputBytes).max
    val rawSpillSeverity = executionMemorySpillThresholds.severityOf(
      stageData.memoryBytesSpilled / maxData.toDouble)
    val details = new ArrayBuffer[String]
    val executionSpillSeverity = if (maxData < maxDataProcessedThreshold) {
      rawSpillSeverity
    } else {
      // don't flag execution memory spill if there is a lot of data being processed,
      // since some spill may be unavoidable in this case.
      if (hasSignificantSeverity(rawSpillSeverity)) {
        details += s"Stage $stageId is processing a lot of data; examine the application to see " +
          s"if this can be reduced."
      }
      Severity.NONE
    }
    if (hasSignificantSeverity(rawSpillSeverity)) {
      val memoryBytesSpilled = MemoryFormatUtils.bytesToString(stageData.memoryBytesSpilled)
      details += s"Stage $stageId has $memoryBytesSpilled execution memory spill."
      if (maxData > maxDataProcessedThreshold) {
        // if a lot of data is being processed, the severity is supressed, but give information
        // about the spill to the user, so that they know that spill is happening, and can check
        // if the application can be modified to process less data.
        details += s"Stage $stageId has ${stageData.numTasks} tasks, " +
          s"${MemoryFormatUtils.bytesToString(stageData.inputBytes)} input read, " +
          s"${MemoryFormatUtils.bytesToString(stageData.shuffleReadBytes)} shuffle read, " +
          s"${MemoryFormatUtils.bytesToString(stageData.shuffleWriteBytes)} shuffle write, " +
          s"${MemoryFormatUtils.bytesToString(stageData.outputBytes)} output."
        stageData.taskSummary.foreach { summary =>
          val memorySpill = summary.memoryBytesSpilled(DISTRIBUTION_MEDIAN_IDX).toLong
          val inputBytes = summary.inputMetrics.map(_.bytesRead(DISTRIBUTION_MEDIAN_IDX))
              .getOrElse(0.0).toLong
          val outputBytes = summary.outputMetrics.map(_.bytesWritten(DISTRIBUTION_MEDIAN_IDX))
            .getOrElse(0.0).toLong
          val shuffleReadBytes = summary.shuffleReadMetrics.map(_.readBytes(DISTRIBUTION_MEDIAN_IDX))
            .getOrElse(0.0).toLong
          val shuffleWriteBytes = summary.shuffleWriteMetrics.map(_.writeBytes(DISTRIBUTION_MEDIAN_IDX))
            .getOrElse(0.0).toLong
          details += s"Stage $stageId has median task values: " +
          s"${MemoryFormatUtils.bytesToString(memorySpill)} memory spill, " +
            s"${MemoryFormatUtils.bytesToString(inputBytes)} input, " +
            s"${MemoryFormatUtils.bytesToString(shuffleReadBytes)} shuffle read, " +
            s"${MemoryFormatUtils.bytesToString(shuffleWriteBytes)} shuffle write, " +
            s"${MemoryFormatUtils.bytesToString(outputBytes)} output."
        }
      }
    }

    val maxTaskSpill = stageData.taskSummary.collect {
      case distribution => distribution.memoryBytesSpilled(DISTRIBUTION_MAX_IDX)
    }.map(_.toLong).getOrElse(0L)
    val score = Utils.getHeuristicScore(executionSpillSeverity, stageData.numTasks)

    ExecutionMemorySpillResult(executionSpillSeverity, rawSpillSeverity, score,
      stageData.memoryBytesSpilled, maxTaskSpill, stageData.inputBytes, details)
  }

  /**
    * Check stage for task skew.
    *
    * @param stageId stage ID.
    * @param stageData stage data
    * @param medianTime median task run time (ms).
    * @param maxTime maximum task run time (ms).
    * @param stageDuration stage duration (ms).
    * @param executionSpillSeverity execution spill severity
    * @return results of task skew analysis for the stage.
    */
  private def checkForTaskSkew(
      stageId: Int,
      stageData: StageData,
      medianTime: Option[Double],
      maxTime: Option[Double],
      stageDuration: Option[Long],
      executionSpillSeverity: Severity): TaskSkewResult = {
    val rawSkewSeverity = (medianTime, maxTime) match {
      case (Some(median), Some(max)) =>
        taskSkewThresholds.severityOf(max / median)
      case _ => Severity.NONE
    }
    val median = medianTime.getOrElse(0.0D)
    val maximum = maxTime.getOrElse(0.0D)
    val taskSkewSeverity =
      if (maximum > taskDurationMinThreshold &&
        maximum > longTaskToStageDurationRatio * stageDuration.getOrElse(Long.MaxValue)) {
      rawSkewSeverity
    } else {
      Severity.NONE
    }
    val details = new ArrayBuffer[String]

    if (hasSignificantSeverity(taskSkewSeverity) || hasSignificantSeverity(executionSpillSeverity)) {
      // add more information about what might be causing skew if skew is being flagged
      // (reported severity is significant), or there is execution memory spill, since skew
      // can also cause execution memory spill.
      val median = Utils.getDuration(medianTime.map(_.toLong).getOrElse(0L))
      val maximum = Utils.getDuration(maxTime.map(_.toLong).getOrElse(0L))
      var inputSkewSeverity = Severity.NONE
      if (hasSignificantSeverity(taskSkewSeverity)) {
        details +=
          s"Stage $stageId has skew in task run time (median is $median, max is $maximum)"
      }
      stageData.taskSummary.foreach { summary =>
        checkSkewedData(stageId, summary.memoryBytesSpilled(DISTRIBUTION_MEDIAN_IDX),
          summary.memoryBytesSpilled(DISTRIBUTION_MAX_IDX), "memory bytes spilled", details)
        summary.inputMetrics.foreach { input =>
          inputSkewSeverity = checkSkewedData(stageId, input.bytesRead(DISTRIBUTION_MEDIAN_IDX),
            input.bytesRead(DISTRIBUTION_MAX_IDX), "task input bytes", details)
          if (hasSignificantSeverity(inputSkewSeverity)) {
            // The stage is reading input data, try to adjust the amount of data to even the partitions
            details += s"Stage $stageId: please set DaliSpark.SPLIT_SIZE to make " +
              "partitions more even."
          }
        }
        summary.outputMetrics.foreach { output =>
          checkSkewedData(stageId, output.bytesWritten(DISTRIBUTION_MEDIAN_IDX),
            output.bytesWritten(DISTRIBUTION_MAX_IDX), "task output bytes", details)
        }
        summary.shuffleReadMetrics.foreach { shuffle =>
          checkSkewedData(stageId, shuffle.readBytes(DISTRIBUTION_MEDIAN_IDX),
            shuffle.readBytes(DISTRIBUTION_MAX_IDX), "task shuffle read bytes", details)
        }
        summary.shuffleWriteMetrics.foreach { shuffle =>
             checkSkewedData(stageId, shuffle.writeBytes(DISTRIBUTION_MEDIAN_IDX),
              shuffle.writeBytes(DISTRIBUTION_MAX_IDX), "task shuffle write bytes", details)
        }
      }
      if (hasSignificantSeverity(rawSkewSeverity) && !hasSignificantSeverity(inputSkewSeverity)) {
        details += s"Stage $stageId: please try to modify the application to make " +
          "the partitions more even."
      }
    }
    val score = Utils.getHeuristicScore(taskSkewSeverity, stageData.numTasks)

    TaskSkewResult(taskSkewSeverity, rawSkewSeverity, score, medianTime, maxTime, stageDuration, details)
  }

  /**
    * Check for skewed data.
    *
    * @param stageId stage ID
    * @param median median data size for tasks.
    * @param maximum maximum data size for tasks.
    * @param description type of data.
    * @param details information and recommendations -- any new recommendations
    *                from analyzing the stage for data skew will be appended.
    */
  private def checkSkewedData(
      stageId: Int,
      median: Double,
      maximum: Double,
      description: String,
      details: ArrayBuffer[String]): Severity = {
    val severity = taskSkewThresholds.severityOf(maximum / median)
    if (hasSignificantSeverity(severity)) {
      details += s"Stage $stageId has skew in $description (median is " +
        s"${MemoryFormatUtils.bytesToString(median.toLong)}, " +
        s"max is ${MemoryFormatUtils.bytesToString(maximum.toLong)})."
    }
    severity
  }

  /**
    * Check the stage for long running tasks.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @param medianTime median task run time.
    * @param curNumPartitions number of partitions for the Spark application
    *                         (spark.sql.shuffle.partitions).
    * @return results of long running task analysis for the stage
    */
  private def checkForLongTasks(
      stageId: Int,
      stageData: StageData,
      medianTime: Option[Double],
      curNumPartitions: Int): LongTaskResult = {
    val longTaskSeverity = stageData.taskSummary.map { distributions =>
      taskDurationThresholds.severityOf(distributions.executorRunTime(DISTRIBUTION_MEDIAN_IDX))
    }.getOrElse(Severity.NONE)
    val details = new ArrayBuffer[String]
    if (hasSignificantSeverity(longTaskSeverity)) {
      val runTime = Utils.getDuration(medianTime.map(_.toLong).getOrElse(0L))
      val maxData = Seq(stageData.inputBytes, stageData.shuffleReadBytes, stageData.shuffleWriteBytes,
        stageData.outputBytes).max
      details += s"Stage $stageId median task run time is $runTime."
      if (stageData.numTasks >= maxRecommendedPartitions) {
        if (maxData >= maxDataProcessedThreshold) {
          val inputBytes = MemoryFormatUtils.bytesToString(stageData.inputBytes)
          val shuffleReadBytes = MemoryFormatUtils.bytesToString(stageData.shuffleReadBytes)
          val shuffleWriteBytes = MemoryFormatUtils.bytesToString(stageData.shuffleWriteBytes)
          details += s"Stage $stageId: has $inputBytes input, $shuffleReadBytes shuffle read, " +
            "$shuffleWriteBytes shuffle write. Please try to reduce the amount of data being processed."
        } else {
          details += s"Stage $stageId: please optimize the code to improve performance."
        }
      }
      else {
        if (stageData.inputBytes > 0) {
          // The stage is reading input data, try to increase the number of readers
          details += s"Stage $stageId: please set DaliSpark.SPLIT_SIZE to a smaller " +
            "value to increase the number of tasks reading input data for this stage."
        } else if (stageData.numTasks != curNumPartitions) {
          details += s"Stage $stageId: please increase the number of partitions, which " +
            s"is currently set to ${stageData.numTasks}."
        }
      }
    }
    val score = Utils.getHeuristicScore(longTaskSeverity, stageData.numTasks)

    LongTaskResult(longTaskSeverity, score, medianTime, details)
  }

  /**
    * Check for stage failure.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @return results of stage failure analysis for the stage.
    */
  private def checkForStageFailure(stageId: Int, stageData: StageData): StageFailureResult = {
    val severity = if (stageData.status == StageStatus.FAILED) {
      Severity.CRITICAL
    } else {
      Severity.NONE
    }
    val score = Utils.getHeuristicScore(severity, stageData.numTasks)
    val details = stageData.failureReason.map(reason => s"Stage $stageId failed: $reason")
    StageFailureResult(severity, score, details.toSeq)
  }

  /**
    * Check for failed tasks, including failures caused by OutOfMemory errors, and containers
    * killed by YARN for exceeding memory limits.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @param failedTasksStageMap map of stage ID to list of failed tasks for the stage.
    * @return result of failed tasks analysis for the stage.
    */
  private def checkForTaskFailure(
      stageId: Int,
      stageData: StageData,
      failedTasksStageMap: Map[Int, Iterable[TaskDataImpl]]): TaskFailureResult = {
    val failedTasks = failedTasksStageMap.get(stageId)

    val details = new ArrayBuffer[String]()

    val taskFailureSeverity = taskFailureRateSeverityThresholds.severityOf(
      stageData.numFailedTasks.toDouble / stageData.numTasks)
    if (hasSignificantSeverity(taskFailureSeverity)) {
      details += s"Stage $stageId has ${stageData.numFailedTasks} failed tasks."
    }

    val score = Utils.getHeuristicScore(taskFailureSeverity, stageData.numFailedTasks)

    val (numTasksWithOOM, oomSeverity) =
      checkForTaskError(stageId, stageData, failedTasks,
        StagesWithFailedTasksHeuristic.OOM_ERROR, "of OutOfMemory exception.",
        details)

    val (numTasksWithContainerKilled, containerKilledSeverity) =
      checkForTaskError(stageId, stageData, failedTasks,
        StagesWithFailedTasksHeuristic.OVERHEAD_MEMORY_ERROR,
        "the container was killed by YARN for exeeding memory limits.", details)

    TaskFailureResult(taskFailureSeverity, oomSeverity, containerKilledSeverity,
      score, stageData.numTasks, stageData.numFailedTasks, numTasksWithOOM,
      numTasksWithContainerKilled, details)
  }

  /**
    * Check the stage for a high ratio of time spent in GC compared to task run time.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @return result of GC analysis for the stage.
    */
  private def checkForGC(stageId: Int, stageData: StageData): StageGCResult = {
    var gcTime = 0.0D
    var taskTime = 0.0D
    val severity = stageData.taskSummary.map { task =>
      gcTime = task.jvmGcTime(DISTRIBUTION_MEDIAN_IDX)
      taskTime = task.executorRunTime(DISTRIBUTION_MEDIAN_IDX)
      DEFAULT_GC_SEVERITY_A_THRESHOLDS.severityOf(gcTime / taskTime)
    }.getOrElse(Severity.NONE)

    val score = Utils.getHeuristicScore(severity, stageData.numTasks)

    val details = if (hasSignificantSeverity(severity)) {
      Seq(s"Stage ${stageId}: tasks are spending signficant time in GC (median task GC time is " +
        s"${Utils.getDuration(gcTime.toLong)}, median task runtime is " +
        s"${Utils.getDuration(gcTime.toLong)}")
    } else {
      Seq.empty
    }

    new StageGCResult(severity, score, details)
  }

  /**
    * Check the stage for tasks that failed for a specified error.
    *
    * @param stageId stage ID.
    * @param stageData stage data.
    * @param failedTasks list of failed tasks.
    * @param taskError the error to check for.
    * @param errorMessage the message/explanation to print if the the specified error is found.
    * @param details information and recommendations -- any new recommendations
    *                from analyzing the stage for errors causing tasks to fail will be appended.
    * @return
    */
  private def checkForTaskError(
      stageId: Int,
      stageData: StageData,
      failedTasks: Option[Iterable[TaskDataImpl]],
      taskError: String,
      errorMessage: String,
      details: ArrayBuffer[String]): (Int, Severity) = {
    val numTasksWithError = getNumTasksWithError(failedTasks, taskError)
    if (numTasksWithError > 0) {
      details += s"Stage $stageId: has $numTasksWithError tasks that failed because " +
        errorMessage
    }
    val severity = taskFailureRateSeverityThresholds.severityOf(numTasksWithError.toDouble / stageData.numTasks)
    (numTasksWithError, severity)
  }

  /**
    * Get the number of tasks that failed with the specified error, using a simple string search.
    *
    * @param tasks list of failed tasks.
    * @param error error to look for.
    * @return number of failed tasks wit the specified error.
    */
  private def getNumTasksWithError(tasks: Option[Iterable[TaskDataImpl]], error: String): Int = {
    tasks.map { failedTasks =>
      failedTasks.filter { task =>
        val hasError = task.errorMessage.map(_.contains(error)).getOrElse(false)
        hasError
      }.size
    }.getOrElse(0)
  }

  /** Given the severity, return true if the severity is not NONE or LOW. */
  private def hasSignificantSeverity(severity: Severity): Boolean = {
    severity != Severity.NONE && severity != Severity.LOW
  }
}
