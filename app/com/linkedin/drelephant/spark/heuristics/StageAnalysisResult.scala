package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.Severity

/** Stage analysis result. */
private[heuristics] sealed trait StageAnalysisResult {

  /** @return the severity for the stage and heuristic evaluated */
  def severity: Severity

  /** @return the heuristics score for the stage and heuristic evaluated */
  def score: Int

  /** @return information, details and advice from the analysis. */
  def details: Seq[String]

}

/**
  * Stage analysis result for examining the stage for long tasks.
  *
  * @param severity long task severity.
  * @param score heuristics score for long tasks.
  * @param medianRunTime the median task run time in ms for the stage.
  * @param details information and recommendations from analysis for long tasks.
  */
private[heuristics] case class LongTaskResult(
    severity: Severity,
    score: Int,
    medianRunTime: Option[Double],
    details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for task skew.
  *
  * @param severity task skew severity.
  * @param rawSeverity severity based only on task skew, and not considering other thresholds
  *                    (task duration or ratio of task duration to stage suration).
  * @param score heuristics score for task skew.
  * @param medianRunTime median task run time in ms for the stage.
  * @param maxRunTime maximum task run time in ms for the stage.
  * @param stageDuration wall clock time for the stage in ms.
  * @param details information and recommendations from analysis for task skew.
  */
private[heuristics] case class TaskSkewResult(
    severity: Severity,
    rawSeverity: Severity,
    score: Int,
    medianRunTime: Option[Double],
    maxRunTime: Option[Double],
    stageDuration: Option[Long],
    details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for execution memory spill.
  *
  * @param severity execution memory spill severity.
  * @param rawSeverity severity based only on execution memory spill, and not considering other
  *                    thresholds (max amount of data processed for the stage).
  * @param score heuristics score for execution memory spill.
  * @param memoryBytesSpilled the total amount of execution memory bytes spilled for the stage.
  * @param maxTaskBytesSpilled the maximum number of bytes spilled by a task.
  * @param inputBytes the total amount of input bytes read for the stage.
  * @param details information and recommendations from analysis for execution memory spill.
  */
private[heuristics] case class ExecutionMemorySpillResult(
     severity: Severity,
     rawSeverity: Severity,
     score: Int,
     memoryBytesSpilled: Long,
     maxTaskBytesSpilled: Long,
     inputBytes: Long,
     details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for task failures.
  *
  * @param severity task failure severity.
  * @param oomSeverity severity for task failures due to OutOfMemory errors.
  * @param containerKilledSeverity severity for task failures due to container killed by YARN.
  * @param score heuristic score for task failures.
  * @param numTasks number for tasks for the stage.
  * @param numFailures number of task failures for the stage.
  * @param numOOM number of tasks which failed to to OutOfMemory errors.
  * @param numContainerKilled number of tasks which failed due to container killed by YARN.
  * @param details information and recommendations from analysis for task failures.
  */
private[heuristics] case class TaskFailureResult(
    severity: Severity,
    oomSeverity: Severity,
    containerKilledSeverity: Severity,
    score: Int,
    numTasks: Int,
    numFailures: Int,
    numOOM: Int,
    numContainerKilled: Int,
    details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for stage failure.
  *
  * @param severity stage failure severity.
  * @param score heuristics score for stage failure.
  * @param details information and recommendations from stage failure analysis.
  */
private[heuristics] case class StageFailureResult(
    severity: Severity,
    score: Int,
    details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for GC.
  * @param severity stage GC severity
  * @param score heuristics score for s.tage GC.
  * @param details information and recommendations from stage GC analysis.
  */
private[heuristics] case class StageGCResult(
    severity: Severity,
    score: Int,
    details: Seq[String]) extends StageAnalysisResult
