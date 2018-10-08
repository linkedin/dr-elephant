package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.Severity

/** Stage analysis result. */
private[heuristics] sealed trait StageAnalysisResult {

  // the severity for the stage and heuristic evaluated
  val severity: Severity

  // the heuristics score for the stage and heuristic evaluated
  val score: Int

  // information, details and advice from the analysis
  val details: Seq[String]
}

/** Simple stage analysis result, with the severity, score, and details. */
private[heuristics] case class SimpleStageAnalysisResult(
    severity: Severity,
    score: Int,
    details: Seq[String]) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for task skew.
  *
  * @param severity task skew severity.
  * @param score heuristics score for task skew.
  * @param details information and recommendations from analysis for task skew.
  * @param rawSeverity severity based only on task skew, and not considering other thresholds
  *                    (task duration or ratio of task duration to stage suration).
  */
private[heuristics] case class TaskSkewResult(
    severity: Severity,
    score: Int,
    details: Seq[String],
    rawSeverity: Severity) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for execution memory spill.
  *
  * @param severity execution memory spill severity.
  * @param score heuristics score for execution memory spill.
  * @param details information and recommendations from analysis for execution memory spill.
  * @param rawSeverity severity based only on execution memory spill, and not considering other
  *                    thresholds (max amount of data processed for the stage).
  * @param memoryBytesSpilled the total amount of execution memory bytes spilled for the stage.
  * @param maxTaskBytesSpilled the maximum number of bytes spilled by a task.
  */
private[heuristics] case class ExecutionMemorySpillResult(
    severity: Severity,
    score: Int,
    details: Seq[String],
    rawSeverity: Severity,
    memoryBytesSpilled: Long,
    maxTaskBytesSpilled: Long) extends StageAnalysisResult

/**
  * Stage analysis result for examining the stage for task failures.
  *
  * @param severity task failure severity.
  * @param score heuristic score for task failures.
  * @param details information and recommendations from analysis for task failures.
  * @param oomSeverity severity for task failures due to OutOfMemory errors.
  * @param containerKilledSeverity severity for task failures due to container killed by YARN.
  * @param numFailures number of task failures for the stage.
  * @param numOOM number of tasks which failed to to OutOfMemory errors.
  * @param numContainerKilled number of tasks which failed due to container killed by YARN.
  */
private[heuristics] case class TaskFailureResult(
    severity: Severity,
    score: Int,
    details: Seq[String],
    oomSeverity: Severity,
    containerKilledSeverity: Severity,
    numFailures: Int,
    numOOM: Int,
    numContainerKilled: Int) extends StageAnalysisResult
