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

import com.linkedin.drelephant.analysis.{Heuristic, HeuristicResult, HeuristicResultDetails, Severity, SeverityThresholds}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.JobData
import org.apache.spark.JobExecutionStatus


/**
  * A heuristic based on metrics for a Spark app's jobs.
  *
  * This heuristic reports job failures and high task failure rates for each job.
  */
class JobsHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
    extends Heuristic[SparkApplicationData] {
  import JobsHeuristic._
  import JavaConverters._

  val jobFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_JOB_FAILURE_RATE_SEVERITY_THRESHOLDS)

  val taskFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS)

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatFailedJobs(failedJobs: Seq[JobData]): String = failedJobs.map(formatFailedJob).mkString("\n")

    def formatFailedJob(jobData: JobData): String = f"job ${jobData.jobId}, ${jobData.name}"

    def formatJobsWithHighTaskFailureRates(jobsWithHighTaskFailureRates: Seq[(JobData, Double)]): String =
      jobsWithHighTaskFailureRates
        .map { case (jobData, taskFailureRate) => formatJobWithHighTaskFailureRate(jobData, taskFailureRate) }
        .mkString("\n")

    def formatJobWithHighTaskFailureRate(jobData: JobData, taskFailureRate: Double): String =
      f"job ${jobData.jobId}, ${jobData.name} (task failure rate: ${taskFailureRate}%1.3f)"

    val resultDetails = Seq(
      new HeuristicResultDetails("Spark completed jobs count", evaluator.numCompletedJobs.toString),
      new HeuristicResultDetails("Spark failed jobs count", evaluator.numFailedJobs.toString),
      new HeuristicResultDetails("Spark failed jobs list", formatFailedJobs(evaluator.failedJobs)),
      new HeuristicResultDetails("Spark job failure rate", f"${evaluator.jobFailureRate.getOrElse(0.0D)}%.3f"),
      new HeuristicResultDetails(
        "Spark jobs with high task failure rates",
        formatJobsWithHighTaskFailureRates(evaluator.jobsWithHighTaskFailureRates)
      )
    )
    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0,
      resultDetails.asJava
    )
    result
  }
}

object JobsHeuristic {
  /** The default severity thresholds for the rate of an application's jobs failing. */
  val DEFAULT_JOB_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)

  /** The default severity thresholds for the rate of a job's tasks failing. */
  val DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)

  val JOB_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_failure_rate_severity_thresholds"

  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "job_task_failure_rate_severity_thresholds"

  class Evaluator(jobsHeuristic: JobsHeuristic, data: SparkApplicationData) {
    lazy val jobDatas: Seq[JobData] = data.jobDatas

    lazy val numCompletedJobs: Int = jobDatas.count { _.status == JobExecutionStatus.SUCCEEDED }

    lazy val numFailedJobs: Int = jobDatas.count { _.status == JobExecutionStatus.FAILED }

    lazy val failedJobs: Seq[JobData] = jobDatas.filter { _.status == JobExecutionStatus.FAILED }

    lazy val jobFailureRate: Option[Double] = {
      // Currently, the calculation assumes there are no jobs with UNKNOWN or RUNNING state.
      val numJobs = numCompletedJobs + numFailedJobs
      if (numJobs == 0) None else Some(numFailedJobs.toDouble / numJobs.toDouble)
    }

    lazy val jobsWithHighTaskFailureRates: Seq[(JobData, Double)] =
      jobsWithHighTaskFailureRateSeverities.map { case (jobData, taskFailureRate, _) => (jobData, taskFailureRate) }

    lazy val severity: Severity = Severity.max((jobFailureRateSeverity +: taskFailureRateSeverities): _*)

    private lazy val jobFailureRateSeverityThresholds = jobsHeuristic.jobFailureRateSeverityThresholds

    private lazy val taskFailureRateSeverityThresholds = jobsHeuristic.taskFailureRateSeverityThresholds

    private lazy val jobFailureRateSeverity: Severity =
      jobFailureRateSeverityThresholds.severityOf(jobFailureRate.getOrElse[Double](0.0D))

    private lazy val jobsWithHighTaskFailureRateSeverities: Seq[(JobData, Double, Severity)] =
      jobsAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.getValue > Severity.MODERATE.getValue }

    private lazy val jobsAndTaskFailureRateSeverities: Seq[(JobData, Double, Severity)] = for {
      jobData <- jobDatas
      (taskFailureRate, severity) = taskFailureRateAndSeverityOf(jobData)
    } yield (jobData, taskFailureRate, severity)

    private lazy val taskFailureRateSeverities: Seq[Severity] =
      jobsAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }

    private def taskFailureRateAndSeverityOf(jobData: JobData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(jobData).getOrElse(0.0D)
      (taskFailureRate, taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
    }

    private def taskFailureRateOf(jobData: JobData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompletedTasks = jobData.numCompletedTasks
      val numFailedTasks = jobData.numFailedTasks
      val numTasks = numCompletedTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }
  }
}
