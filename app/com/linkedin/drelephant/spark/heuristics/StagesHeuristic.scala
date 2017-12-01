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

import java.util.NoSuchElementException
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorSummary, StageData, TaskData, TaskMetricsImpl}
import scala.collection.JavaConverters
import scala.concurrent.duration
import scala.concurrent.duration.Duration
import com.linkedin.drelephant.analysis.{Heuristic, HeuristicResult, HeuristicResultDetails, Severity, SeverityThresholds}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import scala.util.control.Exception


/**
  * A heuristic based on metrics for a Spark app's stages.
  *
  * This heuristic reports stage failures, high task failure rates for each stage, and long average executor runtimes for
  * each stage.
  */
class StagesHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
    extends Heuristic[SparkApplicationData] {
  import StagesHeuristic._
  import JavaConverters._

  val stageFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS)

  val taskFailureRateSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS)

  val stageRuntimeMillisSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .map(minutesSeverityThresholdsToMillisSeverityThresholds)
      .getOrElse(DEFAULT_STAGE_RUNTIME_MILLIS_SEVERITY_THRESHOLDS)

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatStagesWithHighTaskFailureRates(stagesWithHighTaskFailureRates: Seq[(StageData, Double)]): String =
      stagesWithHighTaskFailureRates
        .map { case (stageData, taskFailureRate) => formatStageWithHighTaskFailureRate(stageData, taskFailureRate) }
        .mkString("\n")

    def formatStageWithHighTaskFailureRate(stageData: StageData, taskFailureRate: Double): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (task failure rate: ${taskFailureRate}%1.3f)"

    def formatStagesWithLongAverageExecutorRuntimes(stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)]): String =
      stagesWithLongAverageExecutorRuntimes
        .map { case (stageData, runtime) => formatStageWithLongRuntime(stageData, runtime) }
        .mkString("\n")

    def formatStageWithLongRuntime(stageData: StageData, runtime: Long): String =
      f"stage ${stageData.stageId}, attempt ${stageData.attemptId} (runtime: ${Statistics.readableTimespan(runtime)})"

    var resultDetails = Seq(
      new HeuristicResultDetails("Spark completed stages count", evaluator.numCompletedStages.toString),
      new HeuristicResultDetails("Spark failed stages count", evaluator.numFailedStages.toString),
      new HeuristicResultDetails("Spark stage failure rate", f"${evaluator.stageFailureRate.getOrElse(0.0D)}%.3f"),
      new HeuristicResultDetails(
        "Spark stages with high task failure rates",
        formatStagesWithHighTaskFailureRates(evaluator.stagesWithHighTaskFailureRates)
      ),
      new HeuristicResultDetails(
        "Spark stages with long average executor runtimes",
        formatStagesWithLongAverageExecutorRuntimes(evaluator.stagesWithLongAverageExecutorRuntimes)
      ),
      new HeuristicResultDetails("JVM GC time to Executor Run time ratio", evaluator.ratio.toString),
      new HeuristicResultDetails("Jvm GC total time", evaluator.jvmTime.toString),
      new HeuristicResultDetails("Executor Run time", evaluator.executorRunTimeTotal.toString)
    )

    //adding recommendations to the result, severityTimeA corresponds to the ascending severity calculation
    if (evaluator.severityTimeA.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note above normal", "The ratio of JVM GC Time and executor Time is above normal, we recommend to increase the executor memory")
    }
    //severityTimeD corresponds to the descending severity calculation
    if (evaluator.severityTimeD.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Note below normal", "The ratio of JVM GC Time and executor Time is below normal, we recommend to decrease the executor memory")
    }

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

object StagesHeuristic {
  /** The default severity thresholds for the rate of an application's stages failing. */
  val DEFAULT_STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)

  /** The default severity thresholds for the rate of a stage's tasks failing. */
  val DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.1D, moderate = 0.3D, severe = 0.5D, critical = 0.5D, ascending = true)

  /** The default severity thresholds for a stage's runtime. */
  val DEFAULT_STAGE_RUNTIME_MILLIS_SEVERITY_THRESHOLDS = SeverityThresholds(
    low = Duration("15min").toMillis,
    moderate = Duration("30min").toMillis,
    severe = Duration("45min").toMillis,
    critical = Duration("60min").toMillis,
    ascending = true
  )

  /** The ascending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is above normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)

  /** The descending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is below normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_D_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.04D, severe = 0.03D, critical = 0.01D, ascending = false)

  val STAGE_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_failure_rate_severity_thresholds"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_task_failure_rate_severity_thresholds"
  val STAGE_RUNTIME_MINUTES_SEVERITY_THRESHOLDS_KEY = "stage_runtime_minutes_severity_thresholds"

  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"

  class Evaluator(stagesHeuristic: StagesHeuristic, data: SparkApplicationData) {
    lazy val stageDatas: Seq[StageData] = data.stageDatas

    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries

    lazy val numCompletedStages: Int = stageDatas.count {
      _.status == StageStatus.COMPLETE
    }

    lazy val numFailedStages: Int = stageDatas.count {
      _.status == StageStatus.FAILED
    }

    lazy val stageFailureRate: Option[Double] = {
      val numStages = numCompletedStages + numFailedStages
      if (numStages == 0) None else Some(numFailedStages.toDouble / numStages.toDouble)
    }

    lazy val stagesWithHighTaskFailureRates: Seq[(StageData, Double)] =
      stagesWithHighTaskFailureRateSeverities.map { case (stageData, taskFailureRate, _) => (stageData, taskFailureRate) }

    lazy val stagesWithLongAverageExecutorRuntimes: Seq[(StageData, Long)] =
      stagesAndAverageExecutorRuntimeSeverities
        .collect { case (stageData, runtime, severity) if severity.getValue > Severity.MODERATE.getValue => (stageData, runtime) }

    lazy val severity: Severity = Severity.max((stageFailureRateSeverity +: severityTimeA +: severityTimeD +: (taskFailureRateSeverities ++ runtimeSeverities)): _*)

    var (jvmTime, executorRunTimeTotal) = getTimeValues(stageDatas)

    var ratio: Double = {
      ratio = jvmTime.toDouble / executorRunTimeTotal.toDouble
      ratio
    }
    lazy val severityTimeA: Severity = DEFAULT_GC_SEVERITY_A_THRESHOLDS.severityOf(ratio)
    lazy val severityTimeD: Severity = DEFAULT_GC_SEVERITY_D_THRESHOLDS.severityOf(ratio)


    private lazy val stageFailureRateSeverityThresholds = stagesHeuristic.stageFailureRateSeverityThresholds

    private lazy val taskFailureRateSeverityThresholds = stagesHeuristic.taskFailureRateSeverityThresholds

    private lazy val stageRuntimeMillisSeverityThresholds = stagesHeuristic.stageRuntimeMillisSeverityThresholds

    private lazy val stageFailureRateSeverity: Severity =
      stageFailureRateSeverityThresholds.severityOf(stageFailureRate.getOrElse[Double](0.0D))

    private lazy val stagesWithHighTaskFailureRateSeverities: Seq[(StageData, Double, Severity)] =
      stagesAndTaskFailureRateSeverities.filter { case (_, _, severity) => severity.getValue > Severity.MODERATE.getValue }

    private lazy val stagesAndTaskFailureRateSeverities: Seq[(StageData, Double, Severity)] = for {
      stageData <- stageDatas
      (taskFailureRate, severity) = taskFailureRateAndSeverityOf(stageData)
    } yield (stageData, taskFailureRate, severity)

    private lazy val taskFailureRateSeverities: Seq[Severity] =
      stagesAndTaskFailureRateSeverities.map { case (_, _, severity) => severity }

    private lazy val stagesAndAverageExecutorRuntimeSeverities: Seq[(StageData, Long, Severity)] = for {
      stageData <- stageDatas
      (runtime, severity) = averageExecutorRuntimeAndSeverityOf(stageData)
    } yield (stageData, runtime, severity)

    private lazy val runtimeSeverities: Seq[Severity] = stagesAndAverageExecutorRuntimeSeverities.map { case (_, _, severity) => severity }

    private lazy val executorInstances: Int =
      appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt).getOrElse(executorSummaries.size)


    private def taskFailureRateAndSeverityOf(stageData: StageData): (Double, Severity) = {
      val taskFailureRate = taskFailureRateOf(stageData).getOrElse(0.0D)
      (taskFailureRate, taskFailureRateSeverityThresholds.severityOf(taskFailureRate))
    }

    private def taskFailureRateOf(stageData: StageData): Option[Double] = {
      // Currently, the calculation doesn't include skipped or active tasks.
      val numCompleteTasks = stageData.numCompleteTasks
      val numFailedTasks = stageData.numFailedTasks
      val numTasks = numCompleteTasks + numFailedTasks
      if (numTasks == 0) None else Some(numFailedTasks.toDouble / numTasks.toDouble)
    }

    private def averageExecutorRuntimeAndSeverityOf(stageData: StageData): (Long, Severity) = {
      val averageExecutorRuntime = stageData.executorRunTime / executorInstances
      (averageExecutorRuntime, stageRuntimeMillisSeverityThresholds.severityOf(averageExecutorRuntime))
    }

    /**
      * returns the total JVM GC Time and total executor Run Time across all stages
      * @param stageDatas
      * @return
      */
    private def getTimeValues(stageDatas: Seq[StageData]): (Long, Long) = {
      var jvmGcTimeTotal: Long = 0
      var executorRunTimeTotal: Long = 0
      var taskMetricsDummy: TaskMetricsImpl = new Some(new TaskMetricsImpl(
        executorDeserializeTime = 0,
        executorRunTime = 0,
        resultSize = 0,
        jvmGcTime = 0,
        resultSerializationTime = 0,
        memoryBytesSpilled = 0,
        diskBytesSpilled = 0,
        inputMetrics = None,
        outputMetrics = None,
        shuffleReadMetrics = None,
        shuffleWriteMetrics = None)).get
      //ignoring the exception as there are cases when there is no task data, in such cases, 0 is taken as the default value
      Exception.ignoring(classOf[NoSuchElementException]) {
        stageDatas.foreach((stageData: StageData) => {
          executorRunTimeTotal += stageData.executorRunTime
          stageData.tasks.get.values.foreach((taskData: TaskData) => {
            jvmGcTimeTotal += taskData.taskMetrics.getOrElse(taskMetricsDummy).jvmGcTime
          })
        })
      }
      (jvmGcTimeTotal, executorRunTimeTotal)
    }
  }

  def minutesSeverityThresholdsToMillisSeverityThresholds(minutesSeverityThresholds: SeverityThresholds)
  : SeverityThresholds = SeverityThresholds(
    Duration(minutesSeverityThresholds.low.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.moderate.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.severe.longValue, duration.MINUTES).toMillis,
    Duration(minutesSeverityThresholds.critical.longValue, duration.MINUTES).toMillis,
    minutesSeverityThresholds.ascending
  )
}

