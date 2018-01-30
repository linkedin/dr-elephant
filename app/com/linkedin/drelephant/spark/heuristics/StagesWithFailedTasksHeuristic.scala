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

import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{StageData, TaskData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus

import scala.collection.JavaConverters


/**
  * A heuristic based on errors encountered by failed tasks. Tasks may fail due to Overhead memory issues or OOM errors. These errors are checked and warning is given accordingly.
  */
class StagesWithFailedTasksHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import StagesWithFailedTasksHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Stages with OOM errors", evaluator.stagesWithOOMError.toString),
      new HeuristicResultDetails("Stages with Overhead memory errors", evaluator.stagesWithOverheadError.toString)
    )
    if (evaluator.severityOverheadStages.getValue >= Severity.MODERATE.getValue)
      resultDetails = resultDetails :+ new HeuristicResultDetails("Overhead memory errors", "Some tasks have failed due to overhead memory error. Please try increasing spark.yarn.executor.memoryOverhead by " + increaseMemoryBy +" in spark.yarn.executor.memoryOverhead")
    //TODO: refine recommendations
    if (evaluator.severityOOMStages.getValue >= Severity.MODERATE.getValue)
      resultDetails = resultDetails :+ new HeuristicResultDetails("OOM errors", "Some tasks have failed due to OOM error. Try increasing spark.executor.memory or decreasing spark.memory.fraction (take a look at unified memory heuristic) or decreasing number of cores.")
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

object StagesWithFailedTasksHeuristic {

  val OOM_ERROR = "java.lang.OutOfMemoryError"
  val OVERHEAD_MEMORY_ERROR = "killed by YARN for exceeding memory limits"
  val ratioThreshold: Double = 2
  val increaseMemoryBy: String = "1G"

  class Evaluator(memoryFractionHeuristic: StagesWithFailedTasksHeuristic, data: SparkApplicationData) {
    lazy val stagesWithFailedTasks: Seq[StageData] = data.stagesWithFailedTasks

    /**
      * @return : returns the OOM and Overhead memory errors severity
      */
    private def getErrorsSeverity: (Severity, Severity, Int, Int) = {
      var severityOOM: Severity = Severity.NONE
      var severityOverhead: Severity = Severity.NONE
      var stagesWithOOMError: Int = 0
      var stagesWithOverheadError: Int = 0
      stagesWithFailedTasks.foreach(stageData => {
        val numCompleteTasks: Int = stageData.numCompleteTasks
        var failedOOMTasks = 0
        var failedOverheadMemoryTasks = 0
        stageData.tasks.get.values.foreach((taskData: TaskData) => {
          var errorMessage: String = taskData.errorMessage.getOrElse("")
          failedOOMTasks = hasError(errorMessage, OOM_ERROR, failedOOMTasks)
          failedOverheadMemoryTasks = hasError(errorMessage, OVERHEAD_MEMORY_ERROR, failedOverheadMemoryTasks)
        })
        if (failedOOMTasks > 0) {
          stagesWithOOMError = stagesWithOOMError + 1
        }
        if (failedOverheadMemoryTasks > 0) {
          stagesWithOverheadError = stagesWithOverheadError + 1
        }
        severityOOM = getStageSeverity(failedOOMTasks, stageData.status, severityOOM, numCompleteTasks)
        severityOverhead = getStageSeverity(failedOverheadMemoryTasks, stageData.status, severityOverhead, numCompleteTasks)
      })
      (severityOOM, severityOverhead, stagesWithOOMError, stagesWithOverheadError)
    }

    /**
      * returns the max (severity of this stage, present severity)
      *
      * note : this method is called for all the stages, in turn updating the value of max stage severity if required.
      *
      * @param numFailedTasks
      * @param stageStatus
      * @param severityStage : max severity of all the stages we have encountered till now.
      * @param numCompleteTasks
      * @return
      */
    private def getStageSeverity(numFailedTasks: Int, stageStatus: StageStatus, severityStage: Severity, numCompleteTasks: Int): Severity = {
      var severityTemp: Severity = Severity.NONE
      if(numCompleteTasks == 0) {
        return severityStage
      }

      if (numFailedTasks != 0 && stageStatus != StageStatus.FAILED) {
        if (numFailedTasks.toDouble / numCompleteTasks.toDouble < ratioThreshold / 100.toDouble) {
          severityTemp = Severity.MODERATE
        } else {
          severityTemp = Severity.SEVERE
        }
      } else if (numFailedTasks != 0 && stageStatus == StageStatus.FAILED && numFailedTasks / numCompleteTasks > 0) {
        severityTemp = Severity.CRITICAL
      }
      return Severity.max(severityTemp, severityStage)
    }

    /**
      * checks whether the error message contains the corresponding error
      *
      * @param errorMessage : the entire error message
      * @param whichError   : the error we want to search the error message with
      * @param noTasks      : number of tasks having that error
      * @return : returning the number of tasks having the error.
      */
    private def hasError(errorMessage: String, whichError: String, noTasks: Int): Int = {
      if (errorMessage.contains(whichError))
        return noTasks + 1
      return noTasks
    }

    lazy val (severityOOMStages: Severity, severityOverheadStages: Severity, stagesWithOOMError: Int, stagesWithOverheadError: Int) = getErrorsSeverity
    lazy val severity: Severity = Severity.max(severityOverheadStages, severityOOMStages)
  }

}