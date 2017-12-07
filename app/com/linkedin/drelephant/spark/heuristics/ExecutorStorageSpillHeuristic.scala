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

import com.linkedin.drelephant.analysis.Severity
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorStageSummary, ExecutorSummary, StageData}
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.MemoryFormatUtils

import scala.collection.JavaConverters


/**
  * A heuristic based on memory spilled.
  *
  */
class ExecutorStorageSpillHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import ExecutorStorageSpillHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Total memory spilled", MemoryFormatUtils.bytesToString(evaluator.totalMemorySpilled)),
      new HeuristicResultDetails("Max memory spilled", MemoryFormatUtils.bytesToString(evaluator.maxMemorySpilled)),
      new HeuristicResultDetails("Mean memory spilled", MemoryFormatUtils.bytesToString(evaluator.meanMemorySpilled)),
      new HeuristicResultDetails("Fraction of executors having non zero bytes spilled", evaluator.fractionOfExecutorsHavingBytesSpilled.toString)
    )

    if(evaluator.severity != Severity.NONE){
      resultDetails :+ new HeuristicResultDetails("Note", "Your memory is being spilled. Kindly look into it.")
      if(evaluator.sparkExecutorCores >=4 && evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes("10GB")) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try decreasing the number of cores to reduce the number of concurrently running tasks.")
      }
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

object ExecutorStorageSpillHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  class Evaluator(memoryFractionHeuristic: ExecutorStorageSpillHeuristic, data: SparkApplicationData) {
    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    val ratioMemoryCores: Long = (sparkExecutorMemory / sparkExecutorCores)
    val maxMemorySpilled: Long = executorSummaries.map(_.totalMemoryBytesSpilled).max
    val meanMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum / executorSummaries.size
    val totalMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum
    val fractionOfExecutorsHavingBytesSpilled: Double = executorSummaries.count(_.totalMemoryBytesSpilled > 0).toDouble / executorSummaries.size.toDouble
    val severity: Severity = {
      if (fractionOfExecutorsHavingBytesSpilled != 0) {
        if (fractionOfExecutorsHavingBytesSpilled < 0.2 && maxMemorySpilled < 0.05 * ratioMemoryCores) {
          Severity.LOW
        }
        else if (fractionOfExecutorsHavingBytesSpilled < 0.2 && meanMemorySpilled < 0.05 * ratioMemoryCores) {
          Severity.MODERATE
        }

        else if (fractionOfExecutorsHavingBytesSpilled >= 0.2 && meanMemorySpilled < 0.05 * ratioMemoryCores) {
          Severity.SEVERE
        }
        else if (fractionOfExecutorsHavingBytesSpilled >= 0.2 && meanMemorySpilled >= 0.05 * ratioMemoryCores) {
          Severity.CRITICAL
        } else Severity.NONE
      }
      else Severity.NONE
    }

    lazy val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0)
    lazy val sparkExecutorCores: Int = (appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt)).getOrElse(0)
  }
}

