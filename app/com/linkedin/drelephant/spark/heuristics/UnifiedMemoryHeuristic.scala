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
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary
import com.linkedin.drelephant.util.MemoryFormatUtils

import scala.collection.JavaConverters


/**
  * A heuristic based on peak unified memory for the spark executors
  *
  * This heuristic reports the fraction of memory used/ memory allocated and if the fraction can be reduced. Also, it checks for the skew in peak unified memory and reports if the skew is too much.
  */
class UnifiedMemoryHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import UnifiedMemoryHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  lazy val peakUnifiedMemoryThresholdString: String = heuristicConfigurationData.getParamMap.get(PEAK_UNIFIED_MEMORY_THRESHOLD_KEY)
  val sparkExecutorMemoryThreshold: String = heuristicConfigurationData.getParamMap.getOrDefault(SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY, DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD)

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    var resultDetails = Seq(
      new HeuristicResultDetails("Unified Memory Space Allocated", MemoryFormatUtils.bytesToString(evaluator.maxMemory)),
      new HeuristicResultDetails("Mean peak unified memory", MemoryFormatUtils.bytesToString(evaluator.meanUnifiedMemory)),
      new HeuristicResultDetails("Max peak unified memory", MemoryFormatUtils.bytesToString(evaluator.maxUnifiedMemory)),
      new HeuristicResultDetails("spark.executor.memory", MemoryFormatUtils.bytesToString(evaluator.sparkExecutorMemory)),
      new HeuristicResultDetails("spark.memory.fraction", evaluator.sparkMemoryFraction.toString)
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

object UnifiedMemoryHeuristic {

  val EXECUTION_MEMORY = "executionMemory"
  val STORAGE_MEMORY = "storageMemory"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_MEMORY_FRACTION_KEY = "spark.memory.fraction"
  val PEAK_UNIFIED_MEMORY_THRESHOLD_KEY = "peak_unified_memory_threshold"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"
  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"

  class Evaluator(unifiedMemoryHeuristic: UnifiedMemoryHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD: SeverityThresholds = SeverityThresholds(low = 0.7 * maxMemory, moderate = 0.6 * maxMemory, severe = 0.4 * maxMemory, critical = 0.2 * maxMemory, ascending = false)

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    if (executorSummaries == null) {
      throw new Exception("Executors Summary is null.")
    }

    val executorList: Seq[ExecutorSummary] = executorSummaries.filterNot(_.id.equals("driver"))
    if (executorList.isEmpty) {
      throw new Exception("No executor information available.")
    }

    //allocated memory for the unified region
    val maxMemory: Long = executorList.head.maxMemory

    val PEAK_UNIFIED_MEMORY_THRESHOLDS: SeverityThresholds = if (unifiedMemoryHeuristic.peakUnifiedMemoryThresholdString == null) {
      DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD
    } else {
      SeverityThresholds.parse(unifiedMemoryHeuristic.peakUnifiedMemoryThresholdString.split(",").map(_.toDouble * maxMemory).toString, ascending = false).getOrElse(DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD)
    }

    val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)

    lazy val sparkMemoryFraction: Double = appConfigurationProperties.getOrElse(SPARK_MEMORY_FRACTION_KEY, "0.6").toDouble

    lazy val meanUnifiedMemory: Long = (executorList.map {
      executorSummary => {
        (executorSummary.peakUnifiedMemory.getOrElse(EXECUTION_MEMORY, 0).asInstanceOf[Number].longValue
          + executorSummary.peakUnifiedMemory.getOrElse(STORAGE_MEMORY, 0).asInstanceOf[Number].longValue)
      }
    }.sum) / executorList.size

    lazy val maxUnifiedMemory: Long = executorList.map {
      executorSummary => {
        (executorSummary.peakUnifiedMemory.getOrElse(EXECUTION_MEMORY, 0).asInstanceOf[Number].longValue
          + executorSummary.peakUnifiedMemory.getOrElse(STORAGE_MEMORY, 0).asInstanceOf[Number].longValue)
      }
    }.max

    lazy val severity: Severity = if (sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(unifiedMemoryHeuristic.sparkExecutorMemoryThreshold)) {
      Severity.NONE
    } else {
      PEAK_UNIFIED_MEMORY_THRESHOLDS.severityOf(maxUnifiedMemory)
    }
  }
}
