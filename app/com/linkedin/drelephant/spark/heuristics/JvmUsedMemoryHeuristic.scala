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
  * A heuristic based on peak JVM used memory for the spark executors
  *
  */
class JvmUsedMemoryHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import JvmUsedMemoryHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  lazy val executorPeakJvmMemoryThresholdString: String = heuristicConfigurationData.getParamMap.get(MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY)
  lazy val sparkExecutorMemoryThreshold: String = heuristicConfigurationData.getParamMap.getOrDefault(SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY, DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD)

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    var resultDetails = Seq(
      new HeuristicResultDetails("Max executor peak JVM used memory", MemoryFormatUtils.bytesToString(evaluator.maxExecutorPeakJvmUsedMemory)),
      new HeuristicResultDetails("spark.executor.memory", MemoryFormatUtils.bytesToString(evaluator.sparkExecutorMemory))
    )

    if (evaluator.severity != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Executor Memory", "The allocated memory for the executor (in " + SPARK_EXECUTOR_MEMORY + ") is much more than the peak JVM used memory by executors.")
      resultDetails = resultDetails :+ new HeuristicResultDetails("Suggested spark.executor.memory", MemoryFormatUtils.roundOffMemoryStringToNextInteger((MemoryFormatUtils.bytesToString(((1 + BUFFER_FRACTION) * evaluator.maxExecutorPeakJvmUsedMemory).toLong))))
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

object JvmUsedMemoryHeuristic {
  val JVM_USED_MEMORY = "jvmUsedMemory"
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"

  // 300 * FileUtils.ONE_MB (300 * 1024 * 1024)
  val reservedMemory: Long = 314572800
  val BUFFER_FRACTION: Double = 0.2
  val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "executor_peak_jvm_memory_threshold"
  lazy val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"

  class Evaluator(jvmUsedMemoryHeuristic: JvmUsedMemoryHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    val executorList: Seq[ExecutorSummary] = executorSummaries.filterNot(_.id.equals("driver"))
    val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)
    lazy val maxExecutorPeakJvmUsedMemory: Long = if (executorList.isEmpty) 0L else executorList.map {
      _.peakJvmUsedMemory.getOrElse(JVM_USED_MEMORY, 0).asInstanceOf[Number].longValue
    }.max

    lazy val DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.25 * (maxExecutorPeakJvmUsedMemory + reservedMemory), moderate = 1.5 * (maxExecutorPeakJvmUsedMemory + reservedMemory), severe = 2 * (maxExecutorPeakJvmUsedMemory + reservedMemory), critical = 3 * (maxExecutorPeakJvmUsedMemory + reservedMemory), ascending = true)

    val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS: SeverityThresholds = if (jvmUsedMemoryHeuristic.executorPeakJvmMemoryThresholdString == null) {
      DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS
    } else {
      SeverityThresholds.parse(jvmUsedMemoryHeuristic.executorPeakJvmMemoryThresholdString.split(",").map(_.toDouble * (maxExecutorPeakJvmUsedMemory + reservedMemory)).toString, ascending = false).getOrElse(DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS)
    }

    lazy val severity = if (sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(jvmUsedMemoryHeuristic.sparkExecutorMemoryThreshold)) {
      Severity.NONE
    } else {
      MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(sparkExecutorMemory)
    }
  }

}
