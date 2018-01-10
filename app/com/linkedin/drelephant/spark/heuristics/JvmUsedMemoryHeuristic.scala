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
  * A heuristic based on peak JVM used memory for the spark executors and driver
  *
  */
class JvmUsedMemoryHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import JvmUsedMemoryHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  lazy val executorPeakJvmMemoryThresholdString: String = heuristicConfigurationData.getParamMap.get(MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY)
  lazy val driverPeakJvmMemoryThresholdString: String = heuristicConfigurationData.getParamMap.get(MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY)

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    var resultDetails = Seq(
      new HeuristicResultDetails("Max executor peak JVM used memory", MemoryFormatUtils.bytesToString(evaluator.maxExecutorPeakJvmUsedMemory)),
      new HeuristicResultDetails("Max driver peak JVM used memory", MemoryFormatUtils.bytesToString(evaluator.maxDriverPeakJvmUsedMemory)),
      new HeuristicResultDetails("spark.executor.memory", MemoryFormatUtils.bytesToString(evaluator.sparkExecutorMemory)),
      new HeuristicResultDetails("spark.driver.memory", MemoryFormatUtils.bytesToString(evaluator.sparkDriverMemory))
    )

    if(evaluator.severityExecutor.getValue > Severity.LOW.getValue) {
      resultDetails :+ new HeuristicResultDetails("Executor Memory", "The allocated memory for the executor (in " + SPARK_EXECUTOR_MEMORY +") is much more than the peak JVM used memory by executors.")
      resultDetails :+ new HeuristicResultDetails("Reasonable size for executor memory", ((1+BUFFER_PERCENT.toDouble/100.0)*evaluator.maxExecutorPeakJvmUsedMemory).toString)
    }

    if(evaluator.severityDriver.getValue > Severity.LOW.getValue) {
      resultDetails :+ new HeuristicResultDetails("Driver Memory", "The allocated memory for the driver (in " + SPARK_DRIVER_MEMORY + ") is much more than the peak JVM used memory by the driver.")
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
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  // 300 * FileUtils.ONE_MB (300 * 1024 * 1024)
  val reservedMemory : Long = 314572800
  val BUFFER_PERCENT : Int = 20
  val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "executor_peak_jvm_memory_threshold"
  val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "driver_peak_jvm_memory_threshold"

  class Evaluator(jvmUsedMemoryHeuristic: JvmUsedMemoryHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val driverSummary : Option[ExecutorSummary] = executorSummaries.find(_.id.equals("driver"))
    val maxDriverPeakJvmUsedMemory : Long = driverSummary.get.peakJvmUsedMemory.getOrElse(JVM_USED_MEMORY, 0L).asInstanceOf[Number].longValue
    val executorList : Seq[ExecutorSummary] = executorSummaries.filterNot(_.id.equals("driver"))
    val sparkExecutorMemory : Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)
    val sparkDriverMemory : Long = appConfigurationProperties.get(SPARK_DRIVER_MEMORY).map(MemoryFormatUtils.stringToBytes).getOrElse(0L)
    lazy val maxExecutorPeakJvmUsedMemory: Long = if (executorList.isEmpty) 0L else executorList.map {
      _.peakJvmUsedMemory.getOrElse(JVM_USED_MEMORY, 0).asInstanceOf[Number].longValue
    }.max

    lazy val DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.5 * (maxExecutorPeakJvmUsedMemory + reservedMemory), moderate = 2 * (maxExecutorPeakJvmUsedMemory + reservedMemory), severe = 4 * (maxExecutorPeakJvmUsedMemory + reservedMemory), critical = 8 * (maxExecutorPeakJvmUsedMemory + reservedMemory), ascending = true)

    lazy val DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.5 * (maxDriverPeakJvmUsedMemory + reservedMemory), moderate = 2 * (maxDriverPeakJvmUsedMemory + reservedMemory), severe = 4 * (maxDriverPeakJvmUsedMemory + reservedMemory), critical = 8 * (maxDriverPeakJvmUsedMemory + reservedMemory), ascending = true)

    val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS : SeverityThresholds = if(jvmUsedMemoryHeuristic.executorPeakJvmMemoryThresholdString == null) {
      DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS
    } else {
      SeverityThresholds.parse(jvmUsedMemoryHeuristic.executorPeakJvmMemoryThresholdString.split(",").map(_.toDouble * (maxExecutorPeakJvmUsedMemory + reservedMemory)).toString, ascending = false).getOrElse(DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS)
    }

    val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS : SeverityThresholds = if(jvmUsedMemoryHeuristic.driverPeakJvmMemoryThresholdString == null) {
      DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS
    } else {
      SeverityThresholds.parse(jvmUsedMemoryHeuristic.driverPeakJvmMemoryThresholdString.split(",").map(_.toDouble * (maxDriverPeakJvmUsedMemory + reservedMemory)).toString, ascending = false).getOrElse(DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS)
    }

    val severityExecutor = MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(sparkExecutorMemory)
    val severityDriver = MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(sparkDriverMemory)

    /**
      * disabling the skew check for executors
      * val severitySkew = DEFAULT_JVM_MEMORY_SKEW_THRESHOLDS.severityOf(maxExecutorPeakJvmUsedMemory)
      */
    val severity : Severity = Severity.max(severityDriver, severityExecutor)
  }
}
