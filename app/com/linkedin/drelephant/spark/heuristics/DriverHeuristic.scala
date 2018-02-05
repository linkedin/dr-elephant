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

import java.util.ArrayList

import scala.collection.JavaConverters
import scala.util.Try
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.MemoryFormatUtils
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary

/**
  * A heuristic based the driver's configurations and memory used.
  * It checks whether the configuration values specified are within the threshold range.
  * It also analyses the peak JVM memory used and time spent in GC by the job.
  */
class DriverHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import DriverHeuristic._
  import JavaConverters._

  val gcSeverityThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(GC_SEVERITY_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_GC_SEVERITY_THRESHOLDS)

  val sparkOverheadMemoryThreshold: SeverityThresholds = SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY), ascending = true)
    .getOrElse(DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS)

  val sparkExecutorMemoryThreshold: String = heuristicConfigurationData.getParamMap.getOrDefault(SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY, DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD)

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  lazy val driverPeakJvmMemoryThresholdString: String = heuristicConfigurationData.getParamMap.get(MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY)

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatProperty(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    var resultDetails = Seq(
      new HeuristicResultDetails(
        SPARK_DRIVER_MEMORY_KEY,
        formatProperty(evaluator.driverMemoryBytes.map(MemoryFormatUtils.bytesToString))
      ),
      new HeuristicResultDetails(
        "Ratio of time spent in GC to total time", evaluator.ratio.toString
      ),
      new HeuristicResultDetails(
        SPARK_DRIVER_CORES_KEY,
        formatProperty(evaluator.driverCores.map(_.toString))
      ),
      new HeuristicResultDetails(
        SPARK_YARN_DRIVER_MEMORY_OVERHEAD,
        evaluator.sparkYarnDriverMemoryOverhead
      ),
      new HeuristicResultDetails("Max driver peak JVM used memory", MemoryFormatUtils.bytesToString(evaluator.maxDriverPeakJvmUsedMemory))
    )
    if(evaluator.severityJvmUsedMemory != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Driver Peak JVM used Memory", "The allocated memory for the driver (in " + SPARK_DRIVER_MEMORY_KEY + ") is much more than the peak JVM used memory by the driver.")
    }
    if (evaluator.severityGc != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Gc ratio high", "The driver is spending too much time on GC. We recommend increasing the driver memory.")
    }
    if(evaluator.severityDriverCores != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Driver Cores", "Please do not specify excessive number of driver cores. Change it in the field : " + SPARK_DRIVER_CORES_KEY)
    }
    if(evaluator.severityDriverMemoryOverhead != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Driver Overhead Memory", "Please do not specify excessive amount of overhead memory for Driver. Change it in the field " + SPARK_YARN_DRIVER_MEMORY_OVERHEAD)
    }
    if(evaluator.severityDriverMemory != Severity.NONE) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Spark Driver Memory", "Please do not specify excessive amount of memory for Driver. Change it in the field " + SPARK_DRIVER_MEMORY_KEY)
    }

    // Constructing a mutable ArrayList for resultDetails, otherwise addResultDetail method HeuristicResult cannot be used.
    val mutableResultDetailsArrayList = new ArrayList(resultDetails.asJava)
    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0,
      mutableResultDetailsArrayList
    )
    result
  }
}

object DriverHeuristic {

  val SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory"
  val SPARK_DRIVER_CORES_KEY = "spark.driver.cores"
  val SPARK_YARN_DRIVER_MEMORY_OVERHEAD = "spark.yarn.driver.memoryOverhead"
  val SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY = "spark.overheadMemory.thresholds.key"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold_key"
  val EXECUTION_MEMORY = "executionMemory"
  val STORAGE_MEMORY = "storageMemory"
  val JVM_USED_MEMORY = "jvmUsedMemory"

  // 300 * FileUtils.ONE_MB (300 * 1024 * 1024)
  val reservedMemory : Long = 314572800
  val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "peak_jvm_memory_threshold"
  val GC_SEVERITY_THRESHOLDS_KEY: String = "gc_severity_threshold"
  val DEFAULT_GC_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.09D, severe = 0.1D, critical = 0.15D, ascending = true)

  val DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS =
    SeverityThresholds(low = MemoryFormatUtils.stringToBytes("2G"), MemoryFormatUtils.stringToBytes("4G"),
      severe = MemoryFormatUtils.stringToBytes("6G"), critical = MemoryFormatUtils.stringToBytes("8G"), ascending = true)

  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"

  class Evaluator(driverHeuristic: DriverHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorSummaries : Seq[ExecutorSummary] = data.executorSummaries
    lazy val driver : ExecutorSummary = executorSummaries.find(_.id == "driver").getOrElse(null)

    if(driver == null) {
      throw new Exception("No driver found!")
    }

    //peakJvmMemory calculations
    val maxDriverPeakJvmUsedMemory : Long = driver.peakJvmUsedMemory.getOrElse(JVM_USED_MEMORY, 0L).asInstanceOf[Number].longValue

    lazy val DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.25 * (maxDriverPeakJvmUsedMemory + reservedMemory), moderate = 1.5 * (maxDriverPeakJvmUsedMemory + reservedMemory),
        severe = 2 * (maxDriverPeakJvmUsedMemory + reservedMemory), critical = 3 * (maxDriverPeakJvmUsedMemory + reservedMemory), ascending = true)

    val MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS : SeverityThresholds = if(driverHeuristic.driverPeakJvmMemoryThresholdString == null) {
      DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS
    } else {
      SeverityThresholds.parse(driverHeuristic.driverPeakJvmMemoryThresholdString.split(",").map(_.toDouble * (maxDriverPeakJvmUsedMemory + reservedMemory)).toString, ascending = false).getOrElse(DEFAULT_MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS)
    }

    lazy val severityJvmUsedMemory : Severity = if (driverMemoryBytes.getOrElse(0L).asInstanceOf[Number].longValue <= MemoryFormatUtils.stringToBytes(driverHeuristic.sparkExecutorMemoryThreshold)) {
      Severity.NONE
    } else {
      MAX_DRIVER_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(driverMemoryBytes.getOrElse(0L).asInstanceOf[Number].longValue)
    }

    //Gc Calculations
    val ratio : Double = driver.totalGCTime.toDouble / driver.totalDuration.toDouble
    val severityGc = driverHeuristic.gcSeverityThresholds.severityOf(ratio)

    lazy val driverMemoryBytes: Option[Long] =
      Try(getProperty(SPARK_DRIVER_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(None)

    lazy val driverCores: Option[Int] =
      Try(getProperty(SPARK_DRIVER_CORES_KEY).map(_.toInt)).getOrElse(None)

    lazy val sparkYarnDriverMemoryOverhead: String = if (getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0").matches("(.*)[0-9]"))
      MemoryFormatUtils.bytesToString(MemoryFormatUtils.stringToBytes(getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0") + "MB")) else getProperty(SPARK_YARN_DRIVER_MEMORY_OVERHEAD).getOrElse("0")

    //The following thresholds are for checking if the memory and cores values (driver) are above normal. These thresholds are experimental, and may change in the future.
    val DEFAULT_SPARK_MEMORY_THRESHOLDS =
      SeverityThresholds(low = MemoryFormatUtils.stringToBytes("10G"), MemoryFormatUtils.stringToBytes("15G"),
        severe = MemoryFormatUtils.stringToBytes("20G"), critical = MemoryFormatUtils.stringToBytes("25G"), ascending = true)
    val DEFAULT_SPARK_CORES_THRESHOLDS =
      SeverityThresholds(low = 4, moderate = 6, severe = 8, critical = 10, ascending = true)

    val severityDriverMemory = DEFAULT_SPARK_MEMORY_THRESHOLDS.severityOf(driverMemoryBytes.getOrElse(0).asInstanceOf[Number].longValue)
    val severityDriverCores = DEFAULT_SPARK_CORES_THRESHOLDS.severityOf(driverCores.getOrElse(0).asInstanceOf[Number].intValue)
    val severityDriverMemoryOverhead = driverHeuristic.sparkOverheadMemoryThreshold.severityOf(MemoryFormatUtils.stringToBytes(sparkYarnDriverMemoryOverhead))

    //Severity for the configuration thresholds
    val severityConfThresholds: Severity = Severity.max(severityDriverCores, severityDriverMemory, severityDriverMemoryOverhead)
    lazy val severity: Severity = Severity.max(severityConfThresholds, severityGc, severityJvmUsedMemory)
    private def getProperty(key: String): Option[String] = appConfigurationProperties.get(key)
  }

}
