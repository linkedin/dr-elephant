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
import com.linkedin.drelephant.math.Statistics

/**
  * A heuristic based on an app's known configuration.
  *
  * The results from this heuristic primarily inform users about key app configuration settings, including
  * executor cores, executor instances, executor memory, and the serializer.
  *
  * It also checks whether the values specified are within threshold.
  */
class ConfigurationHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import ConfigurationHeuristic._
  import JavaConverters._

  val sparkOverheadMemoryThreshold: SeverityThresholds = SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY), ascending = true)
    .getOrElse(DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS)

  val serializerIfNonNullRecommendation: String =
    Option(heuristicConfigurationData.getParamMap.get(SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY))
      .getOrElse(DEFAULT_SERIALIZER_IF_NON_NULL_RECOMMENDATION)

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatProperty(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    val resultDetails = Seq(
      new HeuristicResultDetails(
        SPARK_EXECUTOR_MEMORY_KEY,
        formatProperty(evaluator.executorMemoryBytes.map(MemoryFormatUtils.bytesToString))
      ),
      new HeuristicResultDetails(
        SPARK_EXECUTOR_INSTANCES_KEY,
        formatProperty(evaluator.executorInstances.map(_.toString))
      ),
      new HeuristicResultDetails(
        SPARK_EXECUTOR_CORES_KEY,
        formatProperty(evaluator.executorCores.map(_.toString))
      ),
      new HeuristicResultDetails(
        SPARK_APPLICATION_DURATION,
        evaluator.applicationDuration.toString + " Seconds"
      ),
      new HeuristicResultDetails(
        SPARK_DYNAMIC_ALLOCATION_ENABLED,
        formatProperty(evaluator.isDynamicAllocationEnabled.map(_.toString))
      ),
      new HeuristicResultDetails(
        SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD,
        evaluator.sparkYarnExecutorMemoryOverhead
      ),
      new HeuristicResultDetails(
        SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
        evaluator.dynamicMinExecutors.getOrElse(0).toString
      ),
      new HeuristicResultDetails(
        SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS,
        evaluator.dynamicMaxExecutors.getOrElse(0).toString
      )
    )
    // Constructing a mutable ArrayList for resultDetails, otherwise addResultDetail method HeuristicResult cannot be used.
    val mutableResultDetailsArrayList = new ArrayList(resultDetails.asJava)
    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0,
      mutableResultDetailsArrayList
    )
    if (evaluator.serializerSeverity != Severity.NONE) {
      result.addResultDetail(SPARK_SERIALIZER_KEY, formatProperty(evaluator.serializer),
        "KyroSerializer is Not Enabled.")
    }
    if (evaluator.shuffleAndDynamicAllocationSeverity != Severity.NONE) {
      result.addResultDetail(SPARK_SHUFFLE_SERVICE_ENABLED, formatProperty(evaluator.isShuffleServiceEnabled.map(_.toString)),
        "Spark shuffle service is not enabled.")
    }
    if (evaluator.severityMinExecutors != Severity.NONE) {
      result.addResultDetail("Minimum Executors", "The minimum executors for Dynamic Allocation should be "+ THRESHOLD_MIN_EXECUTORS + ". Please change it in the " + SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS + " field.")
    }
    if (evaluator.severityMaxExecutors != Severity.NONE) {
      result.addResultDetail("Maximum Executors", "The maximum executors for Dynamic Allocation should be <=" + THRESHOLD_MAX_EXECUTORS + ". Please change it in the " + SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS + " field.")
    }
    if (evaluator.jarsSeverity != Severity.NONE) {
      result.addResultDetail("Jars notation", "It is recommended to not use * notation while specifying jars in the field " + SPARK_YARN_JARS)
    }
    if(evaluator.severityExecutorMemoryOverhead != Severity.NONE) {
      result.addResultDetail("Executor Overhead Memory", "Please do not specify excessive amount of overhead memory for Executors. Change it in the field " + SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD)
    }
    if(evaluator.severityExecutorCores != Severity.NONE) {
      result.addResultDetail("Executor cores", "The number of executor cores should be <=" + evaluator.DEFAULT_SPARK_CORES_THRESHOLDS.low + ". Please change it in the field " + SPARK_EXECUTOR_CORES_KEY)
    }
    if(evaluator.severityExecutorMemory != Severity.NONE) {
      result.addResultDetail("Executor memory", "Please do not specify excessive amount of executor memory. Change it in the field " + SPARK_EXECUTOR_MEMORY_KEY)
    }
    result
  }
}

object ConfigurationHeuristic {
  val DEFAULT_SERIALIZER_IF_NON_NULL_RECOMMENDATION = "org.apache.spark.serializer.KryoSerializer"
  val DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET = Severity.MODERATE

  val SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY = "serializer_if_non_null_recommendation"

  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"
  val SPARK_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
  val SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors"
  val SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors"
  val SPARK_YARN_JARS = "spark.yarn.secondary.jars"
  val SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"
  val THRESHOLD_MIN_EXECUTORS: Int = 1
  val THRESHOLD_MAX_EXECUTORS: Int = 900
  val SPARK_OVERHEAD_MEMORY_THRESHOLD_KEY = "spark.overheadMemory.thresholds.key"
  val DEFAULT_SPARK_OVERHEAD_MEMORY_THRESHOLDS =
    SeverityThresholds(low = MemoryFormatUtils.stringToBytes("2G"), MemoryFormatUtils.stringToBytes("4G"),
      severe = MemoryFormatUtils.stringToBytes("6G"), critical = MemoryFormatUtils.stringToBytes("8G"), ascending = true)

  class Evaluator(configurationHeuristic: ConfigurationHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val executorMemoryBytes: Option[Long] =
      Try(getProperty(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(None)

    lazy val executorInstances: Option[Int] =
      Try(getProperty(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)).getOrElse(None)

    lazy val executorCores: Option[Int] =
      Try(getProperty(SPARK_EXECUTOR_CORES_KEY).map(_.toInt)).getOrElse(None)

    lazy val dynamicMinExecutors: Option[Int] =
      Try(getProperty(SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS).map(_.toInt)).getOrElse(None)

    lazy val dynamicMaxExecutors: Option[Int] =
      Try(getProperty(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS).map(_.toInt)).getOrElse(None)

    lazy val applicationDuration: Long = {
      require(data.applicationInfo.attempts.nonEmpty)
      val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
      (lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime) / Statistics.SECOND_IN_MS
    }

    lazy val sparkYarnJars: String = getProperty(SPARK_YARN_JARS).getOrElse("")

    lazy val jarsSeverity: Severity = if (sparkYarnJars.contains("*")) {
        Severity.CRITICAL
      } else {
        Severity.NONE
      }

    lazy val sparkYarnExecutorMemoryOverhead: String = if (getProperty(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD).getOrElse("0").matches("(.*)[0-9]"))
      MemoryFormatUtils.bytesToString(MemoryFormatUtils.stringToBytes(getProperty(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD).getOrElse("0") + "MB")) else (getProperty(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD).getOrElse("0"))

    lazy val serializer: Option[String] = getProperty(SPARK_SERIALIZER_KEY)

    /**
      * If the serializer is either not configured or not equal to KryoSerializer, then the severity will be moderate.
      */

    lazy val serializerSeverity: Severity = serializer match {
      case None => Severity.MODERATE
      case Some(`serializerIfNonNullRecommendation`) => Severity.NONE
      case Some(_) => DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET
    }

    //The following thresholds are for checking if the memory and cores values of executors are above normal. These thresholds are experimental, and may change in the future.
    val DEFAULT_SPARK_MEMORY_THRESHOLDS =
      SeverityThresholds(low = MemoryFormatUtils.stringToBytes("10G"), MemoryFormatUtils.stringToBytes("15G"),
        severe = MemoryFormatUtils.stringToBytes("20G"), critical = MemoryFormatUtils.stringToBytes("25G"), ascending = true)
    val DEFAULT_SPARK_CORES_THRESHOLDS =
      SeverityThresholds(low = 4, moderate = 6, severe = 8, critical = 10, ascending = true)

    val severityExecutorMemory = DEFAULT_SPARK_MEMORY_THRESHOLDS.severityOf(executorMemoryBytes.getOrElse(0).asInstanceOf[Number].longValue)
    val severityExecutorCores = DEFAULT_SPARK_CORES_THRESHOLDS.severityOf(executorCores.getOrElse(0).asInstanceOf[Number].intValue)
    val severityMinExecutors = if (dynamicMinExecutors.getOrElse(0).asInstanceOf[Number].intValue > THRESHOLD_MIN_EXECUTORS) {
      Severity.CRITICAL
    } else {
      Severity.NONE
    }
    val severityMaxExecutors = if (dynamicMaxExecutors.getOrElse(0).asInstanceOf[Number].intValue > THRESHOLD_MAX_EXECUTORS) {
      Severity.CRITICAL
    } else {
      Severity.NONE
    }
    val severityExecutorMemoryOverhead = configurationHeuristic.sparkOverheadMemoryThreshold.severityOf(MemoryFormatUtils.stringToBytes(sparkYarnExecutorMemoryOverhead))

    //Severity for the configuration thresholds
    val severityConfThresholds: Severity = Severity.max(severityExecutorCores, severityExecutorMemory,
      severityMinExecutors, severityMaxExecutors, jarsSeverity, severityExecutorMemoryOverhead)

    /**
     * The following logic computes severity based on shuffle service and dynamic allocation flags.
     * If dynamic allocation is disabled, then the severity will be MODERATE if shuffle service is disabled or not specified.
     * If dynamic allocation is enabled, then the severity will be SEVERE if shuffle service is disabled or not specified.
     */

    lazy val isDynamicAllocationEnabled: Option[Boolean] = Some(getProperty(SPARK_DYNAMIC_ALLOCATION_ENABLED).exists(_.toBoolean == true))
    lazy val isShuffleServiceEnabled: Option[Boolean] = Some(getProperty(SPARK_SHUFFLE_SERVICE_ENABLED).exists(_.toBoolean == true))

    lazy val shuffleAndDynamicAllocationSeverity = (isDynamicAllocationEnabled, isShuffleServiceEnabled) match {
      case (_, Some(true)) => Severity.NONE
      case (Some(false), Some(false)) => Severity.MODERATE
      case (Some(true), Some(false)) => Severity.SEVERE
    }

    lazy val severity: Severity = Severity.max(serializerSeverity, shuffleAndDynamicAllocationSeverity, severityConfThresholds)

    private val serializerIfNonNullRecommendation: String = configurationHeuristic.serializerIfNonNullRecommendation

    private def getProperty(key: String): Option[String] = appConfigurationProperties.get(key)
  }

}
