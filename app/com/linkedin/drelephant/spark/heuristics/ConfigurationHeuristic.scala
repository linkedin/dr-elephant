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

import com.linkedin.drelephant.math.Statistics

import scala.collection.JavaConverters
import scala.util.Try

import com.linkedin.drelephant.analysis.{HeuristicResultDetails, Heuristic, HeuristicResult, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.MemoryFormatUtils


/**
  * A heuristic based on an app's known configuration.
  *
  * The results from this heuristic primarily inform users about key app configuration settings, including
  * driver memory, executor cores, executor instances, executor memory, and the serializer.
  */
class ConfigurationHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
    extends Heuristic[SparkApplicationData] {
  import ConfigurationHeuristic._
  import JavaConverters._

  val serializerIfNonNullRecommendation: String =
    Option(heuristicConfigurationData.getParamMap.get(SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY))
      .getOrElse(DEFAULT_SERIALIZER_IF_NON_NULL_RECOMMENDATION)

  val serializerIfNonNullSeverityIfRecommendationUnmet: Severity =
    DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    def formatProperty(property: Option[String]): String =
      property.getOrElse("Not presented. Using default.")

    val resultDetails = Seq(
      new HeuristicResultDetails(
        SPARK_DRIVER_MEMORY_KEY,
        formatProperty(evaluator.driverMemoryBytes.map(MemoryFormatUtils.bytesToString))
      ),
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
        SPARK_SERIALIZER_KEY,
        formatProperty(evaluator.serializer)
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

object ConfigurationHeuristic {
  val DEFAULT_SERIALIZER_IF_NON_NULL_RECOMMENDATION = "org.apache.spark.serializer.KryoSerializer"
  val DEFAULT_SERIALIZER_IF_NON_NULL_SEVERITY_IF_RECOMMENDATION_UNMET = Severity.MODERATE

  val SERIALIZER_IF_NON_NULL_RECOMMENDATION_KEY = "serializer_if_non_null_recommendation"

  val SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"

  class Evaluator(configurationHeuristic: ConfigurationHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    lazy val driverMemoryBytes: Option[Long] =
      Try(getProperty(SPARK_DRIVER_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(None)

    lazy val executorMemoryBytes: Option[Long] =
      Try(getProperty(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(None)

    lazy val executorInstances: Option[Int] =
      Try(getProperty(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)).getOrElse(None)

    lazy val executorCores: Option[Int] =
      Try(getProperty(SPARK_EXECUTOR_CORES_KEY).map(_.toInt)).getOrElse(None)

    lazy val applicationDuration : Long = {
      require(data.applicationInfo.attempts.nonEmpty)
      val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
      (lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime) / Statistics.SECOND_IN_MS
    }

    lazy val serializer: Option[String] = getProperty(SPARK_SERIALIZER_KEY)

    lazy val serializerSeverity: Severity = serializer match {
      case None => Severity.NONE
      case Some(`serializerIfNonNullRecommendation`) => Severity.NONE
      case Some(_) => serializerIfNonNullSeverityIfRecommendationUnmet
    }

    lazy val severity: Severity = serializerSeverity

    private val serializerIfNonNullRecommendation: String = configurationHeuristic.serializerIfNonNullRecommendation

    private val serializerIfNonNullSeverityIfRecommendationUnmet: Severity =
      configurationHeuristic.serializerIfNonNullSeverityIfRecommendationUnmet

    private def getProperty(key: String): Option[String] = appConfigurationProperties.get(key)
  }
}
