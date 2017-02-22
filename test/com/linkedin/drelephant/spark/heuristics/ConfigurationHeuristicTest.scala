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

import com.linkedin.drelephant.spark.data.SparkRestDerivedData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfo, ApplicationInfo}
import scala.collection.JavaConverters

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}
import java.util.Date


class ConfigurationHeuristicTest extends FunSpec with Matchers {
  import ConfigurationHeuristicTest._

  describe("ConfigurationHeuristic") {
    val heuristicConfigurationData = newFakeHeuristicConfigurationData(
      Map(
        "serializer_if_non_null_recommendation" -> "org.apache.spark.serializer.KryoSerializer",
        "shuffle_manager_if_non_null_recommendation" -> "sort"
      )
    )

    val configurationHeuristic = new ConfigurationHeuristic(heuristicConfigurationData)

    describe(".apply") {
      val configurationProperties = Map(
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.storage.memoryFraction" -> "0.3",
        "spark.driver.memory" -> "2G",
        "spark.executor.instances" -> "900",
        "spark.executor.memory" -> "1g",
        "spark.shuffle.memoryFraction" -> "0.5"
      )

      val data = newFakeSparkApplicationData(configurationProperties)
      val heuristicResult = configurationHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.NONE)
      }

      it("returns the driver memory") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("spark.driver.memory")
        details.getValue should be("2 GB")
      }

      it("returns the executor memory") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("spark.executor.memory")
        details.getValue should be("1 GB")
      }

      it("returns the executor instances") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("spark.executor.instances")
        details.getValue should be("900")
      }

      it("returns the executor cores") {
        val details = heuristicResultDetails.get(3)
        details.getName should include("spark.executor.cores")
        details.getValue should include("default")
      }

      it("returns the application duration") {
        val details = heuristicResultDetails.get(4)
        details.getName should include("spark.application.duration")
        details.getValue should include("10")
      }

      it("returns the serializer") {
        val details = heuristicResultDetails.get(5)
        details.getName should include("spark.serializer")
        details.getValue should be("org.apache.spark.serializer.KryoSerializer")
      }
    }

    describe(".Evaluator") {
      import ConfigurationHeuristic.Evaluator

      def newEvaluatorWithConfigurationProperties(configurationProperties: Map[String, String]): Evaluator = {
        new Evaluator(configurationHeuristic, newFakeSparkApplicationData(configurationProperties))
      }

      it("has the driver memory bytes when they're present") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.driver.memory" -> "2G"))
        evaluator.driverMemoryBytes should be(Some(2L * 1024 * 1024 * 1024))
      }

      it("has no driver memory bytes when they're absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.driverMemoryBytes should be(None)
      }

      it("has the executor memory bytes when they're present") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.executor.memory" -> "1g"))
        evaluator.executorMemoryBytes should be(Some(1L * 1024 * 1024 * 1024))
      }

      it("has no executor memory bytes when they're absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.executorMemoryBytes should be(None)
      }

      it("has the executor instances when they're present") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.executor.instances" -> "900"))
        evaluator.executorInstances should be(Some(900))
      }

      it("has no executor instances when they're absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.executorInstances should be(None)
      }

      it("has the executor cores when they're present") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.executor.cores" -> "2"))
        evaluator.executorCores should be(Some(2))
      }

      it("has no executor cores when they're absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.executorCores should be(None)
      }

      it("has the serializer when it's present") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.KryoSerializer"))
      }

      it("has no serializer when it's absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.serializer should be(None)
      }

      it("has the severity of the serializer setting when it matches our recommendation") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"))
        evaluator.serializerSeverity should be(Severity.NONE)
      }

      it("has the severity of the serializer setting when it doesn't match our recommendation and is non-null") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.FooSerializer"))
        evaluator.serializerSeverity should be(Severity.MODERATE)
      }

      it("has the severity of the serializer setting when it is null") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.serializerSeverity should be(Severity.NONE)
      }

      it("computes the overall severity when there are some issues") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.FooSerializer"))
        evaluator.severity should be(Severity.MODERATE)
      }

      it("computes the overall severity when there are no issues") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.severity should be(Severity.NONE)
      }
    }
  }
}

object ConfigurationHeuristicTest {
  import JavaConverters._

  def newFakeHeuristicConfigurationData(params: Map[String, String] = Map.empty): HeuristicConfigurationData =
    new HeuristicConfigurationData("heuristic", "class", "view", new ApplicationType("type"), params.asJava)

  def newFakeSparkApplicationData(appConfigurationProperties: Map[String, String]): SparkApplicationData = {
    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> appConfigurationProperties.toSeq))
    )

    val appId = "application_1"
    val startDate = new Date()
    val endDate = new Date(startDate.getTime() + 10000)
    val applicationAttempt = new ApplicationAttemptInfo(Option("attempt1"),startDate, endDate, "sparkUser")
    val applicationAttempts = Seq(applicationAttempt)

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfo(appId, name = "app", applicationAttempts),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
