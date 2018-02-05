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
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfoImpl, ApplicationInfoImpl}
import scala.collection.JavaConverters

import com.linkedin.drelephant.analysis.{ApplicationType, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}
import java.util.Date

/**
  * Test class for Configuration Heuristic. It checks whether all the values used in the heuristic are calculated correctly.
  */
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

    describe("apply with NO Severity") {
      val configurationProperties = Map(
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.storage.memoryFraction" -> "0.3",
        "spark.driver.memory" -> "2G",
        "spark.executor.instances" -> "900",
        "spark.executor.memory" -> "1g",
        "spark.shuffle.memoryFraction" -> "0.5",
        "spark.shuffle.service.enabled" -> "true",
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.yarn.secondary.jars" -> "something without star",
        "spark.yarn.driver.memoryOverhead" -> "500"
      )

      val data = newFakeSparkApplicationData(configurationProperties)
      val heuristicResult = configurationHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the size of result details") {
        heuristicResultDetails.size() should be(8)
      }

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.NONE)
      }

      it("returns the executor memory") {
        val details = heuristicResultDetails.get(0)
        details.getName should include("spark.executor.memory")
        details.getValue should be("1 GB")
      }

      it("returns the executor instances") {
        val details = heuristicResultDetails.get(1)
        details.getName should include("spark.executor.instances")
        details.getValue should be("900")
      }

      it("returns the executor cores") {
        val details = heuristicResultDetails.get(2)
        details.getName should include("spark.executor.cores")
        details.getValue should include("default")
      }

      it("returns the application duration") {
        val details = heuristicResultDetails.get(3)
        details.getName should include("spark.application.duration")
        details.getValue should include("10")
      }

      it("returns the dynamic allocation flag") {
        val details = heuristicResultDetails.get(4)
        details.getName should include("spark.dynamicAllocation.enabled")
        details.getValue should be("true")
      }
    }

    describe("apply with Severity") {
      val configurationProperties = Map(
        "spark.serializer" -> "dummySerializer",
        "spark.shuffle.service.enabled" -> "false",
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.executor.cores" -> "5"
      )

      val data = newFakeSparkApplicationData(configurationProperties)
      val heuristicResult = configurationHeuristic.apply(data)
      val heuristicResultDetails = heuristicResult.getHeuristicResultDetails

      it("returns the size of result details") {
        heuristicResultDetails.size() should be(11)
      }

      it("returns the severity") {
        heuristicResult.getSeverity should be(Severity.SEVERE)
      }

      it("returns the dynamic allocation flag") {
        val details = heuristicResultDetails.get(4)
        details.getName should include("spark.dynamicAllocation.enabled")
        details.getValue should be("true")
      }

      it("returns the serializer") {
        val details = heuristicResultDetails.get(8)
        details.getName should include("spark.serializer")
        details.getValue should be("dummySerializer")
        details.getDetails should be("KyroSerializer is Not Enabled.")
      }

      it("returns the shuffle service flag") {
        val details = heuristicResultDetails.get(9)
        details.getName should include("spark.shuffle.service.enabled")
        details.getValue should be("false")
        details.getDetails should be("Spark shuffle service is not enabled.")
      }

      it("returns executor cores") {
        val details = heuristicResultDetails.get(10)
        details.getName should include("Executor cores")
        details.getValue should be("The number of executor cores should be <=4. Please change it in the field spark.executor.cores")
      }
    }

    describe(".Evaluator") {
      import ConfigurationHeuristic.Evaluator

      def newEvaluatorWithConfigurationProperties(configurationProperties: Map[String, String]): Evaluator = {
        new Evaluator(configurationHeuristic, newFakeSparkApplicationData(configurationProperties))
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

      it("jars severity when NONE") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.yarn.secondary.jars" -> "somethingWithoutStar"))
        evaluator.jarsSeverity should be(Severity.NONE)
      }

      it("jars severity when CRITICAL") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.yarn.secondary.jars" -> "somethingWith*.jar"))
        evaluator.jarsSeverity should be(Severity.CRITICAL)
      }

      it("has no serializer, dynamic allocation flag, and shuffle flag when they are absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map.empty)
        evaluator.serializer should be(None)
        evaluator.isDynamicAllocationEnabled should be(Some(false))
        evaluator.isShuffleServiceEnabled should be(Some(false))
        evaluator.serializerSeverity should be(Severity.MODERATE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.MODERATE)
        evaluator.severity should be(Severity.MODERATE)
      }

      it("has no dynamic allocation flag and shuffle flag, and serializer setting matches our recommendation") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.KryoSerializer"))
        evaluator.isDynamicAllocationEnabled should be(Some(false))
        evaluator.isShuffleServiceEnabled should be(Some(false))
        evaluator.serializerSeverity should be(Severity.NONE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.MODERATE)
        evaluator.severity should be(Severity.MODERATE)
      }

      it("has no dynamic allocation flag and shuffle flag, and serializer setting doesn't match our recommendation and is non-null") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.FooSerializer"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.FooSerializer"))
        evaluator.isDynamicAllocationEnabled should be(Some(false))
        evaluator.isShuffleServiceEnabled should be(Some(false))
        evaluator.serializerSeverity should be(Severity.MODERATE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.MODERATE)
        evaluator.severity should be(Severity.MODERATE)
      }

      it("true dynamic allocation flag and shuffle flag, and serializer setting matches our recommendation") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.shuffle.service.enabled" -> "true", "spark.dynamicAllocation.enabled" -> "true"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.KryoSerializer"))
        evaluator.isDynamicAllocationEnabled should be(Some(true))
        evaluator.isShuffleServiceEnabled should be(Some(true))
        evaluator.serializerSeverity should be(Severity.NONE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.NONE)
        evaluator.severity should be(Severity.NONE)
      }

      it("true dynamic allocation flag and shuffle flag, and serializer setting is absent") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.shuffle.service.enabled" -> "true",
          "spark.dynamicAllocation.enabled" -> "true"))
        evaluator.serializer should be(None)
        evaluator.isDynamicAllocationEnabled should be(Some(true))
        evaluator.isShuffleServiceEnabled should be(Some(true))
        evaluator.serializerSeverity should be(Severity.MODERATE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.NONE)
        evaluator.severity should be(Severity.MODERATE)
      }

      it("true dynamic allocation flag and false shuffle flag, and serializer setting matches our recommendation") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.shuffle.service.enabled" -> "false", "spark.dynamicAllocation.enabled" -> "true"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.KryoSerializer"))
        evaluator.isDynamicAllocationEnabled should be(Some(true))
        evaluator.isShuffleServiceEnabled should be(Some(false))
        evaluator.serializerSeverity should be(Severity.NONE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.SEVERE)
        evaluator.severityConfThresholds should be(Severity.NONE)
        evaluator.severity should be(Severity.SEVERE)
      }

      it("false dynamic allocation flag and shuffle flag, and serializer setting matches our recommendation") {
        val evaluator = newEvaluatorWithConfigurationProperties(Map("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
          "spark.shuffle.service.enabled" -> "false", "spark.dynamicAllocation.enabled" -> "false"))
        evaluator.serializer should be(Some("org.apache.spark.serializer.KryoSerializer"))
        evaluator.isDynamicAllocationEnabled should be(Some(false))
        evaluator.isShuffleServiceEnabled should be(Some(false))
        evaluator.serializerSeverity should be(Severity.NONE)
        evaluator.shuffleAndDynamicAllocationSeverity should be(Severity.MODERATE)
        evaluator.severity should be(Severity.MODERATE)
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
    val applicationAttempt = new ApplicationAttemptInfoImpl(Option("attempt1"),startDate, endDate, "sparkUser")
    val applicationAttempts = Seq(applicationAttempt)

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, name = "app", applicationAttempts),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = Seq.empty
    )

    SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
  }
}
