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

package com.linkedin.drelephant.spark.data

import java.util.Date

import scala.collection.JavaConverters

import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfoImpl, ApplicationInfoImpl}
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.scalatest.{FunSpec, Matchers}

class SparkApplicationDataTest extends FunSpec with Matchers {
  import SparkApplicationDataTest._
  import JavaConverters._

  describe("SparkApplicationData") {
    val appId = "application_1"
    val attemptId = Some("1")

    val applicationAttemptInfo = {
      val now = System.currentTimeMillis
      val duration = 8000000L
      newFakeApplicationAttemptInfo(attemptId, startTime = new Date(now - duration), endTime = new Date(now))
    }

    val restDerivedData = SparkRestDerivedData(
      new ApplicationInfoImpl(appId, "app", Seq(applicationAttemptInfo)),
      jobDatas = Seq.empty,
      stageDatas = Seq.empty,
      executorSummaries = Seq.empty,
      stagesWithFailedTasks = Seq.empty
    )

    val configurationProperties = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.storage.memoryFraction" -> "0.3",
      "spark.driver.memory" -> "2G",
      "spark.executor.instances" -> "900",
      "spark.executor.memory" -> "1g",
      "spark.shuffle.memoryFraction" -> "0.5"
    )

    val logDerivedData = SparkLogDerivedData(
      SparkListenerEnvironmentUpdate(Map("Spark Properties" -> configurationProperties.toSeq))
    )

    describe(".getConf") {
      it("returns the Spark properties") {
        val data = SparkApplicationData(appId, restDerivedData, Some(logDerivedData))
        data.getConf.asScala should contain theSameElementsAs(configurationProperties)
      }
    }
  }
}

object SparkApplicationDataTest {
  def newFakeApplicationAttemptInfo(
    attemptId: Option[String],
    startTime: Date,
    endTime: Date
  ): ApplicationAttemptInfoImpl = new ApplicationAttemptInfoImpl(
    attemptId,
    startTime,
    endTime,
    sparkUser = "foo",
    completed = true
  )
}
