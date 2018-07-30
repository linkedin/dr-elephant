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

import java.util.Properties

import scala.collection.JavaConverters

import com.linkedin.drelephant.analysis.{ApplicationType, HadoopApplicationData}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationInfo, ExecutorSummary, JobData, StageData}


case class SparkApplicationData(
  appId: String,
  appConfigurationProperties: Map[String, String],
  applicationInfo: ApplicationInfo,
  jobDatas: Seq[JobData],
  stageDatas: Seq[StageData],
  executorSummaries: Seq[ExecutorSummary],
  stagesWithFailedTasks: Seq[StageData]
) extends HadoopApplicationData {
  import SparkApplicationData._
  import JavaConverters._

  override def getApplicationType(): ApplicationType = APPLICATION_TYPE

  override def getConf(): Properties = {
    val properties = new Properties()
    properties.putAll(appConfigurationProperties.asJava)
    properties
  }

  override def getAppId(): String = appId

  // This instance will always have data, or at least the data the Spark REST API gives us.
  override def isEmpty(): Boolean = false
}

object SparkApplicationData {
  val APPLICATION_TYPE = new ApplicationType("SPARK")

  def apply(
    appId: String,
    restDerivedData: SparkRestDerivedData,
    logDerivedData: Option[SparkLogDerivedData]
  ): SparkApplicationData = {
    val appConfigurationProperties: Map[String, String] =
      logDerivedData
        .flatMap { _.environmentUpdate.environmentDetails.get("Spark Properties").map(_.toMap) }
        .getOrElse(Map.empty)
    val applicationInfo = restDerivedData.applicationInfo
    val jobDatas = restDerivedData.jobDatas
    val stageDatas = restDerivedData.stageDatas
    val executorSummaries = restDerivedData.executorSummaries
    val stagesWithFailedTasks = restDerivedData.stagesWithFailedTasks
    apply(appId, appConfigurationProperties, applicationInfo, jobDatas, stageDatas, executorSummaries, stagesWithFailedTasks)
  }
}
