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

import scala.collection.JavaConverters
import java.util.Properties

import com.linkedin.drelephant.analysis.{ApplicationType, HadoopApplicationData}


case class SparkComboApplicationData(
  appId: String,
  restDerivedData: SparkRestDerivedData,
  logDerivedData: Option[SparkLogDerivedData]
) extends HadoopApplicationData {
  import SparkComboApplicationData._
  import JavaConverters._

  override def getApplicationType(): ApplicationType = APPLICATION_TYPE

  override def getConf(): Properties = {
    val properties = new Properties()
    logDerivedData.map(_.appConfigurationProperties) match {
      case Some(appConfigurationProperties) => properties.putAll(appConfigurationProperties.asJava)
      case None => {} // Do nothing.
    }
    properties
  }

  override def getAppId(): String = appId

  // This instance will always have data, or at least the data the Spark REST API gives us.
  override def isEmpty(): Boolean = false
}

object SparkComboApplicationData {
  val APPLICATION_TYPE = new ApplicationType("SPARK")
}
