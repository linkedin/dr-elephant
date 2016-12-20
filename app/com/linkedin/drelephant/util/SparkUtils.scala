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

package com.linkedin.drelephant.util

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import scala.collection.JavaConverters


trait SparkUtils {
  import JavaConverters._

  def defaultEnv: Map[String, String]

  // Below this line are modified utility methods from
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/Utils.scala

  /** Return the path of the default Spark properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = defaultEnv): Option[String] = {
    env.get("SPARK_CONF_DIR")
      .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } finally {
      inReader.close()
    }
  }
}

object SparkUtils extends SparkUtils {
  override val defaultEnv = sys.env
}
