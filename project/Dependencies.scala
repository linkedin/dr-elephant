//
// Copyright 2016 LinkedIn Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//

import play.Project._
import sbt._

object Dependencies {

  // Dependency Version
  lazy val commonsCodecVersion = "1.10"
  lazy val commonsIoVersion = "2.4"
  lazy val gsonVersion = "2.2.4"
  lazy val guavaVersion = "18.0"          // Hadoop defaultly are using guava 11.0, might raise NoSuchMethodException
  lazy val jacksonMapperAslVersion = "1.7.3"
  lazy val jsoupVersion = "1.7.3"
  lazy val mysqlConnectorVersion = "5.1.36"

  lazy val HADOOP_VERSION = "hadoopversion"
  lazy val SPARK_VERSION = "sparkversion"

  var hadoopVersion = "2.3.0"
  if (System.getProperties.getProperty(HADOOP_VERSION) != null) {
    hadoopVersion = System.getProperties.getProperty(HADOOP_VERSION)
  }

  var sparkVersion = "1.4.0"
  if (System.getProperties.getProperty(SPARK_VERSION) != null) {
    sparkVersion = System.getProperties.getProperty(SPARK_VERSION)
  }

  val sparkExclusion = if (sparkVersion >= "1.5.0") {
    "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(
      ExclusionRule(organization = "com.typesafe.akka"),
      ExclusionRule(organization = "org.apache.avro"),
      ExclusionRule(organization = "org.apache.hadoop"),
      ExclusionRule(organization = "net.razorvine")
    )
  } else {
    "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(
      ExclusionRule(organization = "org.apache.avro"),
      ExclusionRule(organization = "org.apache.hadoop"),
      ExclusionRule(organization = "net.razorvine")
    )
  }

  // Dependency coordinates
  var requiredDep = Seq(
    "com.google.code.gson" % "gson" % gsonVersion,
    "com.google.guava" % "guava" % guavaVersion,
    "commons-codec" % "commons-codec" % commonsCodecVersion,
    "commons-io" % "commons-io" % commonsIoVersion,
    "mysql" % "mysql-connector-java" % mysqlConnectorVersion,
    "org.apache.hadoop" % "hadoop-auth" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "compileonly",
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test,
    "org.codehaus.jackson" % "jackson-mapper-asl" % jacksonMapperAslVersion,
    "org.jsoup" % "jsoup" % jsoupVersion,
    "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
    "io.dropwizard.metrics" % "metrics-healthchecks" % "3.1.2",
    "org.mockito" % "mockito-core" % "1.10.19",
    "org.jmockit" % "jmockit" % "1.23" % Test
  ) :+ sparkExclusion 

  var dependencies = Seq(javaJdbc, javaEbean, cache)
  dependencies ++= requiredDep
}
