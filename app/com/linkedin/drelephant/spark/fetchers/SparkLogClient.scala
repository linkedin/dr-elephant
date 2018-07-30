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

package com.linkedin.drelephant.spark.fetchers

import java.io.InputStream
import java.security.PrivilegedAction

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

import com.linkedin.drelephant.security.HadoopSecurity
import com.linkedin.drelephant.spark.data.SparkLogDerivedData
import com.linkedin.drelephant.util.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.jackson.JsonMethods


/**
  * A client for getting data from the Spark event logs.
  */
class SparkLogClient(hadoopConfiguration: Configuration, sparkConf: SparkConf, eventLogUri: Option[String]) {
  import SparkLogClient._

  private val logger: Logger = Logger.getLogger(classOf[SparkLogClient])

  private lazy val security: HadoopSecurity = HadoopSecurity.getInstance()

  protected lazy val sparkUtils: SparkUtils = SparkUtils

  def fetchData(appId: String, attemptId: Option[String])(implicit ec: ExecutionContext): Future[SparkLogDerivedData] =
    doAsPrivilegedAction { () => doFetchData(appId, attemptId) }

  protected def doAsPrivilegedAction[T](action: () => T): T =
    security.doAs[T](new PrivilegedAction[T] { override def run(): T = action() })

  protected def doFetchData(
    appId: String,
    attemptId: Option[String]
  )(
    implicit ec: ExecutionContext
  ): Future[SparkLogDerivedData] = {
    val (eventLogFileSystem, baseEventLogPath) =
      sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, eventLogUri)
    val (eventLogPath, eventLogCodec) =
      sparkUtils.pathAndCodecforEventLog(sparkConf, eventLogFileSystem, baseEventLogPath, appId, attemptId)

    Future { 
        sparkUtils.withEventLog(eventLogFileSystem, eventLogPath, eventLogCodec)(findDerivedData(_))
    }
  }
}

object SparkLogClient {
  import JsonAST._

  private implicit val formats: DefaultFormats = DefaultFormats

  def findDerivedData(in: InputStream, eventsLimit: Option[Int] = None): SparkLogDerivedData = {
    val events = eventsLimit.map { getEvents(in).take(_) }.getOrElse { getEvents(in) }

    var environmentUpdate: Option[SparkListenerEnvironmentUpdate] = None
    while (events.hasNext && environmentUpdate.isEmpty) {
      val event = events.next
      event match {
        case Some(eu: SparkListenerEnvironmentUpdate) => environmentUpdate = Some(eu)
        case _ => {} // Do nothing.
      }
    }

    environmentUpdate
      .map(SparkLogDerivedData(_))
      .getOrElse { throw new IllegalArgumentException("Spark event log doesn't have Spark properties") }
  }

  private def getEvents(in: InputStream): Iterator[Option[SparkListenerEvent]] = getLines(in).map(lineToEvent)

  private def getLines(in: InputStream): Iterator[String] = Source.fromInputStream(in).getLines

  private def lineToEvent(line: String): Option[SparkListenerEvent] = sparkEventFromJson(JsonMethods.parse(line))

  // Below this line are modified utility methods from:
  //
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/io/CompressionCodec.scala
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala
  // https://github.com/apache/spark/blob/v1.4.1/core/src/main/scala/org/apache/spark/util/Utils.scala

  private def sparkEventFromJson(json: JValue): Option[SparkListenerEvent] = {
    val environmentUpdate = getFormattedClassName(SparkListenerEnvironmentUpdate)

    (json \ "Event").extract[String] match {
      case `environmentUpdate` => Some(environmentUpdateFromJson(json))
      case _ => None
    }
  }

  private def getFormattedClassName(obj: AnyRef): String = obj.getClass.getSimpleName.replace("$", "")

  private def environmentUpdateFromJson(json: JValue): SparkListenerEnvironmentUpdate = {
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  private def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.map { case JField(k, JString(v)) => (k, v) }.toMap
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }
}
