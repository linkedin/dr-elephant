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

import java.io.InputStream
import java.net.{HttpURLConnection, URL}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.apache.log4j.Logger

trait HadoopUtils {
  val DFS_NAMESERVICES_KEY = "dfs.nameservices"
  val DFS_HA_NAMENODES_KEY = "dfs.ha.namenodes"
  val DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address"

  protected def logger: Logger

  def findHaNameNodeAddress(conf: Configuration): Option[String] = {

    def findNameNodeAddressInNameServices(nameServices: Array[String]): Option[String] = nameServices match {
      case Array(nameService) => {
        val ids = Option(conf.get(s"${DFS_HA_NAMENODES_KEY}.${nameService}")).map { _.split(",") }
        val namenodeAddress = ids.flatMap { findNameNodeAddressInNameService(nameService, _) }
        namenodeAddress match {
          case Some(address) => logger.info(s"Active namenode for ${nameService}: ${address}")
          case None => logger.info(s"No active namenode for ${nameService}.")
        }
        namenodeAddress
      }
      case Array() => {
        logger.info("No name services found.")
        None
      }
      case _ => {
        logger.info("Multiple name services found. HDFS federation is not supported right now.")
        None
      }
    }

    def findNameNodeAddressInNameService(nameService: String, nameNodeIds: Array[String]): Option[String] =
      nameNodeIds
        .flatMap { id => Option(conf.get(s"${DFS_NAMENODE_HTTP_ADDRESS_KEY}.${nameService}.${id}")) }
        .find(isActiveNameNode)

    val nameServices = Option(conf.get(DFS_NAMESERVICES_KEY)).map { _.split(",") }
    nameServices.flatMap(findNameNodeAddressInNameServices)
  }

  def httpNameNodeAddress(conf: Configuration): Option[String] = Option(conf.get(DFS_NAMENODE_HTTP_ADDRESS_KEY))

  def isActiveNameNode(hostAndPort: String): Boolean = {
    val url = new URL(s"http://${hostAndPort}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    val conn = newAuthenticatedConnection(url)
    try {
      val in = conn.getInputStream()
      try {
        isActiveNameNode(in)
      } finally {
        in.close()
      }
    } finally {
      conn.disconnect()
    }
  }

  protected def isActiveNameNode(in: InputStream): Boolean =
    new ObjectMapper().readTree(in).path("beans").get(0).path("State").textValue() == "active"

  protected def newAuthenticatedConnection(url: URL): HttpURLConnection = {
    val token = new AuthenticatedURL.Token()
    val authenticatedURL = new AuthenticatedURL()
    authenticatedURL.openConnection(url, token)
  }
}

object HadoopUtils extends HadoopUtils {
  override protected lazy val logger = Logger.getLogger(classOf[HadoopUtils])
}
