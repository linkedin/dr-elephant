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
import org.apache.hadoop.http.HttpConfig
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.apache.log4j.Logger

trait HadoopUtils {

  val HTTP_SCHEME = "http"
  val HTTPS_SCHEME = "https"
  protected def logger: Logger

  def findHaNameNodeAddress(conf: Configuration): Option[String] = {

    def findNameNodeAddressInNameServices(nameServices: Array[String]): Option[String] = nameServices match {
      case Array(nameService) => {
        val ids = Option(conf.get(s"${DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX}.${nameService}")).map { _.split(",") }
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
        .flatMap { id => Option(conf.get(getNameNodeHttpAddressKey(conf) + s".${nameService}.${id}")) }
        .find(isActiveNameNode)

    val nameServices = Option(conf.get(DFSConfigKeys.DFS_NAMESERVICES))map { _.split(",") }
    nameServices.flatMap(findNameNodeAddressInNameServices)
  }

  def isActiveNameNode(hostAndPort: String): Boolean = {
    // TO DO : fix this hardcoded sheme prefix
    val url = new URL(s"https://${hostAndPort}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
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

  def httpNameNodeAddress(conf: Configuration): Option[String] = Option(conf.get(getNameNodeHttpAddressKey(conf)))

  protected def isSslEnabled(conf: Configuration, key: String): Boolean = {
    (conf.get(key)!= null) && (HttpConfig.Policy.fromString(conf.get(key))== HttpConfig.Policy.HTTPS_ONLY)
  }

  def isHdfsSslEnabled(conf: Configuration): Boolean = {
    isSslEnabled(conf, DFSConfigKeys.DFS_HTTP_POLICY_KEY)
  }

  protected def getHttpPrefixScheme(conf: Configuration): String = {
    //HttpConfig.getScheme(HttpConfig.Policy.fromString(conf.get(key)))
    if (isHdfsSslEnabled(conf)) HTTPS_SCHEME else HTTP_SCHEME
  }

  protected def getNameNodeHttpAddressKey(conf: Configuration): String = {
    if (isHdfsSslEnabled(conf))
      DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY
    else
      DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY
  }

  protected def newAuthenticatedConnection(url: URL): HttpURLConnection = {
    val token = new AuthenticatedURL.Token()
    val authenticatedURL = new AuthenticatedURL()
    authenticatedURL.openConnection(url, token)
  }
}

object HadoopUtils extends HadoopUtils {
  override protected lazy val logger = Logger.getLogger(classOf[HadoopUtils])
}
