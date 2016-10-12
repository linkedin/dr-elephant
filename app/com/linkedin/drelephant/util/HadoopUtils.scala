package com.linkedin.drelephant.util

import java.io.InputStream
import java.net.{HttpURLConnection, URL}

import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.apache.log4j.Logger

trait HadoopUtils {
  val DFS_NAMESERVICES_KEY = "dfs.nameservices"
  val DFS_HA_NAMENODES_KEY = "dfs.ha.namenodes"
  val DFS_NAMENODE_HTTP_ADDRESS_KEY = "dfs.namenode.http-address"

  def logger: Logger

  def haNamenodeAddress(conf: Configuration): Option[String] = {
    Option(conf.get(DFS_NAMESERVICES_KEY)).flatMap {
      _.split(",") match {
        case Array(nameService) => {
          val namenodeAddress =
            Option(conf.get(s"${DFS_HA_NAMENODES_KEY}.${nameService}")).map {
              _.split(",").flatMap { namenode => Option(conf.get(s"${DFS_NAMENODE_HTTP_ADDRESS_KEY}.${nameService}.${namenode}")) }
            }
              .flatMap { _.find(isActiveNamenode) }
          if (namenodeAddress.isDefined) {
            logger.info(s"Active namenode for ${nameService}: ${namenodeAddress}")
          } else {
            logger.info(s"No active namenode for ${nameService}.")
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
    }
  }

  def isActiveNamenode(hostAndPort: String): Boolean = {
    val url = new URL(s"http://${hostAndPort}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    val conn = newAuthenticatedConnection(url)
    try {
      val in = conn.getInputStream()
      try {
        isActiveNamenode(in)
      } finally {
        in.close()
      }
    } finally {
      conn.disconnect()
    }
  }

  def isActiveNamenode(in: InputStream): Boolean =
    new ObjectMapper().readTree(in).path("beans").get(0).path("State").textValue() == "active"

  def newAuthenticatedConnection(url: URL): HttpURLConnection = {
    val token = new AuthenticatedURL.Token()
    val authenticatedURL = new AuthenticatedURL()
    authenticatedURL.openConnection(url, token)
  }
}

object HadoopUtils extends HadoopUtils {
  override lazy val logger = Logger.getLogger(classOf[HadoopUtils])
}
