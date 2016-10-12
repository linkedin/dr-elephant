package com.linkedin.drelephant.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import java.io.{ ByteArrayInputStream, IOException }
import java.net.{ HttpURLConnection, URL }
import org.apache.log4j.Logger
import org.mockito.Mockito
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

class HadoopUtilsTest extends FunSpec with Matchers with MockitoSugar {
  import HadoopUtilsTest._

  describe("HadoopUtils") {
    describe(".isActiveNameNode") {
      it("returns true for active name nodes") {
        val hadoopUtils =
          newFakeHadoopUtilsForNameNode(Map(("nn1.grid.example.com", ("nn1-ha1.grid.example.com", "active"))))
        hadoopUtils.isActiveNameNode("nn1.grid.example.com") should be(true)
      }

      it("returns false for standby name nodes") {
        val hadoopUtils =
          newFakeHadoopUtilsForNameNode(Map(("nn1.grid.example.com", ("nn1-ha1.grid.example.com", "standby"))))
        hadoopUtils.isActiveNameNode("nn1.grid.example.com") should be(false)
      }
    }
  }
}

object HadoopUtilsTest extends MockitoSugar {
  import scala.annotation.varargs

  @varargs
  def newFakeHadoopUtilsForNameNode(nameNodeHostsAndStatesByJmxHost: (String, (String, String))*): HadoopUtils =
    newFakeHadoopUtilsForNameNode(nameNodeHostsAndStatesByJmxHost.toMap)

  def newFakeHadoopUtilsForNameNode(nameNodeHostsAndStatesByJmxHost: Map[String, (String, String)]): HadoopUtils =
    new HadoopUtils {
      override lazy val logger = mock[Logger]

      override def newAuthenticatedConnection(url: URL): HttpURLConnection = {
        val conn = mock[HttpURLConnection]
        val jmxHost = url.getHost
        nameNodeHostsAndStatesByJmxHost.get(jmxHost) match {
          case Some((host, state)) => {
            val jsonNode = newFakeNameNodeStatus(host, state)
            val bytes = jsonNode.toString.getBytes("UTF-8")
            Mockito.when(conn.getInputStream()).thenReturn(new ByteArrayInputStream(bytes))
          }
          case None => {
            Mockito.when(conn.getInputStream()).thenThrow(new IOException())
          }
        }
        conn
      }
    }

  def newFakeNameNodeStatus(host: String, state: String): JsonNode = {
    val jsonNodeFactory = JsonNodeFactory.instance;

    val beanJsonNode =
      jsonNodeFactory.objectNode()
        .put("name", "Hadoop:service=NameNode, name=NameNodeStatus")
        .put("modelerType", "org.apache.hadoop.hdfs.server.namenode.NameNode")
        .put("NNRole", "NameNode")
        .put("HostAndPort", "s${host}:9000")
        .put("SecurityEnabled", "true")
        .put("State", state)

    val beansJsonNode =
      jsonNodeFactory.arrayNode().add(beanJsonNode)

    jsonNodeFactory.objectNode().set("beans", beansJsonNode)
  }
}
