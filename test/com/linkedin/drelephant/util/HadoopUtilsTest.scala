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

  describe("HadoopUtils") {
    describe(".isActiveNameNode") {
      it("returns true for active name nodes") {
        val hadoopUtils = newFakeHadoopUtilsForNameNode("nn1.example.com", "active")
        hadoopUtils.isActiveNamenode("nn1.example.com") should be(true)
      }

      it("returns false for standby name nodes") {
        val hadoopUtils = newFakeHadoopUtilsForNameNode("nn1.example.com", "standby")
        hadoopUtils.isActiveNamenode("nn1.example.com") should be(false)
      }
    }
  }

  def newFakeHadoopUtilsForNameNode(host: String, state: String): HadoopUtils = new HadoopUtils {
    override lazy val logger = mock[Logger]

    override def newAuthenticatedConnection(url: URL): HttpURLConnection = {
      val conn = mock[HttpURLConnection]
      if (url.getHost == host) {
        val jsonNode = newFakeNameNodeStatus(host, state)
        val bytes = jsonNode.toString.getBytes("UTF-8")
        Mockito.when(conn.getInputStream()).thenReturn(new ByteArrayInputStream(bytes))
      } else {
        Mockito.when(conn.getInputStream()).thenThrow(new IOException())
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
