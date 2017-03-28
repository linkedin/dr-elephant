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

import java.io.{ByteArrayInputStream, IOException}
import java.net.{HttpURLConnection, URL}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.mockito.Mockito
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

class HadoopUtilsTest extends FunSpec with Matchers {
  import HadoopUtilsTest._

  describe("HadoopUtils") {
    describe(".findHaNameNodeAddress") {
      it("returns the first active HA name node it can find") {
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
        )
        val conf = new Configuration(false)
        conf.addResource("core-site.xml")
        val haNameNodeAddress = hadoopUtils.findHaNameNodeAddress(conf)
        haNameNodeAddress should be(Some("sample-ha2.grid.example.com:50070"))
      }

      it("returns no HA name node if one isn't configured") {
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
        )
        val conf = new Configuration(false)
        val haNameNodeAddress = hadoopUtils.findHaNameNodeAddress(conf)
        haNameNodeAddress should be(None)
      }
    }

    describe(".httpNameNodeAddress") {
      it("returns the default name node") {
        val hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
          ("sample-ha1.grid.example.com", ("sample-ha1.grid.example.com", "standby")),
          ("sample-ha2.grid.example.com", ("sample-ha2.grid.example.com", "active"))
        )
        val conf = new Configuration(false)
        conf.addResource("core-site.xml")
        val haNameNodeAddress = hadoopUtils.httpNameNodeAddress(conf)
        haNameNodeAddress should be(Some("sample.grid.example.com:50070"))
      }
    }

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
