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

package org.apache.spark.deploy.history

import java.net.URL;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.{ObjectNode, JsonNodeFactory}

class DummySparkFSFetcher(val fetcherConfData: FetcherConfigurationData) extends SparkFSFetcher(fetcherConfData) {

  override def readJsonNode(url: URL): JsonNode = {

    //  Create json object of the form:
    //    {
    //      "beans" : [ {
    //      "name" : "Hadoop:service=NameNode,name=NameNodeStatus",
    //      "modelerType" : "org.apache.hadoop.hdfs.server.namenode.NameNode",
    //      "NNRole" : "NameNode",
    //      "HostAndPort" : "sample-sample01-ha2.grid.company.com:9000",
    //      "SecurityEnabled" : true,
    //      "State" : "active"
    //    } ]
    //    }
    val nodeFactory: JsonNodeFactory = JsonNodeFactory.instance;
    val node: ObjectNode = nodeFactory.objectNode();
    val child: ObjectNode = nodeFactory.objectNode();
    child.put("name", "Hadoop:service=NameNode, name=NameNodeStatus");
    child.put("modelerType", "org.apache.hadoop.hdfs.server.namenode.NameNode");
    child.put("NNRole", "NameNode");
    child.put("HostAndPort", "sample-sample01-ha2.grid.company.com:9000");
    child.put("SecurityEnabled", "true");

    val activeNameNodeUrls = Array(
      "http://sample-ha2.grid.company.com:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus",
      "http://sample-ha4.grid.company.com:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
    );
    // make ha2,ha4 active and other standby
    if(url.toString().equals(activeNameNodeUrls(0)) || url.toString.equals(activeNameNodeUrls(1))) {
      child.put("State","active")
    } else {
      child.put("State", "standby");
    }
    node.putArray("beans").add(child);
    return node;
  }

}
