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

package com.linkedin.drelephant.exceptions;

import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class EventExceptionTest {
  private static final Logger logger = Logger.getLogger(EventExceptionTest.class);

  @Test
  public void getMessageTest() {

    String rawEventException =
        "java.io.FileNotFoundException: Path is not a file: /data/sample/Sample/Sample/1466675602538-PT-472724050\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:70)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:56)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocationsUpdateTimes(FSNamesystem.java:1914)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocationsInt(FSNamesystem.java:1855)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(FSNamesystem.java:1835)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(FSNamesystem.java:1807)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.getBlockLocations(NameNodeRpcServer.java:552)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.getBlockLocations(ClientNamenodeProtocolServerSideTranslatorPB.java:362)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:619)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.RPC$Server.call(RPC.java:962)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2044)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2040)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat java.security.AccessController.doPrivileged(Native Method)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat javax.security.auth.Subject.doAs(Subject.java:422)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1671)\n"
            + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler.run(Server.java:2038)";

    EventException eventException = new EventException(0, rawEventException);
    assertTrue("getMessageTest failed", eventException.getMessage()
        .equals("Path is not a file: /data/sample/Sample/Sample/1466675602538-PT-472724050"));
    logger.info("correct message" + eventException.getMessage());
  }
}