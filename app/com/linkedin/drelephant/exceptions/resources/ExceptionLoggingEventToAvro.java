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

/*package com.linkedin.drelephant.exceptions;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collection;
import java.util.List;

public class ExceptionLoggingEventToAvro {
  private final Logger logger = Logger.getLogger(ExceptionLoggingEventToAvro.class);
  private String CONTAINER = "hadooptest";
  private String ENVIRONMENT= "EI1";
  private int eventVersion = 9;
  private  String instance = "i001";
  private String service = "haddoptest";
  private String server = "hadooptest";
  private String serviceVersion = "0.0.0";
  Collection NULL = null;


  public JSONObject convert(List<List<String>> rawLog) {
    JSONObject loggingEvent = new JSONObject();;
    JSONObject eventHeader = new JSONObject();;
    JSONObject exceptionChain = new JSONObject();;
    JSONObject header = new JSONObject();;


    long millis = System.currentTimeMillis();
    int eventExceptionIndex = 0;

    EventExceptionToAvro eventExceptionToAvro = new EventExceptionToAvro();
    for (List<String> eventException : rawLog) {
      try {
        exceptionChain.append("array", eventExceptionToAvro.convert(eventException, eventExceptionIndex));
      } catch (JSONException e) {
        e.printStackTrace();
      }
      eventExceptionIndex += 1;
    }
    try {
      eventHeader.put("container", CONTAINER);
      eventHeader.put("environment", ENVIRONMENT);
      eventHeader.put("eventType", "log_event");
      eventHeader.put("eventVersion", eventVersion);
      eventHeader.put("guid", NULL);
      eventHeader.put("instance", instance);
      eventHeader.put("nano", millis*1000000);
      eventHeader.put("server", server);
      eventHeader.put("service", service);
      eventHeader.put("time", millis);
      eventHeader.put("version", serviceVersion);
      header.put("EventHeader", eventHeader);
      loggingEvent.put("exceptionChain", exceptionChain);
      loggingEvent.put("header", header);
      loggingEvent.put("level", "ERROR");
      loggingEvent.put("log", "");
      loggingEvent.put("logger", "");
      loggingEvent.put("loggingContext", NULL);
      loggingEvent.put("message", "");
      loggingEvent.put("requestId", NULL);
      loggingEvent.put("serviceCallContext", NULL);
      loggingEvent.put("thread", NULL);
      loggingEvent.put("timestamp", 0); // To do
      loggingEvent.put("treeId", NULL);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return loggingEvent;
  }
}
*/

