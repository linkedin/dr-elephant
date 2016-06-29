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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collection;
import java.util.List;

public class ExceptionLoggingEventToAvro {
  private final Logger logger = Logger.getLogger(ExceptionLoggingEventToAvro.class);

  public JSONObject convert(ExceptionLoggingEvent exceptionLoggingEvent) {
    JSONObject loggingEvent = new JSONObject();;
    JSONObject eventHeader = new JSONObject();;
    JSONObject exceptionChain = new JSONObject();;
    JSONObject header = new JSONObject();;
    int eventVersion = 9;
    Collection Null = null;
    long millis = System.currentTimeMillis();
    int eventExceptionIndex = 0;

    EventExceptionToAvro eventExceptionToAvro = new EventExceptionToAvro();
    for (List<String> eventException : exceptionLoggingEvent.getExceptionChain()) {
      try {
        exceptionChain.append("array", eventExceptionToAvro.convert(eventException, eventExceptionIndex));
      } catch (JSONException e) {
        e.printStackTrace();
      }
      eventExceptionIndex += 1;
    }
    try {
      eventHeader.put("container", "hadooptest");
      eventHeader.put("environment", "EI1");
      eventHeader.put("eventType", "log_event");
      eventHeader.put("eventVersion", eventVersion);
      eventHeader.put("guid", "aragrawaaragrawa");
      eventHeader.put("instance", "i001");
      eventHeader.put("nano", "");
      eventHeader.put("server", "hadooptest");
      eventHeader.put("service", "hadooptest");
      eventHeader.put("time", millis);
      eventHeader.put("version", "0.0.0");
      header.put("EventHeader", eventHeader);
      loggingEvent.put("exceptionChain", exceptionChain);
      loggingEvent.put("header", header);
      loggingEvent.put("level", "ERROR");
      loggingEvent.put("log", "");
      loggingEvent.put("logger", "");
      loggingEvent.put("loggingContext", "");
      loggingEvent.put("message", "");
      loggingEvent.put("requestId", "");
      loggingEvent.put("serviceCallContext", "");
      loggingEvent.put("thread", Null);
      loggingEvent.put("timestamp", 0); // To do
      loggingEvent.put("treeId", Null);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return loggingEvent;
  }
}

