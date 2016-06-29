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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class EventExceptionToAvro {

  private static final Logger logger = Logger.getLogger(EventExceptionToAvro.class);


  public JSONObject convert(List<String> exception, int exceptionIndex) {
    JSONObject eventException = new JSONObject();
    JSONObject stackTraceFrame;
    String errorType = "unknown";
    String message = "empty";
    int frameIndex = 0;
    Pattern stackTraceLine = Pattern.compile("^[\\\\t \\t]*at (.+)\\.(.+(?=\\())\\((.*)\\)"); //To do
    Pattern exceptionDetails = Pattern.compile("^([^() :]*): (.*)"); //To do

    for (String line : exception) {
      Matcher match = stackTraceLine.matcher(line);
      if (match.find()) {

        stackTraceFrame = StackTraceFrameToAvro(frameIndex, match.group(1), match.group(2), match.group(3));
        try {
          eventException.accumulate("stackTrace", stackTraceFrame);
        } catch (JSONException e) {
          e.printStackTrace();
        }
        frameIndex += 1;
      } else {
        match = exceptionDetails.matcher(line);
        if (match.find()) {
          errorType = match.group(1);
          message = match.group(2);
        }
      }
    }
    try {
      eventException.put("index", exceptionIndex);
      eventException.put("message", message);
      eventException.put("type", errorType);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return eventException;
  }

  private JSONObject StackTraceFrameToAvro(int index, String source, String call, String fileDetails) {
    JSONObject stackTraceFrame = new JSONObject();
    boolean nativeMethod = false;
    String fileName = "";
    String lineNumber = "0";
    Pattern file = Pattern.compile("(.*):(.*)");

    if (fileDetails == "Native Method") {
      nativeMethod = true;
      fileName = fileDetails;
    } else if (fileDetails == "Unknown Source") {
      fileName = fileDetails;
    } else {
      Matcher match = file.matcher(fileDetails);
      if (match.find()) {
        fileName = match.group(1);
        lineNumber = match.group(2);
      }
    }
    try {
      stackTraceFrame.put("call", call);
      stackTraceFrame.put("columnNumber", 0);
      stackTraceFrame.put("fileName", fileName);
      stackTraceFrame.put("index", index);
      stackTraceFrame.put("lineNumber", Integer.parseInt(lineNumber));
      stackTraceFrame.put("nativeMethod", nativeMethod);
      stackTraceFrame.put("source", source);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return stackTraceFrame;
  }
}
