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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;


public class EventException {
  private final Logger logger = Logger.getLogger(EventException.class);
  Pattern stackTraceLine = Pattern.compile("^[\\\\t \\t]*at (.+)\\.(.+(?=\\())\\((.*)\\)"); //Test
  Pattern exceptionDetails = Pattern.compile("^([^() :]*): (.*)"); //Test
  private String _type;
  private int _index;
  private String _message;
  private List<StackTraceFrame> _stackTrace;

  public EventException(int index, String rawEventException) {
    this._index = index;
    processRawString(rawEventException);
  }

  public String getMessage() {
    return _message;
  }

  private void processRawString(String rawEventException) {
    List<StackTraceFrame> stackTrace = new ArrayList<StackTraceFrame>();
    int frameIndex = 0;
    Matcher matcher = Pattern.compile(".*\\n").matcher(rawEventException);
    List<String> lines = new ArrayList<String>();
    while (matcher.find()) {
      lines.add(matcher.group());
    }
    for (String line : lines) {
      matcher = exceptionDetails.matcher(line);
      if (matcher.find()) {
        this._type = matcher.group(1);
        this._message = matcher.group(2);
      } else {
        Matcher match = stackTraceLine.matcher(line);
        if (match.find()) {
          String source = match.group(1);
          String call = match.group(2);
          String fileDetails = match.group(3);
          StackTraceFrame stackTraceFrame = new StackTraceFrame(frameIndex, source, call, fileDetails);
          stackTrace.add(stackTraceFrame);
          frameIndex += 1;
        }
      }
    }
    this._stackTrace = stackTrace;
  }
}
