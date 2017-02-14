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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class represents an exception in the exception chain(a list of exceptions)
 */

public class EventException {
  private final Logger logger = Logger.getLogger(EventException.class);


  Pattern stackTraceLinePattern = Pattern.compile("^[\\\\t \\t]*at (.+)\\.(.+(?=\\())\\((.*)\\)");
  /**
    Example string: '\tat org.testng.Assert.fail(Assert.java:89)'
    matches: ['org.testng.Assert', 'fail', "Assert.java:89']
  */


  Pattern exceptionDetailsPattern = Pattern.compile("^([^() :]*): (.*)");
  /**
  Example string: 'java.lang.AssertionError: Failure 1 expected:<true> but was:<false>'
   matches: ['java.lang.AssertionError','Failure 1 expected:<true> but was:<false>']
  */


  Pattern separateLinesPattern = Pattern.compile(".*\\n");
  private String _type;
  private int _index;
  private String _message;
  private List<StackTraceFrame> _stackTrace;

  public EventException(int index, String rawEventException) {
    this._index = index;
    processRawString(rawEventException);
  }

  /**
   * Returns the message in EventException
   * @return message in event exception
   */
  public String getMessage() {
    return _message;
  }

  /**
   * Process a raw exception string and sets the field of EventException Object
   * @param rawEventException exception in a string form
   */
  private void processRawString(String rawEventException) {
    int frameIndex = 0;
    List<StackTraceFrame> stackTrace = new ArrayList<StackTraceFrame>();
    List<String> lines = stringToListOfLines(rawEventException);

    for (String line : lines) {
      Matcher exceptionDetailsMatcher = exceptionDetailsPattern.matcher(line);
      if (exceptionDetailsMatcher.find()) {
        this._type = exceptionDetailsMatcher.group(1);
        this._message = exceptionDetailsMatcher.group(2);
      } else {
        Matcher stackTraceLineMatcher = stackTraceLinePattern.matcher(line);
        if (stackTraceLineMatcher.find()) {
          String source = stackTraceLineMatcher.group(1);
          String call = stackTraceLineMatcher.group(2);
          String fileDetails = stackTraceLineMatcher.group(3);
          StackTraceFrame stackTraceFrame = new StackTraceFrame(frameIndex, source, call, fileDetails);
          stackTrace.add(stackTraceFrame);
          frameIndex += 1;
        }
      }
    }
    this._stackTrace = stackTrace;
  }

  /**
   * Takes a exception in string form and converts it into a list of string where each string corresponds to a line in
   * exception
   * @param rawEventException exception in a string form
   * @return list of lines in the exception
   */
  private List<String> stringToListOfLines(String rawEventException) {
    Matcher separateLinesMatcher = separateLinesPattern.matcher(rawEventException);
    List<String> lines = new ArrayList<String>();
    while (separateLinesMatcher.find()) {
      lines.add(separateLinesMatcher.group());
    }
    return lines;
  }
}
