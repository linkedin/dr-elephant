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


public class LoggingEvent {

  private final Logger logger = Logger.getLogger(LoggingEvent.class);
  private List<String> _rawLog;
  private String _log;
  private long _timestamp;
  private LoggingLevel _level = LoggingLevel.ERROR;
  private String _message;
  private List<EventException> _exceptionChain;
  public LoggingEvent(String exceptionChainString) {
    this._rawLog = exceptionChainStringToListOfExceptions(exceptionChainString);
    setExceptionChain();
    setMessage();
  }

  public List<List<String>> getLog() {
    List<List<String>> log = new ArrayList<List<String>>();
    for (String exceptionString : _rawLog) {
      List<String> exception = exceptionStringToListOfLines(exceptionString);
      log.add(exception);
    }
    return log;
  }

  private void setExceptionChain() {
    List<EventException> exceptionChain = new ArrayList<EventException>();
    int index = 0;

    for (String rawEventException : _rawLog) {
      EventException eventException = new EventException(index, rawEventException);
      exceptionChain.add(eventException);
      index += 1;
    }
    _exceptionChain = exceptionChain;
  }

  private List<String> exceptionChainStringToListOfExceptions(String s) {
    List<String> chain = new ArrayList<String>();
    Pattern stackTraceCausedByClause = Pattern.compile(".*^(?!Caused by).+\\n(?:.*\\tat.+\\n)+");
    Pattern stackTraceOtherThanCausedByClause = Pattern.compile(".*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*");

    Matcher matcher = stackTraceCausedByClause.matcher(s);
    while (matcher.find()) {
      chain.add(matcher.group());
    }
    matcher = stackTraceOtherThanCausedByClause.matcher(s);
    while (matcher.find()) {
      chain.add(matcher.group());
    }

    if (chain.isEmpty()) { //logs other than stack traces for ex- log of azkaban level failure in azkaban job
      chain.add(s);
    }
    return chain;
  }

  private List<String> exceptionStringToListOfLines(String s) {
    List<String> exception = new ArrayList<String>();
    Matcher matcher = Pattern.compile(".*\\n").matcher(s);
    while (matcher.find()) {
      exception.add(matcher.group());
    }
    return exception;
  }

  private void setMessage() {
    if (!_exceptionChain.isEmpty()) {
      this._message = _exceptionChain.get(0).getMessage();
    }
  }

  private enum LoggingLevel {DEBUG, INFO, WARNING, ERROR, FATAL}
}
