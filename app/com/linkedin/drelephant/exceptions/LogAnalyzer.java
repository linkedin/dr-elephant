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

/*
Given a log string, analyzes and returns the following information:
state:
exception:

 */

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogAnalyzer {
  private static final Logger logger = Logger.getLogger(LogAnalyzer.class);
  //private enum STATES {SUCCEEDED, FAILED, KILLED, SCRIPTFAIL, AZKABANFAIL, UNKNOWN};
  private String _state;
  private ExceptionLoggingEvent _exception;
  private Set<String> _failedSubEvents; /* for azkaban flow log: returns the set of failed azkaban job ids
                                                for azkaban job log: returns the set of failed MR jobs ids
                                                for mr job log: returns the set of failed mr task ids*/
  private String _rawLog;

  public LogAnalyzer(String rawLog) {
    this._state = "UNKNOWN";
    this._exception = new ExceptionLoggingEvent();
    this._failedSubEvents = new HashSet<String>();
    this._rawLog = rawLog;
    analyzeRawLog();
  }

  public void analyzeRawLog(){
    Matcher matcher;
    Set<String> allFailedSubEventMatchs = new HashSet<String>();
    if (match(
        "Flow \\'\\' is set to SUCCEEDED in [0-9]+ seconds").find()) {
      //Successful Azkaban flow log
      this._state = "SUCCEEDED";
    } else if (match(
        "Setting flow \\'\\' status to FAILED in [0-9]+ seconds").find()) {
      //Failed Azkaban flow log
      matcher = match("Job (.*) finished with status (?:FAILED|KILLED) in [0-9]+ seconds");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group(1));
      }
      this._state = "FAILED";
      this._failedSubEvents = allFailedSubEventMatchs;
    } else if (match(
        "Setting flow \\'\\' status to KILLED in [0-9]+ seconds").find()) {
      //Killed Azkaban flow log
      matcher = match("Job (.*) finished with status (?:FAILED|KILLED) in [0-9]+ seconds");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group(1));
      }
      this._state = "KILLED";
      this._failedSubEvents = allFailedSubEventMatchs;
    } else if (match(
        "Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status SUCCEEDED").find()) {
      //Succeeded Azkaban Job log
      this._state = "SUCCEEDED";
    } else if (match(
        "Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status FAILED").find()) {
      //Failed Azkaban Job log
      matcher = match("job_[0-9]+_[0-9]+");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group());
      }
      if (match("ERROR - Job run failed!").find()) {
        this._state = "SCRIPT FAILED";
        this._exception.setType("script");
        this._failedSubEvents = allFailedSubEventMatchs;
        this._exception.setExceptionChain(
            matchExceptionChain(".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*"));
      } else {
        this._state = "Azkaban Fail";
        this._exception.setType("azkaban");
        matcher = match("\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} PDT [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
        if (matcher.find()) {
          this._exception.addEventException(stringToExceptionEvent(matcher.group()));
        }
      }
    } else if (match(
        "Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status KILLED").find()) {
      // Killed Azkaban Job log
      matcher = match("job_[0-9]+_[0-9]+");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group());
      }
      this._state = "KILLED";
      this._failedSubEvents = allFailedSubEventMatchs;
      //**Incomplete**

    } else if (match(
        "Job failed as tasks failed").find()) {
      // Failed MR Job log
      this._state = "FAILED";
      matcher = match("Task failed (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group(1));
      }
      this._failedSubEvents = allFailedSubEventMatchs;
      this._exception.setExceptionChain(
          matchExceptionChain(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*"));
    } else if (match(
        "Job failed as tasks killed").find()) {
      // Killed MR Job log
      this._state = "KILLED";
      matcher = match("Task killed (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
      while (matcher.find()) {
        allFailedSubEventMatchs.add(matcher.group(1));
      }
      this._failedSubEvents = allFailedSubEventMatchs;
      this._exception.setExceptionChain(
          matchExceptionChain(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*"));
    } else {
      // Failed MR Task Task log
      this._state = "FAILED";
      this._exception.setExceptionChain(
          matchExceptionChain("Error: (.*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*)"));
    }
  }

  private List<List<String>> matchExceptionChain(String pattern) {
    Matcher matcher = Pattern.compile(pattern).matcher(_rawLog);
    List<List<String>> exceptionChain = new ArrayList<List<String>>();
    if (matcher.find()) {

      for (String exceptionString : stringToExceptionChain(matcher.group())) {
        exceptionChain.add(stringToExceptionEvent(exceptionString));
      }
      return exceptionChain;
    }
    return null;
  }

  public Matcher match(String pattern) {
    return Pattern.compile(pattern).matcher(_rawLog);
  }

  public List<String> stringToExceptionChain(String s) {
    List<String> chain = new ArrayList<String>();
    Matcher matcher = Pattern.compile(".*^(?!Caused by).+\\n(?:.*\\tat.+\\n)+").matcher(s);
    while (matcher.find()) {
      chain.add(matcher.group());
    }
    matcher = Pattern.compile(".*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*").matcher(s);
    while (matcher.find()) {
      chain.add(matcher.group());
    }
    return chain;
  }

  public List<String> stringToExceptionEvent(String s) {
    List<String> exceptionEvent = new ArrayList<String>();
    Matcher matcher = Pattern.compile(".*\\n").matcher(s);
    while (matcher.find()) {
      exceptionEvent.add(matcher.group());
    }
    return exceptionEvent;
  }

  public String getState() {
    return this._state;
  }

  public Set<String> getFailedSubEvents() {
    return this._failedSubEvents;
  }

  public ExceptionLoggingEvent getException() {
    return this._exception;
  }
}