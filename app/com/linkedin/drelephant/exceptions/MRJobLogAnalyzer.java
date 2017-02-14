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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
* Given a MR Job log, sets the list of unsuccessful tasks and MR job level exception (if any)
*/

public class MRJobLogAnalyzer {
  private static final Logger logger = Logger.getLogger(MRJobLogAnalyzer.class);

  private Pattern _mrJobExceptionPattern =
      Pattern.compile(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*");
  private Pattern _unsuccessfulMRTaskIdPattern =
      Pattern.compile("Task (?:failed) (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
  private LoggingEvent _exception;
  private Set<String> _failedSubEvents;

  public MRJobLogAnalyzer(String rawLog) {
    setFailedSubEvents(rawLog);
    setException(rawLog);
  }

  /**
   * Given MR Job log, finds the list of unsuccessful tasks and sets it equal to _failedSubEvents
   * @param rawLog MR Job log in a string
   */
  private void setFailedSubEvents(String rawLog) {
    Set<String> failedSubEvents = new HashSet<String>();
    Matcher unsuccessfulMRTaskIdMatcher = _unsuccessfulMRTaskIdPattern.matcher(rawLog);
    while (unsuccessfulMRTaskIdMatcher.find()) {
      failedSubEvents.add(unsuccessfulMRTaskIdMatcher.group(1));
    }
    this._failedSubEvents = failedSubEvents;
  }

  /**
   * Given MR Job log, finds the MR Job level exception and sets it equal to _exception
   * @param rawLog MR Job log in a string
   */
  private void setException(String rawLog) {
    Matcher mrJobExceptionMatcher = _mrJobExceptionPattern.matcher(rawLog);
    if (mrJobExceptionMatcher.find()) {
      this._exception = new LoggingEvent(mrJobExceptionMatcher.group());
    }
  }

  /**
   * Returns the list of unsuccessful tasks in given MR Job log
   * @return list of unsuccessful tasks in MR Job log
   */
  public Set<String> getFailedSubEvents() {
    return this._failedSubEvents;
  }

  /**
   * Returns the MR Job level exception
   * @return _exception of type LoggingEvent.
   */
  public LoggingEvent getException() {
    return this._exception;
  }


}