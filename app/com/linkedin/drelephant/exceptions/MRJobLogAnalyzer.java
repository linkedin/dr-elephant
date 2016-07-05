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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;


/*
* Given a MR Job log returns the list of unsuccessful tasks and MR job level exception (if any)
*
* */

public class MRJobLogAnalyzer {
  private static final Logger logger = Logger.getLogger(MRJobLogAnalyzer.class);
  Pattern _mrJobExceptionPattern =
      Pattern.compile(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*");
  Pattern _unsuccessfulMRTaskIdPattern = Pattern.compile("Task (?:failed|killed) (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
  private LoggingEvent _exception;
  private Set<String> _failedSubEvents;
      // to do test

  public MRJobLogAnalyzer(String rawLog) {
    findFailedSubEvents(rawLog);
    findException(rawLog);
  }

  private void findFailedSubEvents(String rawLog) {
    Set<String> failedSubEvents = new HashSet<String>();
    Matcher unsuccessfulMRTaskIdMatcher = _unsuccessfulMRTaskIdPattern.matcher(rawLog);
    while (unsuccessfulMRTaskIdMatcher.find()) {
      failedSubEvents.add(unsuccessfulMRTaskIdMatcher.group(1));
    }
    this._failedSubEvents = failedSubEvents;
  }

  private void findException(String rawLog) {
    Matcher mrJobExceptionMatcher = _mrJobExceptionPattern.matcher(rawLog);
    if (mrJobExceptionMatcher.find()) {
      this._exception = new LoggingEvent(mrJobExceptionMatcher.group());
    }
  }

  public Set<String> getFailedSubEvents() {
    return this._failedSubEvents;
  }

  public LoggingEvent getException() {
    return this._exception;
  }
}