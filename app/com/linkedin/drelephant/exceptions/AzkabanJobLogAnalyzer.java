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
* Given a Azkaban job log returns the Azkaban Job State, list of MR job ids and exception (if any) at the Azkaban job level
*/

public class AzkabanJobLogAnalyzer {
  private static final Logger logger = Logger.getLogger(AzkabanJobLogAnalyzer.class);
  Pattern _successfulAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status SUCCEEDED");
  Pattern _failedAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status FAILED");
  Pattern _killedAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status KILLED");
  Pattern _scriptFailPattern = Pattern.compile("ERROR - Job run failed!");
  Pattern _scriptFailExceptionPattern =
      Pattern.compile(".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*");
  Pattern _azkabanFailExceptionPattern = Pattern.compile(
      "\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} PDT [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
  Pattern _mrJobIdPattern = Pattern.compile("job_[0-9]+_[0-9]+");
  private AzkabanJobState _state;
  private LoggingEvent _exception;
  private Set<String> _subEvents;

  public AzkabanJobLogAnalyzer(String rawLog) {
    if (_successfulAzkabanJobPattern.matcher(rawLog).find()) {
      succeededAzkabanJob();
    } else if (_failedAzkabanJobPattern.matcher(rawLog).find()) {
      if (_scriptFailPattern.matcher(rawLog).find()) {
        scriptLevelFailedAzkabanJob(rawLog);
      } else {
        azkabanLevelFailedAzkabanJob(rawLog);
      }
    } else if (_killedAzkabanJobPattern.matcher(rawLog).find()) {
      killedAzkabanJob();
    }
    findSubEventIds(rawLog);
  }

  private void succeededAzkabanJob() {
    this._state = AzkabanJobState.SUCCEEDED;
    this._exception = null;
  }

  private void scriptLevelFailedAzkabanJob(String rawLog) {
    this._state = AzkabanJobState.SCRIPTFAIL;
    Matcher matcher = _scriptFailExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
  }

  private void azkabanLevelFailedAzkabanJob(String rawLog) {
    this._state = AzkabanJobState.AZKABANFAIL;
    Matcher matcher = _azkabanFailExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
  }

  private void killedAzkabanJob() {
    this._state = AzkabanJobState.KILLED;
    this._exception = null;
  }

  private void findSubEventIds(String rawLog) {
    Set<String> subEvents = new HashSet<String>();
    Matcher matcher = _mrJobIdPattern.matcher(rawLog);
    while (matcher.find()) {
      subEvents.add(matcher.group());
    }
    this._subEvents = subEvents;
  }

  public AzkabanJobState getState() {
    return this._state;
  }

  public Set<String> getSubEvents() {
    return this._subEvents;
  }

  public LoggingEvent getException() {
    return this._exception;
  }

  public enum AzkabanJobState {SCRIPTFAIL, AZKABANFAIL, MRFAIL, SUCCEEDED, KILLED}
}