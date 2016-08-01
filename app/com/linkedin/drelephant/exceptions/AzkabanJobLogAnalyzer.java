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
* Given a Azkaban job log returns the Azkaban Job State, list of all MR job ids in the given log and exception (if any) at the Azkaban job level
*/

public class AzkabanJobLogAnalyzer {

  private static final Logger logger = Logger.getLogger(AzkabanJobLogAnalyzer.class);
  private Pattern _successfulAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status SUCCEEDED");
  private Pattern _failedAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status FAILED");
  private Pattern _killedAzkabanJobPattern =
      Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status KILLED");
  private Pattern _scriptFailPattern = Pattern.compile("ERROR - Job run failed!");
  private Pattern _scriptOrMRFailExceptionPattern =
      Pattern.compile(".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*");
  private Pattern _azkabanFailExceptionPattern = Pattern.compile(
      "\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} PDT [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
  private Pattern _mrJobIdPattern = Pattern.compile("job_[0-9]+_[0-9]+");
  /**
   * Failure at Azkaban job log is broadly categorized into three categorized into three categories
   * AZKABANFAIL: Failure at azkaban level
   * SCRIPTFAIL: Failure at script level
   * MRFAIL: Failure at mapreduce level
   * */
  private AzkabanJobState _state;
  private LoggingEvent _exception;
  private Set<String> _subEvents;
  public AzkabanJobLogAnalyzer(String rawLog) {
    setSubEvents(rawLog);
    // To do: Instead of using regex to find the state, use Azkaban Rest API
    if (_successfulAzkabanJobPattern.matcher(rawLog).find()) {
      succeededAzkabanJob();
    } else if (_failedAzkabanJobPattern.matcher(rawLog).find()) {
      if (!_subEvents.isEmpty()) {
        mrLevelFailedAzkabanJob(rawLog);
      } else if (_scriptFailPattern.matcher(rawLog).find()) {
        scriptLevelFailedAzkabanJob(rawLog);
      } else {
        azkabanLevelFailedAzkabanJob(rawLog);
      }
    } else if (_killedAzkabanJobPattern.matcher(rawLog).find()) {
      killedAzkabanJob();
    }
  }

  /**
   * Sets the _state and _exception for Succeeded Azkaban job
   */
  private void succeededAzkabanJob() {
    this._state = AzkabanJobState.SUCCEEDED;
    this._exception = null;
  }

  /**
   * Sets _state and _exception for Azkaban job which failed at the MR Level
   * @param rawLog Raw Azkaban Job Log
   */
  private void mrLevelFailedAzkabanJob(String rawLog) {
    this._state = AzkabanJobState.MRFAIL;
    Matcher matcher = _scriptOrMRFailExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
      // fetching stacktrace form azkaban job log only for the case if mr logs are not present in job history server
      // so in that this stacktrace can be presented to the user
    }
  }

  /**
   * Set _state and _exception for Azkaban job which failed at the Script Level
   * @param rawLog Raw Azkaban Job log
   */
  private void scriptLevelFailedAzkabanJob(String rawLog) {
    this._state = AzkabanJobState.SCRIPTFAIL;
    Matcher matcher = _scriptOrMRFailExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
  }

  /**
   * Set _state and _exception for Azkaban job which failed at the Azkaban Level
   * @param rawLog Raw Azkaban job log
   */
  private void azkabanLevelFailedAzkabanJob(String rawLog) {
    this._state = AzkabanJobState.AZKABANFAIL;
    Matcher matcher = _azkabanFailExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
  }

  /**
   * Set _state and _exception for killed Azkaban job
   */
  private void killedAzkabanJob() {
    this._state = AzkabanJobState.KILLED;
    this._exception = null;
  }

  /**
   * @return returns Azkaban job state
   */
  public AzkabanJobState getState() {
    return this._state;
  }

  /**
   * @return returns list of MR Job Ids in the given Azkaban job log
   */
  public Set<String> getSubEvents() {
    return this._subEvents;
  }

  /**
   * Sets _subEvents equal to the list of mr job ids in the given Azkaban job log
   * @param rawLog Raw Azkaban job log
   */
  private void setSubEvents(String rawLog) {
    Set<String> subEvents = new HashSet<String>();
    Matcher matcher = _mrJobIdPattern.matcher(rawLog);
    while (matcher.find()) {
      subEvents.add(matcher.group());
    }
    this._subEvents = subEvents;
  }

  /**
   * @return returns _exception
   */
  public LoggingEvent getException() {
    return this._exception;
  }

  public enum AzkabanJobState {SCRIPTFAIL, AZKABANFAIL, MRFAIL, SUCCEEDED, KILLED}
}