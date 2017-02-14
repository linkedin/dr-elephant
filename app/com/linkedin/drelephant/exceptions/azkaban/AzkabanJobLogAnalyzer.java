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

package com.linkedin.drelephant.exceptions.azkaban;

import com.linkedin.drelephant.exceptions.JobState;
import com.linkedin.drelephant.exceptions.LoggingEvent;
import java.util.LinkedHashSet;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
  // Alternate pattern: (".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*");
  private Pattern _scriptOrMRFailExceptionPattern = Pattern.compile("(Caused by.+\\n(?:.*\\n)?((?:.+\\s+at.+\\n)*))+");
  private Pattern _azkabanFailExceptionPattern = Pattern.compile(
      "\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} (PST|PDT) [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
  private Pattern _mrJobIdPattern = Pattern.compile("job_[0-9]+_[0-9]+");
  private Pattern _mrPigJobIdPattern = Pattern.compile("job job_[0-9]+_[0-9]+ has failed!");
  private Pattern _mrHiveJobIdPattern = Pattern.compile("ERROR Ended Job = job_[0-9]+_[0-9]+ with errors");
  private static long SAMPLING_SIZE = 5;

  /**
   * Failure at Azkaban job log is broadly categorized into three categorized into three categories
   * SCHEDULERFAIL: Failure at azkaban level
   * SCRIPTFAIL: Failure at script level
   * MRFAIL: Failure at mapreduce level
   * */
  private JobState _state;
  private LoggingEvent _exception;
  private Set<String> _subEvents;
  private String _rawLog;

  public AzkabanJobLogAnalyzer(String rawLog) {
    this._rawLog = rawLog;
    setSubEvents();
    analyzeLog();
  }

  /**
   * Analyzes the log to find the level of exception
   */
  private void analyzeLog() {
    if (_successfulAzkabanJobPattern.matcher(_rawLog).find()) {
      succeededAzkabanJob();
    } else if (_failedAzkabanJobPattern.matcher(_rawLog).find()) {
      if (!_subEvents.isEmpty()) {
        mrLevelFailedAzkabanJob();
      } else if (_scriptFailPattern.matcher(_rawLog).find()) {
        scriptLevelFailedAzkabanJob();
      } else {
        azkabanLevelFailedAzkabanJob();
      }
    } else if (_killedAzkabanJobPattern.matcher(_rawLog).find()) {
      killedAzkabanJob();
    }
  }

  /**
   * Sets the _state and _exception for Succeeded Azkaban job
   */
  private void succeededAzkabanJob() {
    this._state = JobState.SUCCEEDED;
    this._exception = null;
  }

  /**
   * Sets _state and _exception for Azkaban job which failed at the MR Level
   */
  private void mrLevelFailedAzkabanJob() {
    this._state = JobState.MRFAIL;
    Matcher matcher = _scriptOrMRFailExceptionPattern.matcher(_rawLog);
    StringBuilder exceptionBuilder = new StringBuilder();
    long limit = SAMPLING_SIZE;
    while (matcher.find() && limit > 0) {
      limit--;
      exceptionBuilder.append(matcher.group());
    }
    this._exception = new LoggingEvent(exceptionBuilder.toString());
  }

  /**
   * Set _state and _exception for Azkaban job which failed at the Script Level
   */
  private void scriptLevelFailedAzkabanJob() {
    this._state = JobState.SCRIPTFAIL;
    Matcher matcher = _scriptOrMRFailExceptionPattern.matcher(_rawLog);
    StringBuilder exceptionBuilder = new StringBuilder();
    long limit = SAMPLING_SIZE;
    while (matcher.find() && limit > 0) {
      limit--;
      exceptionBuilder.append(matcher.group());
    }
    this._exception = new LoggingEvent(exceptionBuilder.toString());
  }

  /**
   * Set _state and _exception for Azkaban job which failed at the Azkaban Level
   */
  private void azkabanLevelFailedAzkabanJob() {
    this._state = JobState.SCHEDULERFAIL;
    Matcher matcher = _azkabanFailExceptionPattern.matcher(_rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
  }

  /**
   * Set _state and _exception for killed Azkaban job
   */
  private void killedAzkabanJob() {
    this._state = JobState.KILLED;
    this._exception = null;
  }

  /**
   * @return returns Azkaban job state
   */
  public JobState getState() {
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
   */
  private void setSubEvents() {
    Set<String> subEvents = new LinkedHashSet<String>();

    // check for pig jobs
    Matcher pigJobMatcher = _mrPigJobIdPattern.matcher(_rawLog);
    while (pigJobMatcher.find()) {
      String pigJobFailedString = pigJobMatcher.group();
      Matcher jobIdMatcher = _mrJobIdPattern.matcher(pigJobFailedString);
      if (jobIdMatcher.find()) {
        subEvents.add(jobIdMatcher.group());
        this._subEvents = subEvents;
        return;
      }
    }

    pigJobMatcher.reset();

    // check for hive jobs
    Matcher hiveJobMatcher = _mrHiveJobIdPattern.matcher(_rawLog);
    while (hiveJobMatcher.find()) {
      String hiveJobFailedString = hiveJobMatcher.group();
      Matcher jobIdMatcher = _mrJobIdPattern.matcher(hiveJobFailedString);
      if (jobIdMatcher.find()) {
        subEvents.add(jobIdMatcher.group());
        this._subEvents = subEvents;
        return;
      }
    }

    // any other job than pig or hive
    Matcher matcher = _mrJobIdPattern.matcher(_rawLog);
    long counter = SAMPLING_SIZE;  // sample the applications
    while (matcher.find() && counter > 0) {
      counter--;
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
}