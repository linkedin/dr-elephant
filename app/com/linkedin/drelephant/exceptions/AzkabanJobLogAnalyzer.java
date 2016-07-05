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
* Given a Azkaban job log returns the Azkaban Job State, list of MR jobs and exception (if any) at the Azkaban job level
* */

public class AzkabanJobLogAnalyzer {
  private static final Logger logger = Logger.getLogger(AzkabanJobLogAnalyzer.class);
  private AzkabanJobState _state;
  private LoggingEvent _exception;
  private Set<String> _SubEvents;
  public AzkabanJobLogAnalyzer(String rawLog) {
    //Matcher matcher;
    Pattern successfulAzkabanJobPattern =
        Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status SUCCEEDED");
    Pattern failedAzkabanJobPattern =
        Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status FAILED");
    Pattern killedAzkabanJobPattern =
        Pattern.compile("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status KILLED");
    Pattern scriptFailPattern = Pattern.compile("ERROR - Job run failed!");
    Pattern scriptFailExceptionPattern =
        Pattern.compile(".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*");
    Pattern azkabanFailExceptionPattern = Pattern.compile(
        "\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} PDT [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
    Pattern mrJobIdPattern = Pattern.compile("job_[0-9]+_[0-9]+");

    Set<String> subEvents = new HashSet<String>();
    if (successfulAzkabanJobPattern.matcher(rawLog).find()) {
      this._state = AzkabanJobState.SUCCEEDED;
      this._exception = null;
    } else if (failedAzkabanJobPattern.matcher(rawLog).find()) {
      if (scriptFailPattern.matcher(rawLog).find()) {
        this._state = AzkabanJobState.SCRIPTFAIL;
        Matcher matcher = scriptFailExceptionPattern.matcher(rawLog);
        if (matcher.find()) {
          this._exception = new LoggingEvent(matcher.group());
        }
      } else {
        this._state = AzkabanJobState.AZKABANFAIL;
        Matcher matcher = azkabanFailExceptionPattern.matcher(rawLog);
        if (matcher.find()) {
          logger.info("analyzing " + matcher.group());
          this._exception = new LoggingEvent(matcher.group());
        }
      }
    } else if (killedAzkabanJobPattern.matcher(rawLog).find()) {
      this._state = AzkabanJobState.KILLED;
      this._exception = null;
    }
    Matcher matcher = mrJobIdPattern.matcher(rawLog);
    while (matcher.find()) {
      subEvents.add(matcher.group());
    }
    this._SubEvents = subEvents;
  }

  public AzkabanJobState getState() {
    return this._state;
  }

  public Set<String> getSubEvents() {
    return this._SubEvents;
  }

  public LoggingEvent getException() {
    return this._exception;
  }

  public enum AzkabanJobState {SCRIPTFAIL, AZKABANFAIL, MRFAIL, SUCCEEDED, KILLED}
}