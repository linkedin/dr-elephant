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
 *  Given a raw Azkaban Flow log, it returns the list of unsuccessful azkaban job
 */

public class AzkabanFlowLogAnalyzer {
  private static final Logger logger = Logger.getLogger(AzkabanFlowLogAnalyzer.class);
  private Set<String> _failedSubEvents;
  private Pattern _unsuccessfulAzkabanJobIdPattern =
      Pattern.compile("Job (.*) finished with status (?:FAILED|KILLED) in [0-9]+ seconds");

  public AzkabanFlowLogAnalyzer(String rawLog) {
    setFailedSubEvents(rawLog);
  }

  public Set<String> getFailedSubEvents() {
    return this._failedSubEvents;
  }

  private void setFailedSubEvents(String rawLog) {
    Set<String> failedSubEvents = new HashSet<String>();
    Matcher matcher = _unsuccessfulAzkabanJobIdPattern.matcher(rawLog);
    while (matcher.find()) {
      failedSubEvents.add(matcher.group(1));
    }
    this._failedSubEvents = failedSubEvents;
  }
}