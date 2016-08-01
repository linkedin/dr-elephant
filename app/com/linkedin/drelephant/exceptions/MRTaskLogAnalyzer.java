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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;


/*
* Given a MR Task log, sets the exception (if any) in the log
*/
public class MRTaskLogAnalyzer {
  private static final Logger logger = Logger.getLogger(MRTaskLogAnalyzer.class);
  private LoggingEvent _exception;
  private Pattern mrTaskExceptionPattern =
      Pattern.compile("Error: (.*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*)");

  public MRTaskLogAnalyzer(String rawLog) {
    setException(rawLog);
  }

  /**
   *
   * @return
   */
  public LoggingEvent getException() {
    return this._exception;
  }

  /**
   *
   * @param rawLog
   */
  private void setException(String rawLog) {
    Matcher matcher = mrTaskExceptionPattern.matcher(rawLog);
    if (matcher.find()) {
      this._exception = new LoggingEvent(matcher.group());
    }
    else{
      this._exception = new LoggingEvent("");
    }
  }
}