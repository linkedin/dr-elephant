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

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
* Given a MR Task log, sets the exception (if any) in the log
*/
public class MRTaskLogAnalyzer {
  private static final Logger logger = Logger.getLogger(MRTaskLogAnalyzer.class);
  private LoggingEvent _exception;
  private long MAX_EXCEPTIONS = 5;
  private Pattern mrTaskExceptionPattern =
      Pattern.compile("Error: (.*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*)");

  public MRTaskLogAnalyzer(String rawLog) {
    setException(rawLog);
  }

  /**
   * Gets the exception of the mr task
   * @return The LoggingEvent corresponding to the exception
   */
  public LoggingEvent getException() {
    return this._exception;
  }

  /**
   * Sets the exception of the mr task
   * @param rawLog Raw log of the task
   */
  private void setException(String rawLog) {
    Matcher matcher = mrTaskExceptionPattern.matcher(rawLog);
    long limitOnExceptionChains = MAX_EXCEPTIONS;
    StringBuilder exceptionBuilder = new StringBuilder();
    while (matcher.find() && limitOnExceptionChains>=0) {
      exceptionBuilder.append(matcher.group());
      limitOnExceptionChains--;
    }
      this._exception = new LoggingEvent(exceptionBuilder.toString());
  }
}