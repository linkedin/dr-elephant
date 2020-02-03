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

import java.util.List;
import org.apache.log4j.Logger;


public class HadoopException {
  private final Logger logger = Logger.getLogger(HadoopException.class);
  private String _id = "UNKNOWN";
  private String _appId;
  public enum HadoopExceptionType {FLOW, SCHEDULER, SCRIPT, MR, KILL, MRJOB, MRTASK, SPARK, DRIVER, TONY}
  /**
  * FLOW: HadoopException object for Azkaban flow
  * SCHEDULER : HadoopException object for Azkaban job with Azkaban level failure
  * SCRIPT : HadoopException object for Azkaban job with Script level failure
  * MR: HadoopException object for Azkaban job with MR level failure
  * KILL: HadoopException object for killed Azkaban job
  * MRJOB: HadoopException object for MR Job
  * MRTASK: HadoopException object for MR Task
  * */

  private HadoopExceptionType _type;
  private LoggingEvent _loggingEvent;
  private List<HadoopException> _childExceptions;

  public String getId() {
    return _id;
  }

  public void setId(String id) {
    _id = id;
  }

  public HadoopExceptionType getType() {
    return _type;
  }

  public void setType(HadoopExceptionType type) {
    _type = type;
  }

  public LoggingEvent getLoggingEvent() {
    return _loggingEvent;
  }

  public String getAppId() {
    return _appId;
  }

  public void setAppId(String appId) {
    this._appId = appId;
  }

  void setLoggingEvent(LoggingEvent e) {
    _loggingEvent = e;
  }

  public List<HadoopException> getChildExceptions() {
    return _childExceptions;
  }

  void setChildExceptions(List<HadoopException> childExceptions) {
    _childExceptions = childExceptions;
  }


}


