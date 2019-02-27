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

package com.linkedin.drelephant.exceptions.util;

/**
 * Class to store exception information
 */
public class ExceptionInfo {

  private int exceptionID;
  private String exceptionName;
  private String exceptionStackTrace;
  private ExceptionSource exceptionSource;

  public enum ExceptionSource {DRIVER, EXECUTOR, SCHEDULER}

  public ExceptionInfo(int exceptionID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource) {
    this.exceptionID = exceptionID;
    this.exceptionName = exceptionName;
    this.exceptionStackTrace = exceptionStackTrace;
    this.exceptionSource = exceptionSource;
  }

  //TODO: Currently this has not been used . But the idea to have ID of two excpetion same
  //if they are simillar , so that we can remove simillar exception from the exception fingerprinting
  //system

  public int getExceptionID() {
    return exceptionID;
  }

  public void setExceptionID(int exceptionID) {
    this.exceptionID = exceptionID;
  }

  public String getExceptionName() {
    return exceptionName;
  }

  public String getExcptionStackTrace() {
    return exceptionStackTrace;
  }

  //TODO : Use exception source to prioritize  the exception

  @Override
  public String toString() {
    return "ExceptionInfo{" + "exceptionID=" + exceptionID + ", exceptionName='" + exceptionName + '\''
        + ", exceptionStackTrace='" + exceptionStackTrace + '\'' + ", exceptionSource='" + exceptionSource.name() + '\'' + '}';
  }
}
