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

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.util.List;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


/**
 * Every class who wants to do exception fingerprinting should extend this interface .
 * ExcpetionFingerprintingSpark is the one implementation for this interface.
 */
public interface ExceptionFingerprinting {
  /**
   * This method is used to process the raw data , provided by
   * analyticsJob . It will process the data and look for exception .
   * Creates the List of exceptionInfo .
   * @param analyticJob
   * @return
   */
  List<ExceptionInfo> processRawData(AnalyticJob analyticJob);

  /**
   *
   * @param exceptionInformation
   * @return Based on the exception information , it classifies failure into one of the LogClass
   * classes
   */
  LogClass classifyException(List<ExceptionInfo> exceptionInformation);

  /**
   *  Once the information is classified , this method will be used to save/persisit the information into DB
   * @param jobExecutionID
   * @return true if the information is successfully saved .
   * @throws Exception : Return exception if information is not been saved successfully.
   * return false if the job is not auto tuning enabled.
   */
  boolean saveData(String jobExecutionID) throws Exception;
}
