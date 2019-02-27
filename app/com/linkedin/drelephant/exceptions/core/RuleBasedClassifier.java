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

package com.linkedin.drelephant.exceptions.core;

import com.linkedin.drelephant.exceptions.Classifier;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.exceptions.Rule;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


public class RuleBasedClassifier implements Classifier {
  private static final Logger logger = Logger.getLogger(RuleBasedClassifier.class);
  private List<ExceptionInfo> dataToClassify = null;

  /**
   * In rule based classifier , currently we are not doing any pre processing .
   * But it will be required in future to filter out exception may be based
   * on sources .
   * @param exceptions
   */
  @Override
  public void preProcessingData(List<ExceptionInfo> exceptions) {

  }

  /**
   * This classify applies list of Rules (currently only regex)
   * and the gettheMajority .
   * @param exceptions : List of exceptions provided as input to this method
   * @return
   */
  @Override
  public LogClass classify(List<ExceptionInfo> exceptions) {
    this.dataToClassify = exceptions;
    List<Rule> listOfRules = new ArrayList<Rule>();
    listOfRules.add(new RegexRule().setPriority(RulePriority.HIGH));
    Map<LogClass, Integer> listOfClassesCount = new HashMap<LogClass, Integer>();
    logger.info(" Applying rules ");
    for (Rule rule : listOfRules) {
      LogClass logClass = rule.logic(this.dataToClassify);
      Integer count = listOfClassesCount.get(logClass);
      if (count == null) {
        count = 0;
      }
      count++;
      listOfClassesCount.put(logClass, count);
    }
    return getMajority(listOfClassesCount);
  }

  private LogClass getMajority(Map<LogClass, Integer> listOfClasses) {
    int maxClass = 0;
    LogClass maxLogClass = null;
    for (LogClass logClass : listOfClasses.keySet()) {
      int currentMax = listOfClasses.get(logClass);
      if (currentMax > maxClass) {
        maxClass = currentMax;
        maxLogClass = logClass;
      }
    }
    logger.info(" Class for the exception " + maxLogClass);
    return maxLogClass;
  }
}

