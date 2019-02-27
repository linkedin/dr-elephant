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
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.exceptions.Rule;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;

/**
 * Rule identifies exception class based on regex .
 */
public class RegexRule implements Rule {
  private static final Logger logger = Logger.getLogger(RegexRule.class);
  private List<ExceptionInfo> exceptions ;
  private static final List<Pattern> patterns = new ArrayList<Pattern>();
  RulePriority rulePriority;
  static {
    for (String regex : REGEX_AUTO_TUNING_FAULT.getValue()) {
      patterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }
  }

  /**
   * This rule checks for Out of Memory and even if one log have Out of Memory. It will classify the
   * exceptions to Auto Tune Enabled exception . Another extension to this logic is to check how many
   * logs have out of memory exception/error (percentage out of total)
   * and based on that decide whether its autotuning error or not.
   * @return : Class in which the exception classified.
   */

  public LogClass logic (List<ExceptionInfo> exceptions){
    this.exceptions = exceptions;
    for (ExceptionInfo exceptionInfo : this.exceptions) {
      if(checkForPattern((exceptionInfo.getExceptionName() + " " + exceptionInfo.getExcptionStackTrace()))) {
        logger.info(
            "Exception Found " + exceptionInfo.getExceptionName() + " " + exceptionInfo.getExcptionStackTrace());
        logger.info(" Exceptions which can be because of Auto tuning is found .Hence classifying it AutoTuning Fault ");
        return LogClass.AUTOTUNING_ENABLED;
      }
    }
    logger.info(" No Auto tuning related exception found ,hence classifying into User fault. ");
    return LogClass.USER_ENABLED;
  }

  @Override
  public Rule setPriority(RulePriority priority) {
    rulePriority = priority;
    return this;
  }

  @Override
  public RulePriority getPriority() {
    return rulePriority;
  }

  private boolean checkForPattern(String data){
    for (Pattern pattern : patterns) {
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }
}
