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

import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.util.List;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


/**
 * For rule based classification , there can be different rules to classify the
 * exceptions . One such implementation is RegexRule .
 */
public interface Rule {
  /**
   * This will contain the actual logic of the rule . For e.g for RegexRule
   * it will contain regex which are used to classify exceptions
   * @param exceptions
   * @return
   */
  LogClass logic(List<ExceptionInfo> exceptions);

  /**
   * Every rule set the priorty , this priority can be used by rule based classifier to finally decide the class of the exception.
   * @param priority
   */
  Rule setPriority(RulePriority priority);

  RulePriority getPriority();
}
