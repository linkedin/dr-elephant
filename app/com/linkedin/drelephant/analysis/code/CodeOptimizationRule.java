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

package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.Script;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;


/**
 * This interface defines the methods for describing optimization rule.
 * Optimization rules are the rules which will scan the script and
 * find possible optimization in the script. They should also tell
 * how important<Severity>  its to take this optimization into consideration
 */
public interface CodeOptimizationRule {
  /**
   *  This is the main method which will execute the rule .
   * @param script : Script , which contains framework parsable Code
   * @throws CodeAnalyzerException : It can throw CodeAnalyzerException
   */
  void processRule(Script script) throws CodeAnalyzerException;

  /**
   *  Every rule must have name .
   * @return Rule Name
   */
  String getRuleName();

  /**
   *  After anaylzing the script , rule provides severity (how important its to fix the script
   *  as per the rule)
   * @return
   */
  String getSeverity();
}
