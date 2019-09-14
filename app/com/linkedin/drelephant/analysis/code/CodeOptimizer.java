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

import com.linkedin.drelephant.analysis.code.dataset.Code;
import com.linkedin.drelephant.analysis.code.dataset.Script;
import java.util.List;


/**
 * Code Optimizier Interface , one of the concrete implementation
 * is HiveCodeOptimizer
 */
public interface CodeOptimizer {
  /**
   *
   * @param sourceCode : Give the sourceCode and convert it into Code (which is List<Statement></Statement>)
   * @return Return Code : Format on which optimizations can be applied.
   */
  Code generateCode(String sourceCode);

  /**
   *  Return the Severity of the code, High severity means , it should be fix
   *  immediately
   * @return
   */
  String getSeverity();

  /**
   * This is the brain of the optimization . It will do the optimization of the code.
   * @param sourceScript : Source Code
   * @param rules : Rules to apply to optimize the code.
   * @return
   */

  void optimizationEngine(Script sourceScript, List<CodeOptimizationRule> rules);

  /**
   *
   * @param sourceCode Souce code of the script
   * @return : Script , which contains optimization comments and also Code.
   */

  Script execute(String sourceCode);
}

