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

/**
 * Factory to Return the code optimizer based on the extension of the code file name.
 */
package com.linkedin.drelephant.analysis.code.optimizers;

import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.optimizers.hive.HiveCodeOptimizer;
import org.apache.log4j.Logger;


public class CodeOptimizerFactory {
  private static final Logger logger = Logger.getLogger(CodeOptimizerFactory.class);

  public static CodeOptimizer getCodeOptimizer(String fileName) {
    if (fileName.endsWith(".sql") || fileName.endsWith(".hql")) {
      logger.info(" Source code is of Hive type , therefore hive code optimizer is called ");
      return new HiveCodeOptimizer();
    } else {
      return null;
    }
  }
}
