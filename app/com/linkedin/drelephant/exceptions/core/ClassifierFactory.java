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
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


/**
 * This is factory class to produce the classifier object based on
 * the ClassifierType input
 */
public class ClassifierFactory {
  private static final Logger logger = Logger.getLogger(ClassifierFactory.class);

  /**
   *
   * @param classifierTypes
   * @return Return classifier object based on the type provided
   */
  public static Classifier getClassifier(ClassifierType classifierTypes) {
    switch (classifierTypes) {
      case RULE_BASE_CLASSIFIER:
        logger.info(" Rule Based classifier is called ");
        return new RuleBasedClassifier();
      //TODO: Create a ML based classifier to classify exceptions into one of the classes
      case ML_BASED_CLASSIFIER:
        logger.info(" ML Based classifier is called ");
        return null;
      default:
        logger.error(" Unknown classifier type is passed ");
    }
    return null;
  }
}
