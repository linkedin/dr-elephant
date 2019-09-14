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


package com.linkedin.drelephant.analysis.code.util;

import org.apache.hadoop.conf.Configuration;

import static com.linkedin.drelephant.analysis.code.util.Constant.*;

import org.apache.log4j.Logger;


/**
 * This class is Helper class , contains Helper methods or
 * classes which are required to Build Configuration
 */

public class Helper {
  private static final Logger logger = Logger.getLogger(Helper.class);
  private static boolean debugEnabled = logger.isDebugEnabled();

  /**
   * This class used to create configuration required for code level analysis.
   */
  public static class ConfigurationBuilder {
    public static CodeAnalysisConfiguration<String> BASE_URL_FOR_EXTRACTING_CODE = null;
    public static CodeAnalysisConfiguration<String[]> QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION = null;
    public static CodeAnalysisConfiguration<Integer> THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE = null;
    public static CodeAnalysisConfiguration<String[]> SHUFFLE_OPERATIONS_IN_HIVE = null;

    public static void buildConfigurations(Configuration configuration) {
      if (BASE_URL_FOR_EXTRACTING_CODE == null) {
        BASE_URL_FOR_EXTRACTING_CODE =
            new CodeAnalysisConfiguration<String>().setConfigurationName(BASE_URL_FOR_EXTRACTING_CODE_NAME)
                .setValue(configuration.get(BASE_URL_FOR_EXTRACTING_CODE_NAME))
                .setDoc(" Base URL to extract the source code");
      }
      if (QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION == null) {
        QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION = new CodeAnalysisConfiguration<String[]>().setConfigurationName(
            QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME)
            .setValue(configuration.getStrings(QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME))
            .setDoc(" Projects/queues which are valid for code name extraction");
      }

      THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE =
          new CodeAnalysisConfiguration<Integer>().setConfigurationName(
              THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE_NAME)
              .setValue(configuration.getInt(THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE_NAME, 6))
              .setDoc(" Threshold which will be used by action transformation rule to checkpoint the DAG. ");

      SHUFFLE_OPERATIONS_IN_HIVE =
          new CodeAnalysisConfiguration<String[]>().setConfigurationName(SHUFFLE_OPERATIONS_IN_HIVE_NAME)
              .setValue(
                  configuration.getStrings(SHUFFLE_OPERATIONS_IN_HIVE_NAME, "TOK_ORDERBY", "TOK_GROUPBY", "TOK_UNION",
                      "TOK_JOIN", "TOK_LEFTOUTERJOIN", "TOK_RIGHTOUTERJOIN", "TOK_CROSSJOIN", "TOK_ORDERBY",
                      "TOK_DISTRIBUTEBY", "TOK_CLUSTERBY", "TOK_FULLOUTERJOIN"))
              .setDoc(" shuffle operations in hive");

      if (debugEnabled) {
        logger.debug(" Code Level Analysis configurations ");
        logger.debug(BASE_URL_FOR_EXTRACTING_CODE);
        logger.debug(QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION);
        logger.debug(THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE);
      }
    }
  }
}
