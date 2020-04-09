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

import java.util.regex.Pattern;


/**
 * Thic class have Constants which are used across
 * exception fingerprinting classes or configuration properties name
 */
public final class Constant {

  /**
   * There are two possible type of Classifier , one is RULE Based
   * and another one is ML Based (which can involve supervised classification)
   */
  public enum ClassifierType {
    RULE_BASE_CLASSIFIER, ML_BASED_CLASSIFIER
  }

  public static int DEFAULT_EXCEPTION_LOG_LENGTH_LIMIT = 5120;

  /**
   * Classes in which classifier should classify the exceptions
   * . It can have more classes in future releases , which can further
   * classify user enabled class
   */
  public enum LogClass {
    USER_ENABLED, AUTOTUNING_ENABLED
  }

  /**
   *  Exception Fingerprinting will depend on execution engines .
   *  So different type of execution engines .
   */
  public enum ExecutionEngineType {
    SPARK, MR, TONY
  }

  public enum RulePriority {LOW, MEDIUM, HIGH}

  public static final String REGEX_FOR_EXCEPTION_IN_LOGS_NAME = "ef.regex.for.exception";
  public static final String REGEX_FOR_EXACT_EXCEPTION_PATTERN_IN_TONY_LOGS_KEY =
      "ef.tony.exact.regex.for.exception";
  public static final String REGEX_FOR_PARTIAL_EXCEPTION_PATTERN_IN_TONY_LOGS_KEY =
      "ef.tony.partial.pattern.regex.for.exception";
  public static final String REGEX_FOR_EXCEPTION_PATTERN_IN_AZKABAN_LOGS_KEY =
      "ef.azkaban.regex.for.exception.pattern";
  public static final String REGEX_FOR_REDUNDANT_LOG_PATTERN_IN_AZKABAN_LOGS_CONFIG_NAME =
      "ef.azkaban.regex.redundant.log.pattern";
  public static final String REGEX_AUTO_TUNING_FAULT_NAME = "ef.regex.for.autotuning.fault";
  public static final String FIRST_THRESHOLD_LOG_LENGTH_NAME = "ef.first.threshold.loglength";
  public static final String LAST_THRESHOLD_LOG_LENGTH_NAME = "ef.last.threshold.loglength";
  public static final String THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_NAME = "ef.threshold.percentage.log";
  public static final String THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES_NAME = "ef.threshold.log.index";
  public static final String MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES_NAME = "ef.minimum.loglength.skip.start";
  public static final String NUMBER_OF_STACKTRACE_LINE_NAME = "ef.stacktrace.lines";
  public static final String MAX_NUMBER_OF_STACKTRACE_LINE_TONY_KEY = "ef.max.stacktrace.lines.tony";
  public static final String JHS_TIME_OUT_NAME = "ef.jhs.timeout";
  public static final String THRESHOLD_LOG_LINE_LENGTH_NAME = "ef.log.line.threshold";
  static final String NUMBER_OF_EXCEPTION_TO_PUT_IN_DB_NAME = "ef.number.put.db";
  static final String NUMBER_OF_TONY_EXCEPTION_TO_PUT_IN_DB_NAME = "ef.tony.exception.count.to.save";
  static final String BLACK_LISTED_EXCEPTION_CONF_NAME = "ef.blacklisted.exception";
  static final String MAX_LINE_LENGTH_OF_EXCEPTION_NAME = "ef.max.line.length.exception";
  static final String NUMBER_OF_RETRIES_FOR_FETCHING_DRIVER_LOGS_NAME = "ef.number.retries.fetching.driver.logs";
  static final String DURATION_FOR_THREAD_SLEEP_FOR_FETCHING_DRIVER_LOGS_NAME = "ef.duration.thread.sleep.ms";
  static final String TOTAL_LENGTH_OF_LOG_SAVED_IN_DB_NAME = "ef.loglength.saved.db" ;
  static final String AZKABAN_JOB_LOG_START_OFFSET_CONFIG_KEY = "ef.azkaban.job.log.start.offset" ;
  static final String AZKABAN_JOB_LOG_MAX_LENGTH_CONFIG_KEY = "ef.azkaban.job.log.max.length" ;
  static final String SHOULD_PROCESS_AZKABAN_LOG_CONFIG_NAME = "ef.azkaban.log.processing.enable" ;
  static final String MAX_LOG_SIMILARITY_PERCENTAGE_THRESHOLD_KEY = "ef.max.log.similarity.percentage" ;
}
