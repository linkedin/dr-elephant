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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;


/**
 * It will be util class which will containe helper method and will be static.
 * It also contains configuration builder .
 */
public class ExceptionUtils {
  private static final Logger logger =
      Logger.getLogger(ExceptionUtils.class);
  static boolean debugEnabled = logger.isDebugEnabled();
  private static final List<Pattern> patterns = new ArrayList<Pattern>();

  static {
    for (String regex : ConfigurationBuilder.REGEX_FOR_EXCEPTION_IN_LOGS.getValue()) {
      patterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }
  }

  public static boolean isExceptionContains(String data) {
    for (Pattern pattern : patterns) {
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }

  public static HttpURLConnection intializeHTTPConnection(String url) throws IOException {
    URL amAddress = new URL(url);
    HttpURLConnection connection = (HttpURLConnection) amAddress.openConnection();
    connection.setConnectTimeout(JHS_TIME_OUT.getValue());
    connection.setReadTimeout(JHS_TIME_OUT.getValue());
    connection.connect();
    return connection;
  }

  public static void gracefullyCloseConnection(BufferedReader in, HttpURLConnection connection) {
    try {
      if (in != null) {
        in.close();
      }
      if (connection != null) {
        connection.disconnect();
      }
    } catch (Exception e1) {
      logger.error(" Exception while closing the connections ", e1);
    }
  }

  /**
   * This class used to create configuration required for exception fingerprinting.
   */
  public static class ConfigurationBuilder {
    public static EFConfiguration<Long> FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES = null;
    public static EFConfiguration<Long> LAST_THRESHOLD_LOG_LENGTH_IN_BYTES = null;
    public static EFConfiguration<Float> THRESHOLD_PERCENTAGE_OF_LOG_TO_READ = null;
    public static EFConfiguration<Long> THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES = null;
    public static EFConfiguration<Long> MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES = null;
    public static EFConfiguration<Integer> NUMBER_OF_STACKTRACE_LINE = null;
    public static EFConfiguration<Integer> JHS_TIME_OUT = null;
    public static EFConfiguration<String[]> REGEX_FOR_EXCEPTION_IN_LOGS = null;
    public static EFConfiguration<String[]> REGEX_AUTO_TUNING_FAULT = null;
    public static EFConfiguration<Integer> THRESHOLD_LOG_LINE_LENGTH = null;


    private static final String[] DEFAULT_REGEX_FOR_EXCEPTION_IN_LOGS =
        {"^.+Exception.*", "^.+Error.*", ".*Container\\s+killed.*"};

    private static final String[] DEFAULT_REGEX_AUTO_TUNING_FAULT =
        {".*java.lang.OutOfMemoryError.*", ".*is running beyond virtual memory limits.*",".*is running beyond physical memory limits.*",
            ".*Container killed on request. Exit code is 103.*", ".*Container killed on request. Exit code is 104.*"};

    public static void buildConfigurations(Configuration configuration) {
      FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<Long>().setConfigurationName(FIRST_THRESHOLD_LOG_LENGTH_NAME)
              .setValue(configuration.getLong(FIRST_THRESHOLD_LOG_LENGTH_NAME, 260059L))
              .setDoc("If the driver log length is less than this value , " + "then completely read the driver logs . "
                  + "After skipping MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES from the start. "
                  + "Default value is assigned after testing analyzing the time");
      LAST_THRESHOLD_LOG_LENGTH_IN_BYTES =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<Long>().setConfigurationName(LAST_THRESHOLD_LOG_LENGTH_NAME)
              .setValue(configuration.getLong(LAST_THRESHOLD_LOG_LENGTH_NAME, 1000000L))
              .setDoc("If the driver log length is greater  than  FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES, "
                  + "and less than this threshold " + "then start index for reading the log would of "
                  + "percentage of THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_FROM_LAST. Since "
                  + "reading of complete logs would be an time consuming process "
                  + "and generally important exceptions are at the end of the logs ");
      THRESHOLD_PERCENTAGE_OF_LOG_TO_READ =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<Float>().setConfigurationName(THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_NAME)
              .setValue(configuration.getFloat(THRESHOLD_PERCENTAGE_OF_LOG_TO_READ_NAME, 0.95f))
              .setDoc("If the driver logs are bigger than first threshold and "
                  + "less then second threshold then just read this percentage of log ."
                  + " Note , this would be the starting index of the log and it will read till end");
      THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<Long>().setConfigurationName(THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES_NAME)
              .setValue(configuration.getLong(THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES_NAME, 50000L))
              .setDoc("If the log length is bigger then the second threshold then we "
                  + "will read this many bytes from the end foe exception fingerprinting");
      MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<Long>().setConfigurationName(MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES_NAME)
              .setValue(configuration.getLong(MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES_NAME, 3000L))
              .setDoc("If the framework is supposed to read the complete logs ,"
                  + "then in that case , it will skip the intial these many bytes");
      NUMBER_OF_STACKTRACE_LINE = new com.linkedin.drelephant.exceptions.util.EFConfiguration<Integer>().setConfigurationName(NUMBER_OF_STACKTRACE_LINE_NAME)
          .setValue(configuration.getInt(NUMBER_OF_STACKTRACE_LINE_NAME, 3))
          .setDoc("Number of stack trace lines read , after the exception encountered");

      JHS_TIME_OUT = new com.linkedin.drelephant.exceptions.util.EFConfiguration<Integer>().setConfigurationName(JHS_TIME_OUT_NAME)
          .setValue(configuration.getInt(JHS_TIME_OUT_NAME, 150000))
          .setDoc("If the JHS is unresponsive ,then for how long the thread should wait. This is in ms ");

      REGEX_FOR_EXCEPTION_IN_LOGS =
          new com.linkedin.drelephant.exceptions.util.EFConfiguration<String[]>().setConfigurationName(REGEX_FOR_EXCEPTION_IN_LOGS_NAME)
              .setValue(configuration.getStrings(REGEX_FOR_EXCEPTION_IN_LOGS_NAME, DEFAULT_REGEX_FOR_EXCEPTION_IN_LOGS))
              .setDoc("These are the regex used to search for exception in logs ");

      REGEX_AUTO_TUNING_FAULT = new EFConfiguration<String[]>().setConfigurationName(REGEX_AUTO_TUNING_FAULT_NAME)
          .setValue(configuration.getStrings(REGEX_AUTO_TUNING_FAULT_NAME, DEFAULT_REGEX_AUTO_TUNING_FAULT))
          .setDoc("These are the regex used to tag failure to auto tuning fault");

      THRESHOLD_LOG_LINE_LENGTH = new com.linkedin.drelephant.exceptions.util.EFConfiguration<Integer>()
          .setConfigurationName(THRESHOLD_LOG_LINE_LENGTH_NAME)
          .setValue(configuration.getInt(THRESHOLD_LOG_LINE_LENGTH_NAME, 1000))
          .setDoc("Log lines which have length less than this threshold will only get analyszed or looked for exception");

      if (debugEnabled) {
        logger.debug(" Exception Fingerprinting configurations ");
        logger.debug(FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES);
        logger.debug(LAST_THRESHOLD_LOG_LENGTH_IN_BYTES);
        logger.debug(THRESHOLD_PERCENTAGE_OF_LOG_TO_READ);
        logger.debug(THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES);
        logger.debug(MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES);
        logger.debug(NUMBER_OF_STACKTRACE_LINE);
        logger.debug(JHS_TIME_OUT);
      }
    }
  }
}
