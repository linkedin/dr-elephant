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

import com.google.common.base.Strings;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.exceptions.Classifier;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobExecution;
import models.TuningJobDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionInfo.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;


public class ExceptionFingerprintingSpark implements ExceptionFingerprinting {

  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingSpark.class);
  private static final String JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final int STARTING_INDEX_FOR_URL = 2;
  private static final String CONTAINER_PATTERN = "container";
  private static final String HOSTNAME_PATTERN = ":";
  private static final String STDERR = "STDERR";
  private static final String URL_PATTERN = "/";
  private static final String JOBHISTORY_ADDRESS_FOR_LOG = "http://{0}/jobhistory/nmlogs/{1}";
  private static final String STDERR_URL_CONSTANT = "/stderr/?start=";
  private static final String TRIGGER_FOR_LOG_PARSING = "<PRE>";
  private static final String NEW_LINE_DELIMITER = "\n";
  private static final String TAB_DELIMITER = "\t";
  private static final String REGEX_FOR_NON_DIGIT_CHARACTER = "[^0-9]";

  private enum LogLengthSchema {LOG_LENGTH, LENGTH}

  private AnalyticJob analyticJob;
  private List<StageData> failedStageData;
  private Map<String, String> logSourceInfo;
  private boolean useRestAPI = true;
  // This field is describing the importance of each exceptions
  private int globalExceptionsWeight = 0;
  private long startIndex = 0;
  private String targetURIofFailedStage;

  public ExceptionFingerprintingSpark(List<StageData> failedStageData, String targetURIofFailedStage) {
    this.failedStageData = failedStageData;
    this.logSourceInfo = new HashMap<String, String>();
    this.targetURIofFailedStage = targetURIofFailedStage;
    this.logSourceInfo.put(ExceptionSource.EXECUTOR.name(), targetURIofFailedStage);
    logger.info(" Rest API for Getting Stage failure logs " + targetURIofFailedStage);
  }

  public ExceptionFingerprintingSpark() {
    this.logSourceInfo = new HashMap<String, String>();
  }

  /*
   *
   * @return If use rest API to get
   * TODO : Code for reading the logs from HDFS and then provide option to user for either reading from HDFS or rest API
   */
  public boolean isUseRestAPI() {
    return useRestAPI;
  }

  public void setUseRestAPI(boolean useRestAPI) {
    this.useRestAPI = useRestAPI;
  }

  @Override
  public List<ExceptionInfo> processRawData(AnalyticJob analyticJob) {
    this.analyticJob = analyticJob;
    List<ExceptionInfo> exceptions = new ArrayList<ExceptionInfo>();

    addJobDiagnostics(analyticJob.getJobDiagnostics(), exceptions);
    processStageLogs(exceptions);
    logger.info("Total exception after parsing stages logs " + exceptions.size());
    //TODO : If there are enough information from stage log then we should not call
    //driver logs ,to optimize the process .But it can lead to false positivies  in the system,
    //since failure of stage may or may not be the reason for application failure
    processDriverLogs(exceptions);
    logger.info("Total exception after parsing driver logs " + exceptions.size());
    processExceptionsFromBlackListingPattern(exceptions);
    logger.info("Total exception after removing black listed exceptions  " + exceptions.size());
    return exceptions;
  }

  /**
   *
   * @param jobDiagnostics Job Diagnostics message provided to RM by AM/Driver
   * @param exceptionInfoList Contains all the relevant filtered out exceptions
   */
  private void addJobDiagnostics(String jobDiagnostics, List<ExceptionInfo> exceptionInfoList) {
    String exceptionName = "Job Diagnostics";
    String NOT_AVAILABLE = "Not Available";
    ExceptionInfo jobDiagnosticsInfo = new ExceptionInfo();
    jobDiagnosticsInfo.setExceptionName(exceptionName);
    if (Strings.isNullOrEmpty(jobDiagnostics)) {
      jobDiagnosticsInfo.setExceptionStackTrace(NOT_AVAILABLE);
      //Since No job diagnostic message is present so show this at bottom of results
      jobDiagnosticsInfo.setWeightOfException(Integer.MIN_VALUE);
    } else {
      jobDiagnosticsInfo.setExceptionID(jobDiagnostics.hashCode());
      jobDiagnosticsInfo.setExceptionStackTrace(jobDiagnostics);
      jobDiagnosticsInfo.setWeightOfException(Integer.MAX_VALUE);
    }
    exceptionInfoList.add(jobDiagnosticsInfo);
  }

  /**
   * process stage logs
   * @param exceptions
   */
  private void processStageLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.currentTimeMillis();
    try {
      if (failedStageData != null && failedStageData.size() > 0) {
        for (StageData stageData : failedStageData) {

          /**
           * Currently there is no use of exception unique ID . But in future it can be used to
           * find out simillar exceptions . We might need to change from hashcode to some other id
           * which will give same id , if they similar exceptions .
           */
          //TODO : ID should be same for simillar exceptions
          addExceptions((stageData.failureReason().toString() + "" + stageData.details()).hashCode(),
              stageData.failureReason().toString(), stageData.failureReason().toString()+"\n"+stageData.details(), ExceptionSource.EXECUTOR, exceptions,
              processExceptionTrackingURL(stageData.stageId()));
        }
      } else {
        logger.info(" There are no failed stages data ");
      }
    } catch (Exception e) {
      logger.error("Error process stages logs ", e);
    }
    long endTime = System.currentTimeMillis();
    logger.info(" Total exception/error parsed so far - stage " + exceptions.size());
    debugLog(" Time taken for processing stage logs " + (endTime - startTime) * 1.0 / (1000.0) + "s");
  }

  private String processExceptionTrackingURL(int stageID) {
    return targetURIofFailedStage.replaceAll("failedTasks", "") + stageID;
  }

  private void addExceptions(int uniqueID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource, List<ExceptionInfo> exceptions, String exceptionTrackingURL) {
    globalExceptionsWeight++;
    ExceptionInfo exceptionInfo =
        new ExceptionInfo(uniqueID, exceptionName, exceptionStackTrace, exceptionSource, globalExceptionsWeight,
            exceptionTrackingURL);
    debugLog(" Exception Information " + exceptionInfo);
    exceptions.add(exceptionInfo);
  }

  /**
   * process driver / Application master logs for exception
   * @param exceptions
   */
  //ToDo: In case of unable to get log length there are
  //multiple calls to same URL (we can optimize this)
  // This case shouldn't happen in unless there are some changes
  //in JHS APIs
  private void processDriverLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.currentTimeMillis();
    HttpURLConnection connection = null;
    BufferedReader in = null;
    try {
      String urlToQuery = buildURLtoQuery();
      long startTimeForFirstQuery = System.currentTimeMillis();
      String completeURLToQuery = processForDriverLogURL(urlToQuery);
      logSourceInfo.put(ExceptionInfo.ExceptionSource.DRIVER.name(), urlToQuery + STDERR_URL_CONSTANT + startIndex);
      long endTimeForFirstQuery = System.currentTimeMillis();
      debugLog(
          " Time taken for first query " + (endTimeForFirstQuery - startTimeForFirstQuery) * 1.0 / (1000.0)
              + "s");
      logger.info(" URL to query for driver logs  " + completeURLToQuery);
      connection = intializeHTTPConnection(completeURLToQuery);
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      convertInputStreamToExceptionList(in, exceptions, urlToQuery);
    } catch (Exception e) {
      logger.info(" Exception processing  driver logs ", e);
    } finally {
      gracefullyCloseConnection(in, connection);
    }
    long endTime = System.currentTimeMillis();
    logger.info(" Total exception/error parsed so far - driver " + exceptions.size());
    debugLog(" Time taken for driver logs " + (endTime - startTime) * 1.0 / (1000.0) + "s");
  }

  /**
   *  Read the stream and convert into exceptionList
   * @param in : Reader Stream
   * @param exceptions : list of exceptions
   * @param urlToQuery : JHS Url to query
   */
  public void convertInputStreamToExceptionList(BufferedReader in, List<ExceptionInfo> exceptions, String urlToQuery) {
    String inputLine;
    if (in == null || exceptions == null || urlToQuery == null) {
      logger.error(" Not valid input provided ");
      return;
    }
    try {
      while ((inputLine = in.readLine()) != null) {
        if (inputLine.toUpperCase().contains(TRIGGER_FOR_LOG_PARSING)) {
          debugLog(" Trigger for parsing logs " + inputLine + TAB_DELIMITER + inputLine.length());
          driverLogProcessingForException(in, exceptions, urlToQuery + STDERR_URL_CONSTANT + startIndex,
              (inputLine.trim() + NEW_LINE_DELIMITER).length() - TRIGGER_FOR_LOG_PARSING.length());
          break;
        }
      }
    } catch (IOException e) {
      logger.error(" IO Exception while processing driver logs for ", e);
    }
  }

  private String processForDriverLogURL(String urlToQuery) {
    String completeQuery = getCompleteURLToQuery(urlToQuery, true);
    if (completeQuery == null) {
      logger.info("Since Logs are not there in JHS trying am container logs ");
      completeQuery = getCompleteURLToQuery(analyticJob.getAmContainerLogsURL(), false);
    }
    // This will be in called in very rare cases.
    int numberOfRetries = 0;
    while (completeQuery == null && numberOfRetries <= NUMBER_OF_RETRIES_FOR_FETCHING_DRIVER_LOGS.getValue()) {
      numberOfRetries++;
      logger.info(" Retry for fetching logs from JHS " + numberOfRetries);
      try {
        Thread.sleep(DURATION_FOR_THREAD_SLEEP_FOR_FETCHING_DRIVER_LOGS.getValue());
      } catch (InterruptedException e) {
        logger.warn(" Thread interupted ", e);
      }
      completeQuery = getCompleteURLToQuery(urlToQuery, true);
    }
    if (completeQuery == null) {
      logger.warn(" Cannot get driver logs for app " + analyticJob.getAppId());
      return urlToQuery;
    } else {
      return completeQuery;
    }
  }

  /**
   * If unable to get log length , then read by default last 4096 bytes
   * If log length is greater than threshold then read last some percentage of logs
   * If log length is less than threshold then real complete logs .
   * @param url
   * @return
   */
  private String getCompleteURLToQuery(String url, boolean isJHS) {
    String completeURLToQuery = null;
    BufferedReader in = null;
    HttpURLConnection connection = null;
    try {
      connection = intializeHTTPConnection(url);
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      long logLength = 0l;
      String logLengthTrigger = null;
      if (isJHS) {
        logLengthTrigger = LogLengthSchema.LOG_LENGTH.name().replace("_", " ");
      } else {
        logLengthTrigger = STDERR + " " + HOSTNAME_PATTERN;
      }
      while ((inputLine = in.readLine()) != null) {
        if (inputLine.toUpperCase().contains(logLengthTrigger)) {
          logLength = getLogLength(inputLine.toUpperCase(), logLengthTrigger);
          startIndex = getStartIndexOfDriverLogs(logLength);
          if (startIndex == 0) {
            completeURLToQuery = url;
          } else {
            completeURLToQuery = url + STDERR_URL_CONSTANT + startIndex;
          }
          break;
        }
      }
    } catch (Exception e) {
      logger.error(" Exception while creating complete URL to query ", e);
      return url;
    } finally {
      gracefullyCloseConnection(in, connection);
    }
    return completeURLToQuery;
  }

  /**
   *  Based on the log length , it will give the start index . Driver logs
   *  will be read from given starting index
   * @param logLength
   * @return
   * public for the purpose of testing . Visible for testing is not working
   * if the unit test case is in scala.
   */
  public long getStartIndexOfDriverLogs(long logLength) {
    if (logLength == 0) {
      return 0;
    } else if (logLength <= FIRST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue()) {
      return MINIMUM_LOG_LENGTH_TO_SKIP_IN_BYTES.getValue();
    } else if (logLength <= LAST_THRESHOLD_LOG_LENGTH_IN_BYTES.getValue()) {
      return (long) Math.floor(logLength * THRESHOLD_PERCENTAGE_OF_LOG_TO_READ.getValue());
    } else {
      return (logLength - THRESHOLD_LOG_INDEX_FROM_END_IN_BYTES.getValue());
    }
  }

  private long getLogLength(String logLenghtLine, String logLengthTrigger) {
    long logLength = 0;
    try {
      String logData[] = logLenghtLine.split(logLengthTrigger);
      if (logData.length >= LogLengthSchema.LENGTH.ordinal() + 1) {
        logLength =
            Long.parseLong(logData[LogLengthSchema.LENGTH.ordinal()].replaceAll(REGEX_FOR_NON_DIGIT_CHARACTER, ""));
      }
    } catch (Exception e) {
      logger.error(" Exception parsing log length ", e);
    }
    logger.info(" log length in bytes " + logLength);
    return logLength;
  }

  /**
   *
   * @return Build the JHS URL to query it and get driver/AM logs
   * @throws Exception
   */
  public String buildURLtoQuery() throws Exception {
    Configuration configuration = ElephantContext.instance().getGeneralConf();
    String jobHistoryAddress = configuration.get(JOBHISTORY_WEBAPP_ADDRESS);
    String nodeManagerPort = configuration.get(NODEMANAGER_ADDRESS).split(HOSTNAME_PATTERN)[1];
    String amHostHTTPAddress = parseURL(analyticJob.getAmContainerLogsURL(), nodeManagerPort);
    String completeURLToQuery = MessageFormat.format(JOBHISTORY_ADDRESS_FOR_LOG, jobHistoryAddress, amHostHTTPAddress);
    logger.info(" Query this url for error details " + completeURLToQuery);
    return completeURLToQuery;
  }

  private String parseURL(String url, String nodeManagerPort) throws Exception {
    String amAddress = null, containerID = null, userName = null;
    String data[] = url.split(URL_PATTERN);
    for (int idx = STARTING_INDEX_FOR_URL; idx < data.length; idx++) {
      if (data[idx].contains(HOSTNAME_PATTERN)) {
        amAddress = data[idx].split(HOSTNAME_PATTERN)[0] + HOSTNAME_PATTERN + nodeManagerPort;
      } else if (data[idx].toLowerCase().contains(CONTAINER_PATTERN)) {
        containerID = data[idx];
      } else if (idx == data.length - 1) {
        userName = data[idx];
      }
    }
    return amAddress + URL_PATTERN + containerID + URL_PATTERN + containerID + URL_PATTERN + userName;
  }

  private void driverLogProcessingForException(BufferedReader in, List<ExceptionInfo> exceptions, String queryForJHS,
      int initialOffset) throws IOException {
    String inputLine;
    long countOfChar = initialOffset;
    debugLog(" Initial offset for log " + countOfChar);
    while ((inputLine = in.readLine()) != null) {
      countOfChar += inputLine.length() + NEW_LINE_DELIMITER.length();
      debugLog(inputLine + TAB_DELIMITER + (inputLine + NEW_LINE_DELIMITER).length());
      if (inputLine.length() <= THRESHOLD_LOG_LINE_LENGTH.getValue() && isExceptionContains(inputLine)) {
        String exceptionName = inputLine;
        int stackTraceLine = NUMBER_OF_STACKTRACE_LINE.getValue();
        StringBuilder stackTrace = new StringBuilder();
        int subCount = 0;
        while (stackTraceLine >= 0 && inputLine != null) {
          int maxLengthRow = Math.min(MAX_LINE_LENGTH_OF_EXCEPTION.getValue(), inputLine.length());
          stackTrace.append(inputLine.substring(0, maxLengthRow)).append(NEW_LINE_DELIMITER);
          stackTraceLine--;
          inputLine = in.readLine();
          if (inputLine != null) {
            debugLog("For stack trace " + inputLine + TAB_DELIMITER + (inputLine + NEW_LINE_DELIMITER).length());
            subCount += inputLine.length() + NEW_LINE_DELIMITER.length();
          }
        }
        debugLog(" Length of exceptionStackTrace " + subCount);
        addExceptions((exceptionName + "" + stackTrace).hashCode(), exceptionName, stackTrace.toString(),
            ExceptionInfo.ExceptionSource.DRIVER, exceptions,
            processDriverLogTrackingURL(queryForJHS, countOfChar - (exceptionName + NEW_LINE_DELIMITER).length()));
        countOfChar += subCount;
        debugLog(" Offset after processing whole stack trace " + countOfChar);
        if (inputLine == null) {
          break;
        }
      }
    }
  }

  private String processDriverLogTrackingURL(String queryForJHS, long startIndexOfException) {
    long finalStartIndexForException = this.startIndex + startIndexOfException;
    debugLog(" Exception starting index " + finalStartIndexForException);
    return queryForJHS.split("=")[0] + "=" + finalStartIndexForException;
  }

  /**
   * There are some exceptions which are blacklisted by user and of no importance.
   * These method will remove the black listed exceptions from the final list of exceptions
   *
   * @param exceptions : List of exceptions
   */
  private void processExceptionsFromBlackListingPattern(List<ExceptionInfo> exceptions) {
    List<ExceptionInfo> blackListedException = new ArrayList<>();
    for (ExceptionInfo exceptionInfo : exceptions) {
      for (String blackListedPattern : BLACK_LISTED_EXCEPTION_PATTERN.getValue()) {
        if (exceptionInfo.getExceptionName().trim().toLowerCase().contains(blackListedPattern.trim().toLowerCase())) {
          debugLog(" Blacked listed following exception " + exceptionInfo);
          blackListedException.add(exceptionInfo);
          break;
        }
      }
    }
    exceptions.removeAll(blackListedException);
  }

  @Override
  public LogClass classifyException(List<ExceptionInfo> exceptionInformation) {
    if (exceptionInformation != null && exceptionInformation.size() > 0) {
      Classifier classifier = ClassifierFactory.getClassifier(ClassifierType.RULE_BASE_CLASSIFIER);
      classifier.preProcessingData(exceptionInformation);
      LogClass logClass = classifier.classify(exceptionInformation);
      return logClass;
    }
    return null;
  }

  @Override
  public boolean saveData(String jobExecId) throws Exception {
    JobExecution jobExecution = JobExecution.find.where().eq(JobExecution.TABLE.jobExecId, jobExecId).findUnique();
    if (jobExecution == null) {
      logger.warn(" Job Execution with following id doesn't exist " + jobExecId);
      return false;
    } else {
      TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
          .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobExecution.job.jobDefId)
          .findUnique();
      if (tuningJobDefinition == null) {
        logger.warn("Job definition with following job execution id doesnt' exist. " + jobExecId);
        return false;
      } else {
        if (!tuningJobDefinition.autoApply) {
          logger.info(" Auto tuning is not enabled on the job " + tuningJobDefinition);
          return false;
        } else {
          logger.info(" Job execution is failed because of autotuning . Saving this result to DB" + jobExecution);
          jobExecution.autoTuningFault = true;
          jobExecution.update();
          return true;
        }
      }
    }
  }

  @Override
  public Map<String, String> getExceptionLogSourceInformation() {
    return this.logSourceInfo;
  }
}
