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
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import models.JobDefinition;
import models.JobExecution;
import models.TuningJobDefinition;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import static com.linkedin.drelephant.exceptions.util.ExceptionInfo.*;
import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;


public class ExceptionFingerprintingSpark implements ExceptionFingerprinting {

  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingSpark.class);
  private boolean debugEnabled = logger.isDebugEnabled();
  private static final String JOBHISTORY_WEBAPP_ADDRESS = "mapreduce.jobhistory.webapp.address";
  private static final String NODEMANAGER_ADDRESS = "yarn.nodemanager.address";
  private static final int STARTING_INDEX_FOR_URL = 2;
  private static final String CONTAINER_PATTERN = "container";
  private static final String HOSTNAME_PATTERN = ":";
  private static final String URL_PATTERN = "/";
  private static final String JOBHISTORY_ADDRESS_FOR_LOG = "http://{0}/jobhistory/nmlogs/{1}";

  private enum LogLengthSchema {LOG_LENGTH, LENGTH}

  private AnalyticJob analyticJob;
  private List<StageData> failedStageData;
  private boolean useRestAPI = true;

  static {
    ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  public ExceptionFingerprintingSpark(List<StageData> failedStageData) {
    this.failedStageData = failedStageData;
    if (failedStageData == null) {
      logger.debug(" No data fetched for stages ");
    }
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

    processStageLogs(exceptions);
    //TODO : If there are enough information from stage log then we should not call
    //driver logs ,to optimize the process .But it can lead to false positivies  in the system,
    //since failure of stage may or may not be the reason for application failure
    processDriverLogs(exceptions);
    return exceptions;
  }

  /**
   * process stage logs
   * @param exceptions
   */
  private void processStageLogs(List<ExceptionInfo> exceptions) {
    long startTime = System.nanoTime();
    try {
      if (failedStageData != null && failedStageData.size() > 0) {
        for (StageData stageData : failedStageData) {
          /**
           * Currently there is no use of exception unique ID . But in future it can be used to
           * find out simillar exceptions . We might need to change from hashcode to some other id
           * which will give same id , if they similar exceptions .
           */
          //TODO : ID should be same for simillar exceptions
          addExceptions((stageData.failureReason().get() + "" + stageData.details()).hashCode(),
              stageData.failureReason().get(), stageData.details(), ExceptionSource.EXECUTOR, exceptions);
        }
      } else {
        logger.info(" There are no failed stages data ");
      }
    } catch (Exception e) {
      logger.error("Error process stages logs ", e);
    }
    long endTime = System.nanoTime();
    logger.info(" Total exception/error parsed so far - stage " + exceptions.size());
    logger.info(" Time taken for processing stage logs " + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  private void addExceptions(int uniqueID, String exceptionName, String exceptionStackTrace,
      ExceptionSource exceptionSource, List<ExceptionInfo> exceptions) {
    ExceptionInfo exceptionInfo = new ExceptionInfo(uniqueID, exceptionName, exceptionStackTrace, exceptionSource);
    if (debugEnabled) {
      logger.debug(" Exception Information " + exceptionInfo);
    }
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
    long startTime = System.nanoTime();
    HttpURLConnection connection = null;
    BufferedReader in = null;
    try {
      String urlToQuery = buildURLtoQuery();
      long startTimeForFirstQuery = System.nanoTime();
      String completeURLToQuery = completeURLToQuery(urlToQuery);
      long endTimeForFirstQuery = System.nanoTime();
      logger.info(
          " Time taken for first query " + (endTimeForFirstQuery - startTimeForFirstQuery) * 1.0 / (1000000000.0)
              + "s");
      logger.info(" URL to query for driver logs  " + completeURLToQuery);
      connection =  intializeHTTPConnection(completeURLToQuery);
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      String logLengthTrigger = LogLengthSchema.LOG_LENGTH.name().replace("_", " ");
      try {
        while ((inputLine = in.readLine()) != null) {
          if (inputLine.toUpperCase().contains(logLengthTrigger)) {
            if (debugEnabled) {
              logger.debug(" Processing of logs ");
            }
            driverLogProcessingForException(in, exceptions);
            break;
          }
        }
      } catch (IOException e) {
        logger.error(" IO Exception while processing driver logs for ", e);
      }
    } catch (Exception e) {
      logger.info(" Exception processing  driver logs ", e);
    } finally {
      gracefullyCloseConnection(in, connection);
    }
    long endTime = System.nanoTime();
    logger.info(" Total exception/error parsed so far - driver " + exceptions.size());
    logger.info(" Time taken for driver logs " + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  /**
   * If unable to get log length , then read by default last 4096 bytes
   * If log length is greater than threshold then read last some percentage of logs
   * If log length is less than threshold then real complete logs .
   * @param url
   * @return
   */
  private String completeURLToQuery(String url) {
    String completeURLToQuery = null;
    BufferedReader in = null;
    HttpURLConnection connection = null;
    try {
      connection =  intializeHTTPConnection(url);
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;
      long logLength = 0l;
      String logLengthTrigger = LogLengthSchema.LOG_LENGTH.name().replace("_", " ");
      while ((inputLine = in.readLine()) != null) {
        if (inputLine.toUpperCase().contains(logLengthTrigger)) {
          logLength = getLogLength(inputLine.toUpperCase());
          long startIndex = getStartIndexOfDriverLogs(logLength);
          if (startIndex == 0) {
            completeURLToQuery = url;
          } else {
            completeURLToQuery = url + "/stderr/?start=" + startIndex;
          }
          break;
        }
      }
    } catch (Exception e) {
      logger.error(" Exception while creating complete URL to query ", e);
      return url;
    }
    finally{
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

  private long getLogLength(String logLenghtLine) {
    long logLength = 0;
    try {
      String split[] = logLenghtLine.split(HOSTNAME_PATTERN);
      if (split.length >= LogLengthSchema.LENGTH.ordinal()) {
        logLength = Long.parseLong(split[LogLengthSchema.LENGTH.ordinal()].trim());
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

  private void driverLogProcessingForException(BufferedReader in, List<ExceptionInfo> exceptions) throws IOException {
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      if (inputLine.length() <= THRESHOLD_LOG_LINE_LENGTH.getValue() && isExceptionContains(inputLine)) {
        if (debugEnabled) {
          logger.debug(" ExceptionFingerprinting " + inputLine + "\t" + inputLine);
        }
        String exceptionName = inputLine;
        int stackTraceLine = NUMBER_OF_STACKTRACE_LINE.getValue();
        StringBuffer stackTrace = new StringBuffer();
        while (stackTraceLine >= 0 && inputLine != null) {
          stackTrace.append(inputLine);
          stackTraceLine--;
          inputLine = in.readLine();
        }
        addExceptions((exceptionName + "" + stackTrace).hashCode(), exceptionName, stackTrace.toString(),
            ExceptionInfo.ExceptionSource.DRIVER, exceptions);
        if (inputLine == null) {
          break;
        }
      }
    }
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
          logger.info(" Auto tuning is not enabled on the job "+tuningJobDefinition);
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
}
