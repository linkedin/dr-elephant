/*
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
 *
 */

package com.linkedin.drelephant.exceptions;

import com.google.common.base.Strings;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.exceptions.azkaban.AzkabanExceptionLogAnalyzer;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import models.AppResult;
import models.JobsExceptionFingerPrinting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;


public class TonYExceptionFingerprinting {
  private AnalyticJob _analyticJob;
  private AppResult _appResult;
  private String amContainerStderrLogData = null;
  private String amContainerStdoutLogData = null;
  private String TONY_STDERR_LOG_URL_SUFFIX = "/amstderr.log";
  private String TONY_STDOUT_LOG_URL_SUFFIX = "/amstdout.log";
  private int START_INDEX = 0;
  private String LOG_START_OFFSET_PARAM = "?start=";

  private HashSet<String> exceptionIdSet = new HashSet<>();
  @Getter
  private List<ExceptionInfo> _exceptionInfoList = new ArrayList<>();
  private String exceptionCategoryName = null;
  private static final String ERROR_CLASSIFICATION = "CANNOT CLASSIFY DATA";
  private static final String UNKNOWN_CLASSIFICATION = "USER_ERROR/UNKNOWN";
  private static final String APPLICATION_TYPE = "tony";

  private static final Logger logger = Logger.getLogger(TonYExceptionFingerprinting.class);
  private static final boolean isDebugEnabled = logger.isDebugEnabled();
  public TonYExceptionFingerprinting(AnalyticJob analyticJob, AppResult appResult) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
  }

  /**
   * Method to initiate Exception FingerPrinting for application
   */
  public void doExceptionPrinting() {
    if (isExceptionFingerPrintingAlreadyDone(_analyticJob.getAppId())) {
      return;
    }
    logger.info("Starting to process exception information for " + _analyticJob.getAppId());
    fetchLogData();
    collectJobDiagnosticsInfoFromRM();
    collectExceptionInfoFromLogData();
    exceptionCategoryName = classifyException();
    saveExceptionFingerprintingData();
  }

  /**
   * Fetches log from different locations
   */
  public void fetchLogData() {
    String amStderrLogAddress = _analyticJob.getAmContainerLogsURL() + TONY_STDERR_LOG_URL_SUFFIX +
        LOG_START_OFFSET_PARAM + START_INDEX;
    String amStdoutLogAddress = _analyticJob.getAmContainerLogsURL() + TONY_STDOUT_LOG_URL_SUFFIX +
        LOG_START_OFFSET_PARAM + START_INDEX;
   this.amContainerStderrLogData = fetchLogDataFromContainer(amStderrLogAddress);
   this.amContainerStdoutLogData = fetchLogDataFromContainer(amStdoutLogAddress);
  }

  /**
   * Use the jobDiagnosticsInfoFromRM provided by RM
   */
  public void collectJobDiagnosticsInfoFromRM() {
    if (!Strings.isNullOrEmpty(_analyticJob.getJobDiagnostics())) {
      ExceptionInfo exceptionInfo = new ExceptionInfo();
      exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
      exceptionInfo.setExceptionName("Job Diagnostics");
      exceptionInfo.setExceptionStackTrace("Job Diagnostics: \n" + _analyticJob.getJobDiagnostics());
      exceptionInfo.setExceptionID(1);
      exceptionInfo.setExceptionTrackingURL("");
      _exceptionInfoList.add(exceptionInfo);
    }
  }

  /**
   * Filter out relevant exception stacktraces snippets from whole logs
   */
  public void collectExceptionInfoFromLogData() {
    List<ExceptionInfo> exceptionInfos = new ArrayList<>();

    if (Strings.isNullOrEmpty(amContainerStderrLogData) && Strings.isNullOrEmpty(amContainerStdoutLogData)) {
      logger.warn("Both AM stdout and stderr logs empty.");
      if (SHOULD_PROCESS_AZKABAN_LOG.getValue()) {
        _exceptionInfoList.addAll(getAzkabanExceptionInfoResults());
      }
      return;
    }
    if (!Strings.isNullOrEmpty(amContainerStderrLogData)) {
      exceptionInfos = filterOutRelevantLogSnippetsFromData(amContainerStderrLogData,
          _analyticJob.getAmContainerLogsURL() + TONY_STDERR_LOG_URL_SUFFIX);
    }

    if (exceptionInfos.size() == 0 && !Strings.isNullOrEmpty(amContainerStdoutLogData)) {
        logger.warn("No error information found in AM Stderr logs for " + _analyticJob.getAppId());
        exceptionInfos = filterOutRelevantLogSnippetsFromData(amContainerStdoutLogData,
            _analyticJob.getAmContainerLogsURL() + TONY_STDOUT_LOG_URL_SUFFIX);
    }

    if (exceptionInfos.size() == 0) {
      logger.error("No Error information found neither in AMStderr log nor in AMStdout log " +
          _analyticJob.getAppId());
      //If no logs or only Job Diagnostic found then check for Azkaban logs
      if (SHOULD_PROCESS_AZKABAN_LOG.getValue() && exceptionInfos.size() == 0) {
        exceptionInfos.addAll(getAzkabanExceptionInfoResults());
      }
    }
    _exceptionInfoList.addAll(exceptionInfos);
  }

  /**
   * Filter out relevant exception stackTraces snippets from given log
   */
  private List<ExceptionInfo> filterOutRelevantLogSnippetsFromData(String logData,
      String logLocationUrl) {
    logger.info("Filtering out relevant snippets from logData from " + logLocationUrl);
    List<ExceptionInfo> relevantLogSnippets = new ArrayList<>();
    if (Strings.isNullOrEmpty(logData)) {
      logger.info("Log data is null or empty, log_location " + logLocationUrl);
      return relevantLogSnippets;
    }
    relevantLogSnippets.addAll(filterOutExactExceptionPattern(logData,
        logLocationUrl));
    relevantLogSnippets.addAll(filterOutPartialExceptionPattern(logData, logLocationUrl));
    return relevantLogSnippets;
  }

  protected List<ExceptionInfo> getAzkabanExceptionInfoResults() {
    List<ExceptionInfo> azkabanExceptionInfo = new ArrayList<>();
    try {
      logger.info("Fetching Azkaban logs for " + _appResult.jobExecUrl);
      AzkabanExceptionLogAnalyzer azkabanExceptionLogAnalyzer =
          new AzkabanExceptionLogAnalyzer(_appResult.flowExecUrl, _appResult.jobExecUrl);
      azkabanExceptionInfo = azkabanExceptionLogAnalyzer.getExceptionInfoList();
    } catch (Exception ex) {
      logger.error("Couldn't find exception infos from Azkaban logs", ex);
    }
    return azkabanExceptionInfo;
  }

  /**
   * Find and collect out exception stackTraces matching to very well defined Pattern which
   * have known terminate condition/pattern
   */
  private List<ExceptionInfo> filterOutExactExceptionPattern(String logData, String logLocationURL) {
    List<ExceptionInfo> exactlyMatchingExceptionList = new ArrayList<>();
    List<Pattern> exactExceptionRegexList = new ArrayList<>();
    for (String exactExceptionRegex : ConfigurationBuilder.REGEX_FOR_EXACT_EXCEPTION_PATTERN_IN_TONY_LOGS.getValue()) {
      exactExceptionRegexList.add(Pattern.compile(exactExceptionRegex));
    }
    for (Pattern exactExceptionPattern : exactExceptionRegexList) {
      Matcher exact_exception_pattern_matcher = exactExceptionPattern.matcher(logData);
      while (exact_exception_pattern_matcher.find()) {
        if (!isExceptionLogSimilar(exact_exception_pattern_matcher.group(0))) {
          exceptionIdSet.add(exact_exception_pattern_matcher.group(0));
          ExceptionInfo exceptionInfo = new ExceptionInfo();
          exceptionInfo.setExceptionID(exact_exception_pattern_matcher.group(0).hashCode());
          exceptionInfo.setExceptionName(exact_exception_pattern_matcher.group(1));
          exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
          exceptionInfo.setExceptionStackTrace(Utils.truncateStackTrace(exact_exception_pattern_matcher.group(0),
              MAX_NUMBER_OF_STACKTRACE_LINE_TONY.getValue()));
          exceptionInfo.setExceptionTrackingURL(logLocationURL + LOG_START_OFFSET_PARAM +
              exact_exception_pattern_matcher.start());
          exactlyMatchingExceptionList.add(exceptionInfo);
        }
      }
    }
    return exactlyMatchingExceptionList;
  }

  /**
   * Find and collect out exception stackTraces matching Pattern which
   * don't have known terminate condition/pattern, so we limit them by stackTrace line count
   */
  private List<ExceptionInfo> filterOutPartialExceptionPattern(String logData,
      String logLocationURL) {
    List<ExceptionInfo> exceptionInfoList = new ArrayList<>();
    List<Pattern> partialExceptionRegexList = new ArrayList<>();
    for (String partialExceptionRegex : REGEX_FOR_PARTIAL_EXCEPTION_PATTERN_IN_TONY_LOGS.getValue()) {
      partialExceptionRegex = partialExceptionRegex + String.format("(.*\n?){%d,%d}", 0, NUMBER_OF_STACKTRACE_LINE.getValue());
      partialExceptionRegexList.add(Pattern.compile(partialExceptionRegex));
    }
    for (Pattern exactExceptionPattern : partialExceptionRegexList) {
      Matcher partial_pattern_matcher = exactExceptionPattern.matcher(logData);
      while (partial_pattern_matcher.find()) {
        if (!(isExceptionLogSimilar(partial_pattern_matcher.group(0)))) {
          exceptionIdSet.add(partial_pattern_matcher.group(0));
          ExceptionInfo exceptionInfo = new ExceptionInfo();
          exceptionInfo.setExceptionID(partial_pattern_matcher.group(0).hashCode());
          exceptionInfo.setExceptionName(partial_pattern_matcher.group(1));
          exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
          exceptionInfo.setExceptionStackTrace(partial_pattern_matcher.group(0));
          exceptionInfo.setExceptionTrackingURL(logLocationURL + LOG_START_OFFSET_PARAM +
              partial_pattern_matcher.start());
          exceptionInfoList.add(exceptionInfo);
        }
      }
    }
    return exceptionInfoList;
  }

  /**
   * Save the result to Database
   */
  public void saveExceptionFingerprintingData() {
    if (_exceptionInfoList != null) {
      final String NOT_APPLICABLE = "NA";
      String exceptionsTrace = convertToJSON(_exceptionInfoList);

      JobsExceptionFingerPrinting jobsExceptionFingerPrinting = new JobsExceptionFingerPrinting();
      jobsExceptionFingerPrinting.appId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.taskId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.flowExecUrl = _appResult.flowExecUrl;
      jobsExceptionFingerPrinting.jobName = getJobName(_appResult.jobExecUrl);
      jobsExceptionFingerPrinting.exceptionLog = "";
      jobsExceptionFingerPrinting.exceptionType = HadoopException.HadoopExceptionType.TONY.toString();

      JobsExceptionFingerPrinting tonyJobException = new JobsExceptionFingerPrinting();
      tonyJobException.flowExecUrl = _appResult.flowExecUrl;
      tonyJobException.appId = _appResult.id;
      tonyJobException.taskId = NOT_APPLICABLE;
      tonyJobException.jobName = getJobName(_appResult.jobExecUrl);
      tonyJobException.logSourceInformation = _analyticJob.getAmContainerLogsURL();
      if (exceptionsTrace.trim().length() > 2) {
        tonyJobException.exceptionLog = exceptionsTrace;
      } else {
        tonyJobException.exceptionLog = "Couldn't gather driver logs for the job";
      }
      tonyJobException.exceptionType = ExceptionInfo.ExceptionSource.DRIVER.toString();
      tonyJobException.exceptionLog = exceptionsTrace;
      tonyJobException.classification = exceptionCategoryName;
      tonyJobException.save();
      jobsExceptionFingerPrinting.save();
    }
  }

  /**
   * Fetch log data from container URL
   * @param containerURL URL of the container to fetch logs from
   * @return logData: Parse the response and filter out the log part as string
   */
  private String fetchLogDataFromContainer(String containerURL) {
    try {
      logger.info("Fetching log data from URL " + containerURL);
      URL amAddress = new URL(containerURL);
      HttpURLConnection connection = (HttpURLConnection) amAddress.openConnection();
      connection.connect();
      if (connection.getResponseCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
        logger.warn("Request to fetch container log not successful " + IOUtils.toString(connection.
            getErrorStream()));
        return StringUtils.EMPTY;
      }
      InputStream inputStream = connection.getInputStream();
      String responseString = IOUtils.toString(inputStream);
      String logDataString = filterOutLogsFromHTMLResponse(responseString);
      logDataString = StringEscapeUtils.unescapeHtml4(logDataString);
      inputStream.close();
      connection.disconnect();
      return logDataString;
    } catch (IOException ex) {
      logger.error("Error occurred while fetching log data from " + containerURL, ex);
    }
    return null;
  }

  /**
   * Filter out logs part from HTML response provided by RM API
   * @param htmlResponse reponse from RM API
   * @return Logs data
   */
  private String filterOutLogsFromHTMLResponse(String htmlResponse) {
    String regex_for_pattern_of_logs_in_html_response = "<pre>([\\s\\S]+)</pre>";
    Matcher logPatternMatcher = Pattern.compile(regex_for_pattern_of_logs_in_html_response).matcher(htmlResponse);
    StringBuilder logData = new StringBuilder();
    while (logPatternMatcher.find()) {
      logData.append(logPatternMatcher.group(1));
    }
    return logData.toString();
  }

  /**
   * @param exceptionInfoList : Exception List
   * @return JSON Array Object as String containing exceptions
   */
  private String convertToJSON(List<ExceptionInfo> exceptionInfoList) {
    ObjectMapper Obj = new ObjectMapper();
    String exceptionInJson = null;
    try {
      logger.info("Size \n\n" + exceptionInfoList.size());
      exceptionInJson = Obj.writeValueAsString(exceptionInfoList.subList(0,
          Math.min(exceptionInfoList.size(), NUMBER_OF_TONY_EXCEPTION_TO_PUT_IN_DB.getValue())));
    } catch (IOException ex) {
      logger.error("Exception while serializing exception info list to JSON ", ex);
    }
    return exceptionInJson;
  }

  /**
   * @param  appId Application id
   * @return Whether exception fingerprinting is done for this APP_ID earlier
   */
  private boolean isExceptionFingerPrintingAlreadyDone(String appId) {
    List<JobsExceptionFingerPrinting> result = JobsExceptionFingerPrinting.find.select("*")
        .where()
        .eq(JobsExceptionFingerPrinting.TABLE.APP_ID, appId)
        .findList();
    if (result != null && result.size() > 0) {
      logger.info("ExceptionFingerPrinting is already done for appId " + appId);
      return true;
    }
    return false;
  }

  /**
   * @param exceptionStackTrace HashCode for filterOut Duplicate or Similar exception stackTrace
   * @return whether this exception stackTrace is parsed and saved before
   */
  private boolean isExceptionLogSimilar(String exceptionStackTrace) {
    if (exceptionIdSet.contains(exceptionStackTrace)) {
      for (String uniqueExceptions : exceptionIdSet) {
        if (uniqueExceptions.equals(exceptionStackTrace)) {
          return true;
        }
      }
    }
    for (String uniqueExceptions : exceptionIdSet) {
      if (getLogSimilarityPercentage(exceptionStackTrace, uniqueExceptions) >
          MAX_LOG_SIMILARITY_PERCENTAGE_THRESHOLD.getValue()) {
        return true;
      }
    }
    return false;
  }
  /**
   *
   * @return Return the classification for the exception .
   */
  public String classifyException() {
    if (EXCEPTION_CATEGORIZATION_CONFIGURATION == null || this._exceptionInfoList == null
        || this._exceptionInfoList.size() == 0) {
      logger.error(" Necessary information is not available for classifying exceptions ");
      return ERROR_CLASSIFICATION;
    }
    List<ExceptionCategorizationData> exceptionCategorizationData =
        EXCEPTION_CATEGORIZATION_CONFIGURATION.getValue().getExceptionCategorizationData().get(APPLICATION_TYPE);
    if (exceptionCategorizationData == null || exceptionCategorizationData.size() == 0) {
      logger.error(" No exception categorization rules for tony application ");
      return ERROR_CLASSIFICATION;
    }
    String exceptionCategoryName = UNKNOWN_CLASSIFICATION;
    for (ExceptionCategorizationData exceptionData : exceptionCategorizationData) {
      List<String> searchPattern = exceptionData.getRuleTriggers();
      if (isDebugEnabled) {
        logger.debug(" Search pattern " + exceptionData.getRuleTriggers());
      }
      for (ExceptionInfo exceptionInfo : this._exceptionInfoList) {
        for (String pattern : searchPattern) {
          if (isDebugEnabled) {
            logger.debug(" " + pattern + " " + exceptionInfo.getExceptionStackTrace());
          }
          if (exceptionInfo.getExceptionStackTrace().toLowerCase().contains(pattern)) {
            exceptionCategoryName = exceptionData.getCategory();
            logger.info(
                " Class of the exception " + exceptionCategoryName + "\t because " + exceptionInfo.getExceptionStackTrace());
            return exceptionCategoryName;
          }
        }
      }
    }
    return exceptionCategoryName;
  }


}
