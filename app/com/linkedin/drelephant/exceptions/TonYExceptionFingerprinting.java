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
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.AppResult;
import models.JobsExceptionFingerPrinting;
import org.apache.commons.io.IOUtils;
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

  private HashSet<Integer> exceptionIdSet = new HashSet<>();
  private List<ExceptionInfo> _exceptionInfoList = new ArrayList<>();

  private static final Logger logger = Logger.getLogger(TonYExceptionFingerprinting.class);

  public TonYExceptionFingerprinting(AnalyticJob analyticJob, AppResult appResult) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
  }

  public void doExceptionPrinting() {
    if (isExceptionFingerPrintingAlreadyDone(_analyticJob.getAppId())) {
      return;
    }
    logger.info("Starting to process exception information for " + _analyticJob.getAppId());
    fetchLogData();
    collectJobDiagonsticsInfoFromRM();
    collectExceptionInfoFromLogData();
    saveExceptionFingerprintingData();
  }

  public void fetchLogData() {
    String amStderrLogAddress = _analyticJob.getAmContainerLogsURL() + TONY_STDERR_LOG_URL_SUFFIX +
        LOG_START_OFFSET_PARAM + START_INDEX;
    String amStdoutLogAddress = _analyticJob.getAmContainerLogsURL() + TONY_STDOUT_LOG_URL_SUFFIX +
        LOG_START_OFFSET_PARAM + START_INDEX;
   this.amContainerStderrLogData = fetchLogDataFromAddress(amStderrLogAddress);
   this.amContainerStdoutLogData = fetchLogDataFromAddress(amStdoutLogAddress);
  }

  public void collectJobDiagonsticsInfoFromRM() {
    ExceptionInfo exceptionInfo = new ExceptionInfo();
    exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
    exceptionInfo.setExceptionName("Job Diagnostics");
    exceptionInfo.setExceptionStackTrace("Job Diagnostics: \n" + _analyticJob.getJobDiagnostics());
    exceptionInfo.setWeightOfException(1000);
    exceptionInfo.setExceptionID(1);
    exceptionInfo.setExceptionTrackingURL("");
    _exceptionInfoList.add(exceptionInfo);
  }

  public void collectExceptionInfoFromLogData() {
    List<ExceptionInfo> exceptionInfos = new ArrayList<>();
    if (amContainerStderrLogData.length() != 0) {
      exceptionInfos = filterOutRelevantLogSnippetsFromData(amContainerStderrLogData,
          _analyticJob.getAmContainerLogsURL() + TONY_STDERR_LOG_URL_SUFFIX);
    }

    if (exceptionInfos.size() == 0 && amContainerStdoutLogData.length() != 0) {
        logger.warn("No error information found in AM Stderr logs");
        exceptionInfos = filterOutRelevantLogSnippetsFromData(amContainerStdoutLogData,
            _analyticJob.getAmContainerLogsURL() + TONY_STDOUT_LOG_URL_SUFFIX);
      }
      if (exceptionInfos.size() == 0) {
        logger.error("No Error information found neither in AMStderr log nor in AMStdout log");
      }
      _exceptionInfoList.addAll(exceptionInfos);
    //handle the condition when no data available in the Stdout or Stderr -> Also checkout the condition when LogAggregation is not done
  }

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


  private List<ExceptionInfo> filterOutExactExceptionPattern(String logData, String logLocationURL) {
    List<ExceptionInfo> exactlyMatchingExceptionList = new ArrayList<>();
    List<Pattern> exactExceptionRegexList = new ArrayList<>();
    for (String exactExceptionRegex : ConfigurationBuilder.REGEX_FOR_EXACT_EXCEPTION_PATTERN_IN_TONY_LOGS.getValue()) {
      exactExceptionRegexList.add(Pattern.compile(exactExceptionRegex));
    }
    for (Pattern exactExceptionPattern : exactExceptionRegexList) {
      Matcher exact_exception_pattern_matcher = exactExceptionPattern.matcher(logData);
      while (exact_exception_pattern_matcher.find()) {
        int exceptionId = exact_exception_pattern_matcher.group(0).hashCode();
        if (isExceptionLogDuplicate(exceptionId)) {
          ExceptionInfo exceptionInfo = new ExceptionInfo();
          exceptionInfo.setExceptionID(exceptionId);
          exceptionInfo.setExceptionName(exact_exception_pattern_matcher.group(1));
          exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
          exceptionInfo.setExceptionStackTrace(exact_exception_pattern_matcher.group(0));
          logger.info("Offset exact " + exact_exception_pattern_matcher.start());
          exceptionInfo.setExceptionTrackingURL(logLocationURL + LOG_START_OFFSET_PARAM +
              exact_exception_pattern_matcher.start());
          exactlyMatchingExceptionList.add(exceptionInfo);
        }
      }
    }
    return exactlyMatchingExceptionList;
  }

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
        ExceptionInfo exceptionInfo = new ExceptionInfo();
        exceptionInfo.setExceptionID(partial_pattern_matcher.group(0).hashCode());
        exceptionInfo.setExceptionName(partial_pattern_matcher.group(0).split("\n", 2)[0]);
        exceptionInfo.setExceptionSource(ExceptionInfo.ExceptionSource.DRIVER);
        exceptionInfo.setExceptionStackTrace(partial_pattern_matcher.group(0));
        logger.info("Offset partial " + partial_pattern_matcher.start());
        logger.info("Match for partial exception pattern " + exceptionInfo.getExceptionStackTrace());

        exceptionInfo.setExceptionTrackingURL(logLocationURL + LOG_START_OFFSET_PARAM +
            partial_pattern_matcher.start());
        exceptionInfoList.add(exceptionInfo);
      }
    }
    return exceptionInfoList;
  }

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
      //sparkJobException.exceptionLog = exceptionsTrace;
      if (exceptionsTrace.trim().length() > 2) {
        tonyJobException.exceptionLog = exceptionsTrace;
      } else {
        tonyJobException.exceptionLog = "Couldn't gather driver logs for the job";
      }
      tonyJobException.exceptionType = ExceptionInfo.ExceptionSource.DRIVER.toString();
      tonyJobException.exceptionLog = exceptionsTrace;
      tonyJobException.save();
      jobsExceptionFingerPrinting.save();
    }
  }

  private String fetchLogDataFromAddress(String containerURL) {
      try {
        logger.info("Fetching log data from URL " + containerURL);
        URL amAddress = new URL(containerURL);
        HttpURLConnection connection = (HttpURLConnection) amAddress.openConnection();
        connection.connect();
        InputStream inputStream = connection.getInputStream();
        String responseString = IOUtils.toString(inputStream);
        String logDataString = filterOutLogsFromHTMLResponse(responseString);
        logDataString = StringEscapeUtils.unescapeHtml4(logDataString);
        //Todo: remove this before PR, this for implementation time debugging
        logger.info("Length and Response Code for " + _appResult.id + " "  + responseString.length() + " " + connection.getResponseCode());
        inputStream.close();
        connection.disconnect();
        return logDataString;
      } catch (IOException ex) {
        logger.error("Error occured while fetching log data from " + containerURL, ex);
      }
      return null;
  }

  private String filterOutLogsFromHTMLResponse(String htmlResponse)  {
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
      exceptionInJson = Obj.writeValueAsString(exceptionInfoList.subList(0,
          Math.min(exceptionInfoList.size(), NUMBER_OF_EXCEPTION_TO_PUT_IN_DB.getValue())));
    } catch (IOException ex) {
      logger.error(" Exception while serializing exception info list to JSON ", ex);
    }
    return exceptionInJson;
  }

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

  private boolean isExceptionLogDuplicate(int exceptionLogHashCode) {
    if (!exceptionIdSet.contains(exceptionLogHashCode)) {
      exceptionIdSet.add(exceptionLogHashCode);
      return false;
    }
    return true;
  }
}
