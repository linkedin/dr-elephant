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
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.exceptions.HadoopException;
import com.linkedin.drelephant.exceptions.TonYExceptionFingerprinting;
import com.linkedin.drelephant.exceptions.azkaban.AzkabanExceptionLogAnalyzer;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.persistence.PersistenceException;
import models.AppResult;
import models.JobsExceptionFingerPrinting;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.drelephant.exceptions.util.Constant.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;


/**
 * This class is actually responsible to run exception fingerprinting . Class which
 * want to run exception fingerprinting should call this .;
 * It implemented Runnable because it will run as separate thread in future versions
 */
public class ExceptionFingerprintingRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ExceptionFingerprintingRunner.class);
  private AnalyticJob _analyticJob;
  private AppResult _appResult;
  private HadoopApplicationData data;
  private ExecutionEngineType executionType;

  static {
    ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  public ExceptionFingerprintingRunner(AnalyticJob analyticJob, AppResult appResult, HadoopApplicationData data,
      ExecutionEngineType executionType) {
    this._analyticJob = analyticJob;
    this._appResult = appResult;
    this.data = data;
    this.executionType = executionType;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();
    try {
      if (executionType.equals(ExecutionEngineType.TONY)) {
        logger.info("Executing exception fingerprinting for TonY application " + _analyticJob.getAppId());
        new TonYExceptionFingerprinting(_analyticJob, _appResult).doExceptionPrinting();
      } else {
        logger.info(" Exception Fingerprinting thread started for app " + _analyticJob.getAppId());
        ExceptionFingerprinting exceptionFingerprinting =
            ExceptionFingerprintingFactory.getExceptionFingerprinting(executionType, data);
        List<ExceptionInfo> exceptionInfos = exceptionFingerprinting.processRawData(_analyticJob);
        if (SHOULD_PROCESS_AZKABAN_LOG.getValue() && exceptionInfos.size() <= 1) {
          exceptionInfos.addAll(getAzkabanExceptionInfoResults());
        }
        saveDriverExceptionLogForExceptionFingerPrinting(exceptionInfos, exceptionFingerprinting.getExceptionLogSourceInformation());
        LogClass logClass = exceptionFingerprinting.classifyException(exceptionInfos);
        boolean isAutoTuningFault = false;
        if (logClass != null && logClass.equals(LogClass.AUTOTUNING_ENABLED)) {
          isAutoTuningFault = true;
        }
        if (isAutoTuningFault) {
          logger.info(" Since auto tuning fault , saving information into db for execution id " + _appResult.jobExecId);
          exceptionFingerprinting.saveData(_appResult.jobExecId);
        }
      }
    } catch(Exception e){
        logger.error(" Error while processing exception fingerprinting for app " + _analyticJob.getAppId(), e);
    }
    long endTime = System.nanoTime();
    logger.info("Total time spent in exception fingerprinting in  " + _analyticJob.getAppId() + " "
          + (endTime - startTime) * 1.0 / (1000000000.0) + "s");

  }

  private List<ExceptionInfo> getAzkabanExceptionInfoResults() {
    List<ExceptionInfo> azkabanExceptionInfo = new ArrayList<>();
    try {
      logger.info("Fetching Azkaban logs for job " + _appResult.jobExecUrl);
      AzkabanExceptionLogAnalyzer azkabanExceptionLogAnalyzer =
          new AzkabanExceptionLogAnalyzer(_appResult.flowExecUrl, _appResult.jobExecUrl);
      azkabanExceptionInfo = azkabanExceptionLogAnalyzer.getExceptionInfoList();
      logger.info(String.format("Size of exception info list for Azkaban job %s is %d", _appResult.jobExecUrl,
          azkabanExceptionInfo.size()));
    } catch (Exception ex) {
      logger.error("Couldn't find exception infos from Azkaban logs", ex);
    }
    return azkabanExceptionInfo;
  }

  /**
   *
   * @param exceptionInfoList : List contains all the exceptions
   * @param logSourceInformation : Map containing sources of exception
   */
  private void saveDriverExceptionLogForExceptionFingerPrinting(List<ExceptionInfo> exceptionInfoList,
      Map<String, String> logSourceInformation) {
    if (exceptionInfoList != null) {
      final String NOT_APPLICABLE = "NA";

      String exceptionsTrace = parseExceptions(exceptionInfoList, true);

      JobsExceptionFingerPrinting jobsExceptionFingerPrinting = new JobsExceptionFingerPrinting();
      jobsExceptionFingerPrinting.appId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.taskId = NOT_APPLICABLE;
      jobsExceptionFingerPrinting.flowExecUrl = _appResult.flowExecUrl;
      jobsExceptionFingerPrinting.jobName = getJobName(_appResult.jobExecUrl);
      jobsExceptionFingerPrinting.exceptionLog = "";
      jobsExceptionFingerPrinting.exceptionType = HadoopException.HadoopExceptionType.SPARK.toString();

      JobsExceptionFingerPrinting sparkJobException = new JobsExceptionFingerPrinting();
      sparkJobException.flowExecUrl = _appResult.flowExecUrl;
      sparkJobException.appId = _appResult.id;
      sparkJobException.taskId = NOT_APPLICABLE;
      sparkJobException.jobName = getJobName(_appResult.jobExecUrl);
      sparkJobException.logSourceInformation = processLogSourceInformation(logSourceInformation);
      //sparkJobException.exceptionLog = exceptionsTrace;
      if (exceptionsTrace.trim().length() > 2) {
        sparkJobException.exceptionLog = exceptionsTrace;
      } else {
        sparkJobException.exceptionLog = "Couldn't gather driver logs for the job";
      }
      sparkJobException.exceptionType = ExceptionInfo.ExceptionSource.DRIVER.toString();
      saveSparkJobException(sparkJobException, exceptionInfoList);
      jobsExceptionFingerPrinting.save();
    }
  }

  private void saveSparkJobException(JobsExceptionFingerPrinting sparkJobException,
      List<ExceptionInfo> exceptionInfoList) {
    try {
      if (sparkJobException.exceptionLog.length() < TOTAL_LENGTH_OF_LOG_SAVED_IN_DB.getValue()) {
        logger.info("Saving complete logs into db ");
        sparkJobException.save();
      } else {
        logger.info(
            "Complete logs cannot be stored in db , hence creating mix of exceptions with half of original length");
        List<ExceptionInfo> halfExceptionList = createMixofException(exceptionInfoList, exceptionInfoList.size() / 2);
        sparkJobException.exceptionLog = parseExceptions(halfExceptionList, false);
        if (sparkJobException.exceptionLog.length() < TOTAL_LENGTH_OF_LOG_SAVED_IN_DB.getValue()) {
          sparkJobException.save();
        } else {
          logger.info("Since half of the mix logs are not stored , only storing two exceptions ");
          List<ExceptionInfo> firstException = createMixofException(exceptionInfoList, 2);
          sparkJobException.exceptionLog = parseExceptions(firstException, false);
          sparkJobException.save();
        }
      }
      debugLog("Final exception saved in db " + sparkJobException.exceptionLog);
    }
    catch(PersistenceException pe){
      logger.error(" Exception stack trace cannot be saved for "+_appResult.id,pe);
      }
  }

  /**
   * Since we are not able to store all the exceptions in database , this method
   * will create the mix of exceptions (including Executor and Driver)
   * @param exceptionInfoList : List of exceptions .
   * @param size : Maximum size of the list
   * @return : Mix of exceptions
   */
  private List<ExceptionInfo> createMixofException(List<ExceptionInfo> exceptionInfoList, int size) {
    List<ExceptionInfo> mixOfException = new ArrayList<>();
    List<ExceptionInfo> executorExceptions = new ArrayList<>();
    for (ExceptionInfo exceptionInfo : exceptionInfoList) {
      if (exceptionInfo.getExceptionSource().name().equals(ExceptionInfo.ExceptionSource.EXECUTOR.name())) {
        executorExceptions.add(exceptionInfo);
      }
    }
    logger.info(" Size of executor exceptions " + executorExceptions.size());
    for (int index = 0; index <= size / 2; index++) {
      mixOfException.add(exceptionInfoList.get(index));
    }
    logger.info(" Size of mix exceptions " + mixOfException.size());
    int max = Math.min(executorExceptions.size(), size - (size / 2));
    for (int index = 0; index < max; index++) {
      mixOfException.add(executorExceptions.get(index));
    }
    logger.info(" Size of mix exceptions " + mixOfException.size());
    return mixOfException;
  }

  /**
   *  User provided configuration is being used to set the number of
   *  exceptions in DB
   * @param exceptionInfoList : Exception List
   * @param isFirstTime : Is the list parsed first time
   * @return JSON Array as String containing exceptions
   */
  private String parseExceptions(List<ExceptionInfo> exceptionInfoList, boolean isFirstTime) {
    ObjectMapper Obj = new ObjectMapper();
    if (isFirstTime) {
      Collections.sort(exceptionInfoList);
    }
    String exceptionInJson = null;
    try {
      exceptionInJson = Obj.writeValueAsString(exceptionInfoList.subList(0,
          Math.min(exceptionInfoList.size(), NUMBER_OF_EXCEPTION_TO_PUT_IN_DB.getValue())));
      logger.info(" Final Size of the exception List , which will be stored in db " + exceptionInJson);
    } catch (IOException e) {
      logger.error(" Exception while searlizing stack tract list to JSON ", e);
    }
    return exceptionInJson;
  }

  private String processLogSourceInformation(Map<String, String> logSourceInformation) {
    StringBuilder logSource = new StringBuilder();
    for (Map.Entry<String, String> entry : logSourceInformation.entrySet()) {
      logSource.append("SOURCE " + entry.getKey() + "\tURL " + entry.getValue()).append("\n");
    }
    return logSource.toString();
  }

}
