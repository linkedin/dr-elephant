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

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.ExceptionFingerprinting;
import com.linkedin.drelephant.exceptions.HadoopException;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.AppResult;
import models.JobsExceptionFingerPrinting;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.exceptions.util.Constant.*;


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
  private String jobNameRegex = ".*&job=(.*)&.*";
  private Pattern jobNamePattern = Pattern.compile(jobNameRegex);

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
      logger.info(" Exception Fingerprinting thread started for app " + _analyticJob.getAppId());
      ExceptionFingerprinting exceptionFingerprinting =
          ExceptionFingerprintingFactory.getExceptionFingerprinting(executionType, data);
      List<ExceptionInfo> exceptionInfos = exceptionFingerprinting.processRawData(_analyticJob);
      saveDriverExceptionLogForExceptionFingerPrinting(exceptionInfos);
      LogClass logClass = exceptionFingerprinting.classifyException(exceptionInfos);
      boolean isAutoTuningFault = false;
      if (logClass != null && logClass.equals(LogClass.AUTOTUNING_ENABLED)) {
        isAutoTuningFault = true;
      }
      if (isAutoTuningFault) {
        logger.info(" Since auto tuning fault , saving information into db for execution id " + _appResult.jobExecId);
        exceptionFingerprinting.saveData(_appResult.jobExecId);
      }
    } catch (Exception e) {
      logger.error(" Error while processing exception fingerprinting for app " + _analyticJob.getAppId(), e);
    }
    long endTime = System.nanoTime();
    logger.info("Total time spent in exception fingerprinting in  " + _analyticJob.getAppId() + " "
        + (endTime - startTime) * 1.0 / (1000000000.0) + "s");
  }

  private void saveDriverExceptionLogForExceptionFingerPrinting(List<ExceptionInfo> exceptionInfoList) {
    if (exceptionInfoList != null) {
      final String NOT_APPLICABLE = "NA";
      Optional<ExceptionInfo> driverExceptionInfo = exceptionInfoList.parallelStream()
          .filter(ex -> ex.getExceptionSource().equals(ExceptionInfo.ExceptionSource.DRIVER))
          .findFirst();
      if (driverExceptionInfo.isPresent()) {
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
        sparkJobException.exceptionLog = driverExceptionInfo.get().getExcptionStackTrace();
        sparkJobException.exceptionType = ExceptionInfo.ExceptionSource.DRIVER.toString();
        sparkJobException.save();
        jobsExceptionFingerPrinting.save();
      }
    }
  }

  private String getJobName(String jobExecUrl) {
    Matcher matcher = jobNamePattern.matcher(jobExecUrl);
    if (matcher.find()){
      return matcher.group(1);
    }
    return matcher.group(1);
  }
}
