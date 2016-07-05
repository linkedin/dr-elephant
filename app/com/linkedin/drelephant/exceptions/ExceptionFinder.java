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

package com.linkedin.drelephant.exceptions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;


public class ExceptionFinder {
  private final Logger logger = Logger.getLogger(ExceptionFinder.class);
  private HadoopException _exception;
  private AzkabanClient _azkabanClient;
  private MRClient _mrClient;
  private String azkabanLogOffset = "0";
  private String azkabanLogLengthLimit = "99999999";

  public ExceptionFinder(String url)
      throws URISyntaxException {

    String execId = null;
    List<NameValuePair> params = URLEncodedUtils.parse(new URI(url), "UTF-8");
    for (NameValuePair param : params) {
      if (param.getName() == "execid") {
        execId = param.getValue();
      }
    }

    _mrClient = new MRClient();
    _azkabanClient = new AzkabanClient(url);
    _azkabanClient.azkabanLogin("", "");
    String rawFlowLog = _azkabanClient.getAzkabanFlowLog(azkabanLogOffset, azkabanLogLengthLimit);

    _exception = analyzeAzkabanFlow(execId, rawFlowLog);
  }

  private HadoopException analyzeAzkabanFlow(String execId, String rawAzkabanFlowLog) {
    HadoopException flowLevelException = new HadoopException();
    List<HadoopException> childExceptions = new ArrayList<HadoopException>();

    AzkabanFlowLogAnalyzer analyzedLog = new AzkabanFlowLogAnalyzer(rawAzkabanFlowLog);
    Set<String> unsuccessfulAzkabanJobIds = analyzedLog.getFailedSubEvents();

    for (String failedAzkabanJobId : unsuccessfulAzkabanJobIds) {
      //logger.info("flow failed subevent "+ failedAzkabanJobId);
      String rawAzkabanJobLog =
          _azkabanClient.getAzkabanJobLog(failedAzkabanJobId, azkabanLogOffset, azkabanLogLengthLimit);
      HadoopException azkabanJobLevelException = analyzeAzkabanJob(failedAzkabanJobId, rawAzkabanJobLog);
      childExceptions.add(azkabanJobLevelException);
    }

    flowLevelException.setType(HadoopException.HadoopExceptionType.FLOW);
    flowLevelException.setId(execId);
    flowLevelException.setLoggingEvent(null); // No flow level exception
    flowLevelException.setChildExceptions(childExceptions);
    logger.info("flow: " + flowLevelException.getType());
    logger.info("flow: " + flowLevelException.getChildExceptions());
    return flowLevelException;
  }

  private HadoopException analyzeAzkabanJob(String azkabanJobId, String rawAzkabanJobLog) {
    HadoopException azkabanJobLevelException = new HadoopException();
    List<HadoopException> childExceptions = new ArrayList<HadoopException>();
    AzkabanJobLogAnalyzer analyzedLog = new AzkabanJobLogAnalyzer(rawAzkabanJobLog);
    Set<String> mrJobIds = analyzedLog.getSubEvents(); // returns all mrjobs in the azkaban job

    for (String mrJobId : mrJobIds) {
      //To do: Check if mr job logs are there or not in job history server
      String rawMRJobLog = _mrClient.getMRJobLog(mrJobId);
      if (rawMRJobLog != null) { // log not found or successful job
        //To do: rawMRJob is null for successful mr jobs but this is not a job to figure out whether a job failed or succeeded
        HadoopException mrJobLevelException = analyzeMRJob(mrJobId, rawAzkabanJobLog);
        childExceptions.add(mrJobLevelException);
      }
    }
    if (analyzedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.MRFAIL) {
      azkabanJobLevelException.setType(HadoopException.HadoopExceptionType.MR);
      azkabanJobLevelException.setLoggingEvent(analyzedLog.getException());
      azkabanJobLevelException.setChildExceptions(childExceptions);
    } else if (analyzedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.AZKABANFAIL) {
      azkabanJobLevelException.setType(HadoopException.HadoopExceptionType.AZKABAN);
      azkabanJobLevelException.setLoggingEvent(analyzedLog.getException());
      azkabanJobLevelException.setChildExceptions(null);
    } else if (analyzedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.SCRIPTFAIL) {
      azkabanJobLevelException.setType(HadoopException.HadoopExceptionType.SCRIPT);
      azkabanJobLevelException.setLoggingEvent(analyzedLog.getException());
      azkabanJobLevelException.setChildExceptions(null);
    } else if (analyzedLog.getState() == AzkabanJobLogAnalyzer.AzkabanJobState.KILLED) {
      azkabanJobLevelException.setType(HadoopException.HadoopExceptionType.KILL);
      azkabanJobLevelException.setLoggingEvent(null);
      azkabanJobLevelException.setChildExceptions(null);
    }
    azkabanJobLevelException.setId(azkabanJobId);
    return azkabanJobLevelException;
  }

  private HadoopException analyzeMRJob(String mrJobId, String rawMRJoblog) {
    // This method is called only for unsuccessful MR jobs
    HadoopException mrJobLevelException = new HadoopException();
    List<HadoopException> childException = new ArrayList<HadoopException>();
    MRJobLogAnalyzer analyzedLog = new MRJobLogAnalyzer(rawMRJoblog);
    Set<String> failedMRTaskIds = analyzedLog.getFailedSubEvents();

    for (String failedMRTaskId : failedMRTaskIds) {
      String rawMRTaskLog = _mrClient.getMRTaskLog(mrJobId, failedMRTaskId);
      HadoopException mrTaskLevelException = analyzeMRTask(failedMRTaskId, rawMRTaskLog);
      childException.add(mrTaskLevelException);
    }
    mrJobLevelException.setChildExceptions(childException);
    mrJobLevelException.setLoggingEvent(analyzedLog.getException());
    mrJobLevelException.setType(HadoopException.HadoopExceptionType.MRJOB);
    mrJobLevelException.setId(mrJobId);
    return mrJobLevelException;
  }

  private HadoopException analyzeMRTask(String mrTaskId, String rawMRTaskLog) {
    HadoopException mrTaskLevelException = new HadoopException();
    MRTaskLogAnalyzer analyzedLog = new MRTaskLogAnalyzer(rawMRTaskLog);
    mrTaskLevelException.setLoggingEvent(analyzedLog.getException());
    mrTaskLevelException.setType(HadoopException.HadoopExceptionType.MRTASK);
    mrTaskLevelException.setId(mrTaskId);
    mrTaskLevelException.setChildExceptions(null);
    return mrTaskLevelException;
  }

  public HadoopException getExceptions() {
    return this._exception;
  }
}