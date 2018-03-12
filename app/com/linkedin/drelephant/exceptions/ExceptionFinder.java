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

import com.linkedin.drelephant.clients.WorkflowClient;
import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.InfoExtractor;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;

import javax.naming.AuthenticationException;

import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * ExceptionFinder class finds the exception along with the level of the exception. It takes the scheduler and the url of the workflow as
 * parameters.
 */
public class ExceptionFinder {
  private final Logger logger = Logger.getLogger(ExceptionFinder.class);
  private HadoopException _exception;
  private WorkflowClient _workflowClient;
  private MRClient _mrClient;

  private static String USERNAME = "username";
  private static String PRIVATE_KEY  = "private_key";
  private static String PASSWORD = "password";
  private static int SAMPLE_SIZE = 3;

  /**
   * Constructor for ExceptionFinder class
   * @param url The url of the workflow to analyze
   * @param scheduler The scheduler where the workflow was run.
   * @throws URISyntaxException
   * @throws MalformedURLException
   */
  public ExceptionFinder(String url, String scheduler)
      throws URISyntaxException, MalformedURLException, AuthenticationException, IOException {

    // create a new MRClient
    _mrClient = new MRClient();

    // create a new workflow client
    _workflowClient = InfoExtractor.getWorkflowClientInstance(scheduler, url);

    // get the schedulerData
    SchedulerConfigurationData schedulerData = InfoExtractor.getSchedulerData(scheduler);

    if(schedulerData==null) {
      throw new RuntimeException(String.format("Cannot find scheduler %s", scheduler));
    }

    if (schedulerData.getParamMap().containsKey("exception_enabled") == false
        || schedulerData.getParamMap().get("exception_enabled").equals("false")) {
      throw new RuntimeException(String.format("Scheduler %s is not configured for Exception fingerprinting ",
          scheduler));
    }

    if(!schedulerData.getParamMap().containsKey(USERNAME)) {
      throw new RuntimeException(String.format("Cannot find username for login"));
    }

    String username = schedulerData.getParamMap().get(USERNAME);

    if(schedulerData.getParamMap().containsKey(PRIVATE_KEY)) {
      _workflowClient.login(username, new File(schedulerData.getParamMap().get(PRIVATE_KEY)));
    } else if (schedulerData.getParamMap().containsKey(PASSWORD)) {
      _workflowClient.login(username, schedulerData.getParamMap().get(PASSWORD));
    } else {
      throw new RuntimeException("Neither private key nor password was specified");
    }
    _exception = analyzeFlow(url);
  }

  /**
   * Analyzes a Flow and returns a HadoopException object which captures all the exception in the flow.
   * @param execUrl the execution URL of the flow
   * @return HadoopException object which captures all the exceptions in the given Flow
   */
  private HadoopException analyzeFlow(final String execUrl) throws AuthenticationException, IOException {
    HadoopSecurity _hadoopSecurity = HadoopSecurity.getInstance();

    return _hadoopSecurity.doAs(new PrivilegedAction<HadoopException>() {
      @Override
      public HadoopException run() {
        HadoopException flowLevelException = new HadoopException();
        List<HadoopException> childExceptions = new ArrayList<HadoopException>();
        Map<String, String> jobIdStatus = _workflowClient.getJobsFromFlow();

        // Find exceptions in all the unsuccessful jobs of the workflow
        for (String unsuccessfulJobId : jobIdStatus.keySet()) {
          if (jobIdStatus.get(unsuccessfulJobId).toLowerCase().equals("failed")) {
            HadoopException jobLevelException = analyzeJob(unsuccessfulJobId);
            childExceptions.add(jobLevelException);
          }
        }

        flowLevelException.setType(HadoopException.HadoopExceptionType.FLOW);
        flowLevelException.setId(execUrl);
        flowLevelException.setLoggingEvent(null); // No flow level exception
        flowLevelException.setChildExceptions(childExceptions);
        return flowLevelException;
      }
    });
 }

  /**
   * Given a failed Job, this method analyzes the job and returns a HadoopException object which captures all the exception in the given job.
   * @param jobId The job execution id/url, specific to the scheduler
   * @return HadoopException object which captures all the exceptions in the given job
   */
  private HadoopException analyzeJob(String jobId) {
    HadoopException jobLevelException = new HadoopException();
    List<HadoopException> childExceptions = new ArrayList<HadoopException>();

    _workflowClient.analyzeJob(jobId);

    // get the set of all the yarn jobs from workflowClient
    Set<String> yarnJobIds = _workflowClient.getYarnApplicationsFromJob(jobId);

    for (String mrJobId : yarnJobIds) {
      //To do: Check if mr job logs are there or not in job history server
      String rawMRJobLog = _mrClient.getMRJobLog(mrJobId);
      if (rawMRJobLog != null && !rawMRJobLog.isEmpty()) { // null for log not found and empty for successful mr jobs
        //To do: rawMRJob is empty for successful mr jobs but this is not a good way to figure out whether a job failed
        // or succeeded, do this using the state field in rest api
        HadoopException mrJobLevelException = analyzeMRJob(mrJobId, rawMRJobLog);
        childExceptions.add(mrJobLevelException);
      }
    }

    if (_workflowClient.getJobState(jobId) == JobState.MRFAIL) {
      jobLevelException.setType(HadoopException.HadoopExceptionType.MR);
      jobLevelException.setLoggingEvent(_workflowClient.getJobException(jobId));
      //LoggingEvent is set only for the case if mr logs could not be found in job history server and childException is
      // empty
      jobLevelException.setChildExceptions(childExceptions);
    } else if (_workflowClient.getJobState(jobId) == JobState.SCHEDULERFAIL) {
      jobLevelException.setType(HadoopException.HadoopExceptionType.SCHEDULER);
      jobLevelException.setLoggingEvent(_workflowClient.getJobException(jobId));
      jobLevelException.setChildExceptions(null);
    } else if (_workflowClient.getJobState(jobId) == JobState.SCRIPTFAIL) {
      jobLevelException.setType(HadoopException.HadoopExceptionType.SCRIPT);
      jobLevelException.setLoggingEvent(_workflowClient.getJobException(jobId));
      jobLevelException.setChildExceptions(null);
    } else if (_workflowClient.getJobState(jobId) == JobState.KILLED) {
      jobLevelException.setType(HadoopException.HadoopExceptionType.KILL);
      jobLevelException.setLoggingEvent(null);
      jobLevelException.setChildExceptions(null);
    }
    jobLevelException.setId(jobId);
    return jobLevelException;
  }

  /**
   * Given a failed MR Job id and diagnostics of the job, this method analyzes it and returns a HadoopException object which captures all the exception in the given MR Job.
   * @param mrJobId Mapreduce job id
   * @param rawMRJoblog Diagnostics of the mapreduce job in a string
   * @return HadoopException object which captures all the exceptions in the given Mapreduce job
   */
  private HadoopException analyzeMRJob(String mrJobId, String rawMRJoblog) {
    // This method is called only for unsuccessful MR jobs
    HadoopException mrJobLevelException = new HadoopException();
    List<HadoopException> childExceptions = new ArrayList<HadoopException>();
    MRJobLogAnalyzer analyzedLog = new MRJobLogAnalyzer(rawMRJoblog);
    Set<String> failedMRTaskIds = analyzedLog.getFailedSubEvents();

    // sampling of tasks
    int samplingSize = SAMPLE_SIZE;
    for (String failedMRTaskId : failedMRTaskIds) {
      if(samplingSize<=0) {
        break;
      }
      String rawMRTaskLog = _mrClient.getMRTaskLog(mrJobId, failedMRTaskId);
      HadoopException mrTaskLevelException = analyzeMRTask(failedMRTaskId, rawMRTaskLog);
      childExceptions.add(mrTaskLevelException);

      samplingSize--;
    }

    mrJobLevelException.setChildExceptions(childExceptions);
    mrJobLevelException.setLoggingEvent(analyzedLog.getException());
    mrJobLevelException.setType(HadoopException.HadoopExceptionType.MRJOB);
    mrJobLevelException.setId(mrJobId);
    return mrJobLevelException;
  }

  /**
   * Given a failed MR Task id and diagnostics of the task, this method analyzes it and returns a HadoopException object which captures all the exception in the given MR task.
   * @param mrTaskId The task id of the map reduce job
   * @param rawMRTaskLog Raw map-reduce log
   * @return HadoopException object which captures all the exceptions in the given Mapreduce task
   */
  private HadoopException analyzeMRTask(String mrTaskId, String rawMRTaskLog) {
    HadoopException mrTaskLevelException = new HadoopException();
    MRTaskLogAnalyzer analyzedLog = new MRTaskLogAnalyzer(rawMRTaskLog);
    mrTaskLevelException.setLoggingEvent(analyzedLog.getException());
    mrTaskLevelException.setType(HadoopException.HadoopExceptionType.MRTASK);
    mrTaskLevelException.setId(mrTaskId);
    mrTaskLevelException.setChildExceptions(null);
    return mrTaskLevelException;
  }

  /**
   * Returns the Hadoop Exception object
   * @return Returns the Hadoop Exception object
   */
  public HadoopException getExceptions() {
    return this._exception;
  }
}
