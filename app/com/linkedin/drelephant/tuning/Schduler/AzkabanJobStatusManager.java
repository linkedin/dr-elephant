package com.linkedin.drelephant.tuning.Schduler;

import com.linkedin.drelephant.clients.azkaban.AzkabanJobStatusUtil;
import com.linkedin.drelephant.tuning.AbstractJobStatusManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


public class AzkabanJobStatusManager extends AbstractJobStatusManager {

  private final Logger logger = Logger.getLogger(getClass());
  private AzkabanJobStatusUtil _azkabanJobStatusUtil;


  public enum AzkabanJobStatus {
    FAILED, CANCELLED, KILLED, SUCCEEDED, SKIPPED
  }

  protected List<TuningJobExecutionParamSet> detectJobsExecutionInProgress() {
    logger.info("Fetching the executions which are in progress");
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
        TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobExecution)
            .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet)
            .where()
            .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.IN_PROGRESS)
            .findList();


    logger.info("Number of executions which are in progress: " + tuningJobExecutionParamSets.size());
    return tuningJobExecutionParamSets;
  }

  @Override
  protected Boolean analyzeCompletedJobsExecution(List<TuningJobExecutionParamSet> inProgressExecutionParamSet) {
    logger.info("Fetching the list of executions completed since last iteration");
    List<JobExecution> completedExecutions = new ArrayList<JobExecution>();
    try {
      for (TuningJobExecutionParamSet tuningJobExecutionParamSet : inProgressExecutionParamSet) {
        JobSuggestedParamSet jobSuggestedParamSet = tuningJobExecutionParamSet.jobSuggestedParamSet;
        JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
        logger.info("Checking current status of started execution: " + jobExecution.jobExecId);
        assignAzkabanJobStatusUtil();
        analyzeJobExecution(jobExecution,jobSuggestedParamSet);
      }
    } catch (Exception e) {
      logger.error("Error in fetching list of completed executions", e);
      e.printStackTrace();
    }
    logger.info("Number of executions completed since last iteration: " + completedExecutions.size());
    return true;
  }

  private void assignAzkabanJobStatusUtil() {
    if (_azkabanJobStatusUtil == null) {
      logger.info("Initializing  AzkabanJobStatusUtil");
      _azkabanJobStatusUtil = new AzkabanJobStatusUtil();
    }
  }

  private void analyzeJobExecution(JobExecution jobExecution,JobSuggestedParamSet jobSuggestedParamSet){
    try {
      Map<String, String> jobStatus = _azkabanJobStatusUtil.getJobsFromFlow(jobExecution.flowExecution.flowExecId);
      if (jobStatus != null) {
        for (Map.Entry<String, String> job : jobStatus.entrySet()) {
          logger.info("Job Found:" + job.getKey() + ". Status: " + job.getValue());
          if (job.getKey().equals(jobExecution.job.jobName)) {
            updateJobExecutionMetrics(job, jobSuggestedParamSet, jobExecution);
          }
        }
      } else {
        logger.info("No jobs found for flow execution: " + jobExecution.flowExecution.flowExecId);
      }
    } catch (Exception e) {
      logger.error("Error in checking status of execution: " + jobExecution.jobExecId, e);
    }
  }

  private void updateJobExecutionMetrics(Map.Entry<String, String> job, JobSuggestedParamSet jobSuggestedParamSet,
      JobExecution jobExecution) {
    if (job.getValue().equals(AzkabanJobStatus.FAILED.toString())) {
      if (jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.SENT)) {
        jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.EXECUTED;
      }
      jobExecution.executionState = JobExecution.ExecutionState.FAILED;
    } else if (job.getValue().equals(AzkabanJobStatus.SUCCEEDED.toString())) {
      if (jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.SENT)) {
        jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.EXECUTED;
      }
      jobExecution.executionState = JobExecution.ExecutionState.SUCCEEDED;
    } else if (job.getValue().equals(AzkabanJobStatus.CANCELLED.toString()) || job.getValue()
        .equals(AzkabanJobStatus.KILLED.toString()) || job.getValue()
        .equals(AzkabanJobStatus.SKIPPED.toString())) {
      if (jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.SENT)) {
        jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.EXECUTED;
      }
      jobExecution.executionState = JobExecution.ExecutionState.CANCELLED;
    }
  }

  @Override
  public String getManagerName() {
    return "AzkabanJobStatusManager";
  }

}
