package com.linkedin.drelephant.tuning;

import controllers.AutoTuningMetricsController;
import java.util.List;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


/**
 * Abstract Job Status Manager provides the functionality to check the job status of the job. This should have implementation specific to schduler
 */
public abstract class AbstractJobStatusManager implements Manager {
  private final Logger logger = Logger.getLogger(getClass());

  /**
   * This method find out the completed execution of the job and update the database.
   * This method is schduler dependent.
   * @param inProgressExecutionParamSet : Jobs for which status have to find out.
   * @return
   */
  protected abstract boolean analyzeCompletedJobsExecution(
      List<TuningJobExecutionParamSet> inProgressExecutionParamSet);

  /**
   * This method detect jobs whose execution state in job execution table is in in progress
   * @return List of tuning jobs whose status is in progress.
   */
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

  /**
   *
   * @param jobs : It takes jobs whose state is changes from execution to completed and also paramset w
   *             whose status is changed to executed.
   * @return : If the database update successfully then it return true other false
   */
  protected boolean updateDataBase(List<TuningJobExecutionParamSet> jobs) {
    try {
      for (TuningJobExecutionParamSet job : jobs) {
        JobSuggestedParamSet jobSuggestedParamSet = job.jobSuggestedParamSet;
        JobExecution jobExecution = job.jobExecution;
        if (isJobCompleted(jobExecution)) {
          jobExecution.update();
          jobSuggestedParamSet.update();
          logger.info("Execution " + jobExecution.jobExecId + " is completed");
        } else {
          logger.info("Execution " + jobExecution.jobExecId + " is still in running state");
        }
      }
    } catch (Exception e) {
      logger.info("Exception occur while updating database " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   *
   * @param jobExecution : Take jobExecution as input
   * @return true if the job execution is not in progress.
   */
  private Boolean isJobCompleted(JobExecution jobExecution) {
    if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED) || jobExecution.executionState.equals(
        JobExecution.ExecutionState.FAILED) || jobExecution.executionState.equals(
        JobExecution.ExecutionState.CANCELLED)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   *
   * @param completedJobs : Take jobs whose execution is completed. Update ingraph metrics
   * @return true if update is successfull else false.
   */
  protected boolean updateMetrics(List<TuningJobExecutionParamSet> completedJobs) {
    try {
      for (TuningJobExecutionParamSet completedJob : completedJobs) {
        JobExecution jobExecution = completedJob.jobExecution;
        if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
          AutoTuningMetricsController.markSuccessfulJobs();
        } else if (jobExecution.executionState.equals(JobExecution.ExecutionState.FAILED)) {
          AutoTuningMetricsController.markFailedJobs();
        }
      }
    } catch (Exception e) {
      logger.info(" Exception while updating Metrics to ingraph " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * 1) Get the jobs for which Status need to be queried from Azkaban
   * 2) Queried the Azkaban and get current status
   * 3) Update the data base if the execution is completed
   * 4) Update the metrics
   * @return true if method executes successfully  . Otherwise false
   */
  @Override
  public final boolean execute() {
    logger.info("Executing Job Status Manager");
    boolean calculateCompletedJobExDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSet = detectJobsExecutionInProgress();
    if (tuningJobExecutionParamSet != null && tuningJobExecutionParamSet.size() >= 1) {
      logger.info("Calculating  Completed Jobs");
      calculateCompletedJobExDone = analyzeCompletedJobsExecution(tuningJobExecutionParamSet);
    }
    if (calculateCompletedJobExDone) {
      logger.info("Updating Database");
      databaseUpdateDone = updateDataBase(tuningJobExecutionParamSet);
    }
    if (databaseUpdateDone) {
      logger.info("Updating Metrics");
      updateMetricsDone = updateMetrics(tuningJobExecutionParamSet);
    }
    logger.info("Baseline Done");
    return updateMetricsDone;
  }
}
