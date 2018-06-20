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

package com.linkedin.drelephant.tuning;

import controllers.AutoTuningMetricsController;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.List;
import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


/**
 * This class pools the scheduler for completion status of execution and updates the database with current status
 * of the job.
 */
public abstract class JobCompleteDetector {
  private static final Logger logger = Logger.getLogger(JobCompleteDetector.class);

  /**
   * Updates the status of completed executions
   * @throws MalformedURLException MalformedURLException
   * @throws URISyntaxException URISyntaxException
   */
  public void updateCompletedExecutions() throws MalformedURLException, URISyntaxException {
    logger.info("Updating execution status");
    List<TuningJobExecutionParamSet> inProgressExecutionParamSet = getExecutionsInProgress();
    List<JobExecution> completedExecutions = getCompletedExecutions(inProgressExecutionParamSet);
    updateMetrics(completedExecutions);
    logger.info("Finished updating execution status");
  }

  /**
   * Updates metrics for auto tuning monitoring for job completion daemon
   * @param completedExecutions List completed job executions
   */
  private void updateMetrics(List<JobExecution> completedExecutions) {
    for (JobExecution jobExecution : completedExecutions) {
      if (jobExecution.executionState.equals(ExecutionState.SUCCEEDED)) {
        AutoTuningMetricsController.markSuccessfulJobs();
      } else if (jobExecution.executionState.equals(ExecutionState.FAILED)) {
        AutoTuningMetricsController.markFailedJobs();
      }
    }
  }

  /**
   * Returns the executions in progress
   * @return JobExecution list
   */
  private List<TuningJobExecutionParamSet> getExecutionsInProgress() {
    logger.info("Fetching the executions which are in progress");
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobExecution)
        .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet)
        .where()
        .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
            ExecutionState.IN_PROGRESS)
        .findList();
    logger.info("Number of executions which are in progress: " + tuningJobExecutionParamSets.size());
    return tuningJobExecutionParamSets;
  }

  /**
   * Returns the list of completed executions.
   * @param inProgressExecutionParamSet List of executions (with corresponding param set) in progress
   * @return List of completed executions
   * @throws MalformedURLException MalformedURLException
   * @throws URISyntaxException URISyntaxException
   */
  protected abstract List<JobExecution> getCompletedExecutions(
      List<TuningJobExecutionParamSet> inProgressExecutionParamSet) throws MalformedURLException, URISyntaxException;
}
