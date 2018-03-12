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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.TuningJobExecution;
import models.TuningJobExecution.ParamSetStatus;

import org.apache.log4j.Logger;

import controllers.AutoTuningMetricsController;
import play.libs.Json;


/**
 * This class pools the scheduler for completion status of execution and updates the database with current status
 * of the job.
 */
public abstract class JobCompleteDetector {
  private static final Logger logger = Logger.getLogger(JobCompleteDetector.class);

  /**
   * Updates the status of completed executions
   * @return List of completed executions
   * @throws MalformedURLException
   * @throws URISyntaxException
   */
  public List<TuningJobExecution> updateCompletedExecutions() throws MalformedURLException, URISyntaxException {
    logger.info("Checking execution status");
    List<TuningJobExecution> runningExecutions = getStartedExecutions();
    List<TuningJobExecution> completedExecutions = getCompletedExecutions(runningExecutions);
    updateExecutionStatus(completedExecutions);
    updateMetrics(completedExecutions);
    logger.info("Finished updating execution status");
    return completedExecutions;
  }

  /**
   * This method is for updating metrics for auto tuning monitoring for job completion daemon
   * @param completedExecutions
   */
  private void updateMetrics(List<TuningJobExecution> completedExecutions) {
    for (TuningJobExecution tuningJobExecution : completedExecutions) {
      if (tuningJobExecution.paramSetState.equals(ParamSetStatus.EXECUTED)) {
        if (tuningJobExecution.jobExecution.executionState.equals(ExecutionState.SUCCEEDED)) {
          AutoTuningMetricsController.markSuccessfulJobs();
        } else if (tuningJobExecution.jobExecution.executionState.equals(ExecutionState.FAILED)) {
          AutoTuningMetricsController.markFailedJobs();
        }
      }
    }
  }

  /**
   * Returns the list of executions which have already received param suggestion
   * @return JobExecution list
   */
  private List<TuningJobExecution> getStartedExecutions() {
    logger.info("Fetching the executions which were running");
    List<TuningJobExecution> tuningJobExecutionList = new ArrayList<TuningJobExecution>();
    try {
      tuningJobExecutionList = TuningJobExecution.find.select("*")
          .where()
          .eq(TuningJobExecution.TABLE.paramSetState, ParamSetStatus.SENT)
          .findList();
    } catch (NullPointerException e) {
      logger.info("None of the executions were running ", e);
    }
    logger.info("Number of executions which were in running state: " + tuningJobExecutionList.size());
    return tuningJobExecutionList;
  }

  /**
   * Returns the list of completed executions.
   * @param jobExecutions Started Execution list
   * @return List of completed executions
   * @throws MalformedURLException
   * @throws URISyntaxException
   */
  protected abstract List<TuningJobExecution> getCompletedExecutions(List<TuningJobExecution> jobExecutions)
      throws MalformedURLException, URISyntaxException;

  /**
   * Updates the job execution status
   * @param jobExecutions JobExecution list
   * @return Update status
   */
  private void updateExecutionStatus(List<TuningJobExecution> jobExecutions) {
    logger.info("Updating status of executions completed since last iteration");
    for (TuningJobExecution tuningJobExecution : jobExecutions) {
      JobExecution jobExecution = tuningJobExecution.jobExecution;
      logger.info("Updating execution status to EXECUTED for the execution: " + jobExecution.jobExecId);
      jobExecution.update();
      tuningJobExecution.update();
    }
  }
}
