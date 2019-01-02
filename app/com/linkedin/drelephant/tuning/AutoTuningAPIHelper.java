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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.util.Utils;

import controllers.AutoTuningMetricsController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import models.FlowDefinition;
import models.FlowExecution;
import models.JobDefinition;
import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamSet.ParamSetStatus;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class processes the API requests and returns param suggestion as response
 */
public class AutoTuningAPIHelper {

  private static final String ALLOWED_MAX_RESOURCE_USAGE_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_resource_usage_percent";
  private static final String ALLOWED_MAX_EXECUTION_TIME_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_execution_time_percent";
  private static final Logger logger = Logger.getLogger(AutoTuningAPIHelper.class);

  /**
   * For a job, returns the best parameter set of the given job if it exists else  the default parameter set
   * @param jobDefId Sting JobDefId of the job
   * @return JobSuggestedParamSet the best parameter set of the given job if it exists else  the default parameter set
   */
  private JobSuggestedParamSet getBestParamSet(String jobDefId) {
    JobSuggestedParamSet jobSuggestedParamSetBestParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
        .setMaxRows(1)
        .findUnique();

    if (jobSuggestedParamSetBestParamSet == null) {
      jobSuggestedParamSetBestParamSet = JobSuggestedParamSet.find.select("*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.jobDefId, jobDefId)
          .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, true)
          .setMaxRows(1)
          .findUnique();
    }
    return jobSuggestedParamSetBestParamSet;
  }

  /**
   * Returns the param values corresponding to the given param set id
   * @param paramSetId Long parameter set id
   * @return List<JobSuggestedParamValue> list of parameters
   */
  private List<JobSuggestedParamValue> getParamSetValues(Long paramSetId) {
    List<JobSuggestedParamValue> jobSuggestedParamValues = JobSuggestedParamValue.find.where()
        .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id, paramSetId)
        .findList();
    return jobSuggestedParamValues;
  }

  /**
   * Sets the max allowed increase percentage for metrics: execution time and resource usage if not provided in API call
   * @param tuningInput TuningInput
   */
  private void setMaxAllowedMetricIncreasePercentage(TuningInput tuningInput) {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    if (tuningInput.getAllowedMaxExecutionTimePercent() == null) {
      Double allowedMaxExecutionTimePercent =
          new Double(Utils.getNonNegativeInt(configuration, ALLOWED_MAX_EXECUTION_TIME_PERCENT_DEFAULT, 150));
      tuningInput.setAllowedMaxExecutionTimePercent(allowedMaxExecutionTimePercent);
    }
    if (tuningInput.getAllowedMaxResourceUsagePercent() == null) {
      Double allowedMaxResourceUsagePercent =
          new Double(Utils.getNonNegativeInt(configuration, ALLOWED_MAX_RESOURCE_USAGE_PERCENT_DEFAULT, 150));
      tuningInput.setAllowedMaxResourceUsagePercent(allowedMaxResourceUsagePercent);
    }
  }

  /**
   * Sets the tuning algorithm based on the job type and optimization metric
   * @param tuningInput TuningInput for which tuning algorithm is to be set
   */
  private void setTuningAlgorithm(TuningInput tuningInput) throws IllegalArgumentException {
    //Todo: Handle algorithm version later
    TuningAlgorithm tuningAlgorithm = TuningAlgorithm.find.select("*")
        .where()
        .eq(TuningAlgorithm.TABLE.jobType, tuningInput.getJobType())
        .eq(TuningAlgorithm.TABLE.optimizationMetric, tuningInput.getOptimizationMetric())
        .findUnique();
    if (tuningAlgorithm == null) {
      throw new IllegalArgumentException(
          "Wrong job type or optimization metric. Job Type " + tuningInput.getJobType() + ". Optimization Metrics: "
              + tuningInput.getOptimizationMetric());
    }
    tuningInput.setTuningAlgorithm(tuningAlgorithm);
  }

  /**
   * Applies penalty to the param set corresponding to the given execution
   * @param jobExecId String job execution id/url of the execution whose parameter set has to be penalized
   * Assumption: Best param set will never be penalized
   */
  private void applyPenalty(String jobExecId) {
    Integer penaltyConstant = 3;
    logger.info("Execution " + jobExecId + " failed/cancelled. Applying penalty");

    TuningJobExecutionParamSet tuningJobExecutionParamSet = TuningJobExecutionParamSet.find.where()
        .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.jobExecId, jobExecId)
        .setMaxRows(1)
        .findUnique();

    JobSuggestedParamSet jobSuggestedParamSet = tuningJobExecutionParamSet.jobSuggestedParamSet;
    JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
    JobDefinition jobDefinition = jobExecution.job;

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .findUnique();
    Double averageResourceUsagePerGBInput =
        tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
    Double maxDesiredResourceUsagePerGBInput =
        averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;

    jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
    jobSuggestedParamSet.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
    jobSuggestedParamSet.fitnessJobExecution = jobExecution;
    jobSuggestedParamSet.update();

    jobExecution.resourceUsage = 0D;
    jobExecution.executionTime = 0D;
    jobExecution.inputSizeInBytes = 1D;
    jobExecution.save();
  }

  /**
   * Returns flow definition corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput TuningInput containing the flow definition id corresponding to which flow definition
   *                    is to be returned
   * @return FlowDefinition flow definition
   */
  private FlowDefinition getFlowDefinition(TuningInput tuningInput) {
    FlowDefinition flowDefinition =
        FlowDefinition.find.where().eq(FlowDefinition.TABLE.flowDefId, tuningInput.getFlowDefId()).findUnique();
    if (flowDefinition == null) {
      flowDefinition = new FlowDefinition();
      flowDefinition.flowDefId = tuningInput.getFlowDefId();
      flowDefinition.flowDefUrl = tuningInput.getFlowDefUrl();
      flowDefinition.save();
    }
    return flowDefinition;
  }

  /**
   * Returns flow execution corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput TuningInput containing the flow execution id corresponding to which flow execution
   *                    is to be returned
   * @return FlowExecution flow execution
   */
  private FlowExecution getFlowExecution(TuningInput tuningInput) {
    FlowExecution flowExecution =
        FlowExecution.find.where().eq(FlowExecution.TABLE.flowExecId, tuningInput.getFlowExecId()).findUnique();

    if (flowExecution == null) {
      flowExecution = new FlowExecution();
      flowExecution.flowExecId = tuningInput.getFlowExecId();
      flowExecution.flowExecUrl = tuningInput.getFlowExecUrl();
      flowExecution.flowDefinition = getFlowDefinition(tuningInput);
      flowExecution.save();
    }
    return flowExecution;
  }

  /**
   * Adds new job for tuning
   * @param tuningInput Tuning input parameters
   * @return Job
   */
  private TuningJobDefinition addNewJobForTuning(TuningInput tuningInput) {
    logger.info("Adding new job for tuning, job id: " + tuningInput.getJobDefId());
    FlowDefinition flowDefinition = getFlowDefinition(tuningInput);
    JobDefinition job =
        JobDefinition.find.select("*").where().eq(JobDefinition.TABLE.jobDefId, tuningInput.getJobDefId()).findUnique();

    if (job == null) {
      job = new JobDefinition();
      job.jobDefId = tuningInput.getJobDefId();
      job.scheduler = tuningInput.getScheduler();
      job.username = tuningInput.getUserName();
      job.jobName = tuningInput.getJobName();
      job.jobDefUrl = tuningInput.getJobDefUrl();
      job.flowDefinition = flowDefinition;
      job.save();
    }

    String client = tuningInput.getClient();
    Map<String, Double> defaultParams = null;
    try {
      defaultParams = tuningInput.getDefaultParams();
    } catch (IOException e) {
      logger.error("Error in getting default parameters from request. ", e);
    }
    TuningJobDefinition tuningJobDefinition = new TuningJobDefinition();
    tuningJobDefinition.job = job;
    tuningJobDefinition.client = client;
    tuningJobDefinition.tuningAlgorithm = tuningInput.getTuningAlgorithm();
    tuningJobDefinition.tuningEnabled = true;
    tuningJobDefinition.allowedMaxExecutionTimePercent = tuningInput.getAllowedMaxExecutionTimePercent();
    tuningJobDefinition.allowedMaxResourceUsagePercent = tuningInput.getAllowedMaxResourceUsagePercent();
    tuningJobDefinition.save();

    insertParamSet(job, tuningInput.getTuningAlgorithm(), defaultParams);

    logger.info("Added job: " + tuningInput.getJobDefId() + " for tuning");
    return tuningJobDefinition;
  }

  /**
   * Returns the job definition corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput Tuning Input corresponding to which job definition is to be returned
   * @return JobDefinition corresponding to the given tuning input
   */
  private JobDefinition getJobDefinition(TuningInput tuningInput) {

    String jobDefId = tuningInput.getJobDefId();

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .setMaxRows(1)
        .orderBy(TuningJobDefinition.TABLE.createdTs + " desc")
        .findUnique();

    if (tuningJobDefinition == null) {
      // Job new to tuning
      logger.debug("Registering job: " + tuningInput.getJobName() + " for auto tuning tuning");
      AutoTuningMetricsController.markNewAutoTuningJob();
      tuningJobDefinition = addNewJobForTuning(tuningInput);
    }
    return tuningJobDefinition.job;
  }

  /**
   * Creates a new job execution entry corresponding to the given tuning input
   * @param tuningInput Input corresponding to which job execution is to be created
   * @return JobExecution: the newly created job execution
   */
  private JobExecution addNewExecution(TuningInput tuningInput) {
    JobDefinition jobDefinition = getJobDefinition(tuningInput);
    FlowExecution flowExecution = getFlowExecution(tuningInput);

    JobExecution jobExecution = new JobExecution();
    jobExecution.jobExecId = tuningInput.getJobExecId();
    jobExecution.jobExecUrl = tuningInput.getJobExecUrl();
    jobExecution.job = jobDefinition;
    jobExecution.executionState = ExecutionState.IN_PROGRESS;
    jobExecution.flowExecution = flowExecution;
    jobExecution.save();
    return jobExecution;
  }

  /**
   * Returns the job execution corresponding to the given tuning input if it exists, else creates one and returns it
   * @param tuningInput Tuning Input corresponding to which job execution is to be returned
   * @return JobExecution corresponding to the given tuning input
   */
  private JobExecution getJobExecution(TuningInput tuningInput) {

    JobExecution jobExecution = JobExecution.find.select("*")
        .fetch(JobExecution.TABLE.job, "*")
        .where()
        .eq(JobExecution.TABLE.jobExecId, tuningInput.getJobExecId())
        .findUnique();

    if (jobExecution == null) {
      jobExecution = addNewExecution(tuningInput);
    }
    return jobExecution;
  }

  /**
   * Handles the api request and returns param suggestions as response
   * @param tuningInput Rest api parameters
   * @return Parameter Suggestion
   */
  public Map<String, Double> getCurrentRunParameters(TuningInput tuningInput) throws Exception {
    logger.info("Parameter set request received from execution: " + tuningInput.getJobExecId());

    if (tuningInput.getAllowedMaxExecutionTimePercent() == null
        || tuningInput.getAllowedMaxResourceUsagePercent() == null) {
      setMaxAllowedMetricIncreasePercentage(tuningInput);
    }
    setTuningAlgorithm(tuningInput);

    JobSuggestedParamSet jobSuggestedParamSet;
    JobExecution jobExecution = getJobExecution(tuningInput);

    if (tuningInput.getRetry()) {
      applyPenalty(tuningInput.getJobExecId());
      jobSuggestedParamSet = getBestParamSet(tuningInput.getJobDefId());
    } else {
      logger.debug("Finding parameter suggestion for job: " + jobExecution.job.jobName);
      jobSuggestedParamSet = getNewSuggestedParamSet(jobExecution.job);
      markParameterSetSent(jobSuggestedParamSet);
    }

    addNewTuningJobExecutionParamSet(jobSuggestedParamSet, jobExecution);

    List<JobSuggestedParamValue> jobSuggestedParamValues = getParamSetValues(jobSuggestedParamSet.id);
    logger.debug("Number of output parameters for execution " + tuningInput.getJobExecId() + " = "
        + jobSuggestedParamValues.size());
    logger.info("Finishing getCurrentRunParameters");
    return jobSuggestedParamValueListToMap(jobSuggestedParamValues);
  }

  /**
   * Adds a new entry to the "tuning_job_execution_param_set"  for the given param set and job execution
   * @param jobSuggestedParamSet JobSuggestedParamSet: param set
   * @param jobExecution JobExecution
   */
  private void addNewTuningJobExecutionParamSet(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution) {
    TuningJobExecutionParamSet tuningJobExecutionParamSet = new TuningJobExecutionParamSet();
    tuningJobExecutionParamSet.jobSuggestedParamSet = jobSuggestedParamSet;
    tuningJobExecutionParamSet.jobExecution = jobExecution;

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobExecution.job.id)
        .order()
        .desc(TuningJobDefinition.TABLE.createdTs)
        .setMaxRows(1)
        .findUnique();
    tuningJobExecutionParamSet.tuningEnabled = tuningJobDefinition.tuningEnabled;
    tuningJobExecutionParamSet.save();
  }

  /**
   * Returns a parameter set in "CREATED" state corresponding to the given job definition if it exists, else returns
   * the best parameter set
   * @param jobDefinition jobDefinition for which param set is to be returned
   * @return JobSuggestedParamSet corresponding to the given job definition
   */
  private JobSuggestedParamSet getNewSuggestedParamSet(JobDefinition jobDefinition) {
    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.paramSetState, ParamSetStatus.CREATED)
        .order()
        .asc(JobSuggestedParamSet.TABLE.id)
        .setMaxRows(1)
        .findUnique();

    if (jobSuggestedParamSet == null) {
      //No new parameter set exists, returning the best parameter set
      logger.info("Returning best parameter set as no parameter suggestion found for job: " + jobDefinition.jobName);
      AutoTuningMetricsController.markParamSetNotFound();
      jobSuggestedParamSet = getBestParamSet(jobDefinition.jobDefId);
    }
    return jobSuggestedParamSet;
  }

  /**
   * Returns the list of JobSuggestedParamValue as Map of String to Double
   * @param jobSuggestedParamValues List of JobSuggestedParamValue
   * @return Map of string to double containing the parameter name and corresponding value
   */
  private Map<String, Double> jobSuggestedParamValueListToMap(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    Map<String, Double> paramValues = new HashMap<String, Double>();
    if (jobSuggestedParamValues != null) {
      for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
        logger.debug("Param Name is " + jobSuggestedParamValue.tuningParameter.paramName + " And value is "
            + jobSuggestedParamValue.paramValue);
        paramValues.put(jobSuggestedParamValue.tuningParameter.paramName, jobSuggestedParamValue.paramValue);
      }
    }
    return paramValues;
  }

  /**
   *Updates parameter set state to SENT if it is in CREATED state
   * @param jobSuggestedParamSet JobSuggestedParamSet which is to be updated
   */
  private void markParameterSetSent(JobSuggestedParamSet jobSuggestedParamSet) {
    if (jobSuggestedParamSet.paramSetState.equals(ParamSetStatus.CREATED)) {
      logger.info("Marking paramSetID: " + jobSuggestedParamSet.id + " SENT");
      jobSuggestedParamSet.paramSetState = ParamSetStatus.SENT;
      jobSuggestedParamSet.save();
    }
  }

  /**
   * Inserts a parameter set in database
   * @param job Job
   */
  private void insertParamSet(JobDefinition job, TuningAlgorithm tuningAlgorithm, Map<String, Double> paramValueMap) {
    logger.debug("Inserting default parameter set for job: " + job.jobName);
    JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
    jobSuggestedParamSet.jobDefinition = job;
    jobSuggestedParamSet.tuningAlgorithm = tuningAlgorithm;
    jobSuggestedParamSet.paramSetState = ParamSetStatus.CREATED;
    jobSuggestedParamSet.isParamSetDefault = true;
    jobSuggestedParamSet.areConstraintsViolated = false;
    jobSuggestedParamSet.isParamSetBest = false;
    jobSuggestedParamSet.save();
    insertParameterValues(jobSuggestedParamSet, paramValueMap);
    logger.debug("Default parameter set inserted for job: " + job.jobName);
  }

  /**
   * Inserts parameter values in database
   * @param jobSuggestedParamSet Set of the parameters which is to be inserted
   * @param paramValueMap Map of parameter values as string
   */
  @SuppressWarnings("unchecked")
  private void insertParameterValues(JobSuggestedParamSet jobSuggestedParamSet, Map<String, Double> paramValueMap) {
    if (paramValueMap != null) {
      for (Map.Entry<String, Double> paramValue : paramValueMap.entrySet()) {
        insertParameterValue(jobSuggestedParamSet, paramValue.getKey(), paramValue.getValue());
      }
    } else {
      logger.warn("ParamValueMap is null ");
    }
  }

  /**
   * Inserts parameter value in database
   * @param jobSuggestedParamSet Parameter set to which the parameter belongs
   * @param paramName Parameter name
   * @param paramValue Parameter value
   */
  private void insertParameterValue(JobSuggestedParamSet jobSuggestedParamSet, String paramName, Double paramValue) {
    logger.debug("Starting insertParameterValue");
    JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
    jobSuggestedParamValue.jobSuggestedParamSet = jobSuggestedParamSet;
    TuningParameter tuningParameter =
        TuningParameter.find.where().eq(TuningParameter.TABLE.paramName, paramName).findUnique();
    if (tuningParameter != null) {
      jobSuggestedParamValue.tuningParameter = tuningParameter;
      jobSuggestedParamValue.paramValue = paramValue;
      jobSuggestedParamValue.save();
    } else {
      logger.warn("TuningAlgorithm param null " + paramName);
    }
  }
}
