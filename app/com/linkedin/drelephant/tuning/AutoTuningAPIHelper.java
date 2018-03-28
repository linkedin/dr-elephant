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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.FlowDefinition;
import models.FlowExecution;
import models.JobDefinition;
import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecution;
import models.TuningJobExecution.ParamSetStatus;
import models.TuningParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class processes the API requests and returns param suggestion as response
 */
public class AutoTuningAPIHelper {

  private static final Logger logger = Logger.getLogger(AutoTuningAPIHelper.class);

  public static final String ALLOWED_MAX_RESOURCE_USAGE_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_resource_usage_percent";

  public static final String ALLOWED_MAX_EXECUTION_TIME_PERCENT_DEFAULT =
      "autotuning.default.allowed_max_execution_time_percent";

  /**
   * This method creates a job execution with default parameters. This is required when there is no parameters set
   * remains for suggestion.
   * @param tuningJobDefinition Job definition
   * @return
   */
  private TuningJobExecution createDefaultJobExecution(TuningJobDefinition tuningJobDefinition) {
    logger.info("Creating an execution with default parameter values for job: " + tuningJobDefinition.job.jobDefId);

    //Get default execution from DB and clone that to create a new default execution
    TuningJobExecution tuningJobExecutionDefault = TuningJobExecution.find.select("*")
        .where()
        .eq(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.job + "." + JobDefinition.TABLE.id,
            tuningJobDefinition.job.id)
        .eq(TuningJobExecution.TABLE.isDefaultExecution, true)
        .setMaxRows(1)
        .findUnique();

    TuningJobExecution tuningJobExecution = new TuningJobExecution();
    JobExecution jobExecution = new JobExecution();
    jobExecution.id = 0L;
    jobExecution.job = tuningJobExecutionDefault.jobExecution.job;
    jobExecution.executionState = ExecutionState.NOT_STARTED;
    jobExecution.save();

    tuningJobExecution.jobExecution = jobExecution;
    tuningJobExecution.isDefaultExecution = tuningJobExecutionDefault.isDefaultExecution;
    tuningJobExecution.tuningAlgorithm = tuningJobExecutionDefault.tuningAlgorithm;
    tuningJobExecution.paramSetState = ParamSetStatus.CREATED;
    tuningJobExecution.save();

    logger.debug("Execution with default parameter created with execution id: " + tuningJobExecution.jobExecution.id);

    List<JobSuggestedParamValue> jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
        .eq(JobSuggestedParamValue.TABLE.jobExecution + "." + JobExecution.TABLE.id,
            tuningJobExecutionDefault.jobExecution.id)
        .findList();

    //Save default parameters corresponding to new default execution
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      JobSuggestedParamValue jobSuggestedParamValue1 = new JobSuggestedParamValue();
      jobSuggestedParamValue1.id = 0;
      jobSuggestedParamValue1.jobExecution = jobExecution;
      jobSuggestedParamValue1.paramValue = jobSuggestedParamValue.paramValue;
      jobSuggestedParamValue1.tuningParameter = jobSuggestedParamValue.tuningParameter;
      jobSuggestedParamValue1.save();
    }

    tuningJobExecution = TuningJobExecution.find.select("*")
        .where()
        .eq(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.id, tuningJobExecution.jobExecution.id)
        .setMaxRows(1)
        .findUnique();

    return tuningJobExecution;
  }

  /**
   * Set default values for allowed max execution time and resource usage in case not provided in API call
   * And set tuning algorithm based on value of job type and optimization metric
   * @param tuningInput
   */
  public void setDefaultValue(TuningInput tuningInput) {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    if (tuningInput.getAllowedMaxExecutionTimePercent() == null) {
      Double allowedMaxExecutionTimePercent =
          new Double(Utils.getNonNegativeInt(configuration, "autotuning.default.allowed_max_execution_time_percent",
              150));
      tuningInput.setAllowedMaxExecutionTimePercent(allowedMaxExecutionTimePercent);
    }
    if (tuningInput.getAllowedMaxResourceUsagePercent() == null) {
      Double allowedMaxResourceUsagePercent =
          new Double(Utils.getNonNegativeInt(configuration, "autotuning.default.allowed_max_resource_usage_percent",
              150));
      tuningInput.setAllowedMaxResourceUsagePercent(allowedMaxResourceUsagePercent);
    }

    //Todo: Handle algorithm version later
    TuningAlgorithm tuningAlgorithm = TuningAlgorithm.find.select("*")
        .where()
        .eq(TuningAlgorithm.TABLE.jobType, tuningInput.getJobType())
        .eq(TuningAlgorithm.TABLE.optimizationMetric, tuningInput.getOptimizationMetric())
        .findUnique();
    tuningInput.setTuningAlgorithm(tuningAlgorithm);
  }

  /**
   * Handles the api request and returns param suggestions as response
   * @param tuningInput Rest api parameters
   * @return Parameter Suggestion
   */
  public Map<String, Double> getCurrentRunParameters(TuningInput tuningInput) {
    logger.info("Parameter suggestion request for execution: " + tuningInput.getJobExecId());
    setDefaultValue(tuningInput);

    String jobDefId = tuningInput.getJobDefId();

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId, jobDefId)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findUnique();

    //If new job for tuning, update db with new job configuration
    if (tuningJobDefinition == null) {
      logger.debug("New job encountered for tuning");
      AutoTuningMetricsController.markNewAutoTuningJob();
      tuningJobDefinition = addNewJobForTuning(tuningInput);
    }

    logger.debug("Finding parameter suggestion for job: " + tuningJobDefinition.job.jobName);
    TuningJobExecution tuningJobExecution = TuningJobExecution.find.select("*")
        .fetch(TuningJobExecution.TABLE.jobExecution, "*")
        .fetch(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.job, "*")
        .where()
        .eq(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.job + "." + JobDefinition.TABLE.id,
            tuningJobDefinition.job.id)
        .eq(TuningJobExecution.TABLE.paramSetState, ParamSetStatus.CREATED)
        .order()
        .asc(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.createdTs)
        .setMaxRows(1)
        .findUnique();

    //If no new parameter set for suggestion, create a new suggestion with default parameter
    if (tuningJobExecution == null) {
      logger.info(
          "Returning default parameters as no parameter suggestion found for job: " + tuningJobDefinition.job.jobName);
      AutoTuningMetricsController.markParamSetNotFound();
      tuningJobExecution = createDefaultJobExecution(tuningJobDefinition);
    }

    logger.debug("Finding parameters corresponding to execution id: " + tuningJobExecution.jobExecution.id);
    List<JobSuggestedParamValue> jobSuggestedParamValues = JobSuggestedParamValue.find.where()
        .eq(JobSuggestedParamValue.TABLE.jobExecution + "." + JobExecution.TABLE.id, tuningJobExecution.jobExecution.id)
        .findList();

    logger.debug("Number of output parameters : " + jobSuggestedParamValues.size());
    Map<String, Double> paramValues = new HashMap<String, Double>();

    if (jobSuggestedParamValues != null) {
      for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
        logger.debug("Param Name is " + jobSuggestedParamValue.tuningParameter.paramName + " And value is "
            + jobSuggestedParamValue.paramValue);
        paramValues.put(jobSuggestedParamValue.tuningParameter.paramName, jobSuggestedParamValue.paramValue);
      }
    }

    updateJobExecutionParameter(tuningJobExecution, tuningInput);

    logger.info("Finishing getCurrentRunParameters");
    return paramValues;
  }

  /**
   *This is to update job execution with IN_PROGRESS and parameter set with IN_PROGRESS. Also update flow_exec_id
   *, flowExecURL, JobExecID and jobExecURL
   * @param tuningJobExecution
   * @param tuningInput
   */
  private void updateJobExecutionParameter(TuningJobExecution tuningJobExecution, TuningInput tuningInput) {

    FlowExecution flowExecution =
        FlowExecution.find.where().eq(FlowExecution.TABLE.flowExecId, tuningInput.getFlowExecId()).findUnique();

    if (flowExecution == null) {
      flowExecution = new FlowExecution();
      flowExecution.flowExecId = tuningInput.getFlowExecId();
      flowExecution.flowExecUrl = tuningInput.getFlowExecUrl();
      flowExecution.flowDefinition = tuningJobExecution.jobExecution.job.flowDefinition;
      flowExecution.save();
    }

    JobExecution jobExecution = tuningJobExecution.jobExecution;
    jobExecution.jobExecId = tuningInput.getJobExecId();
    jobExecution.jobExecUrl = tuningInput.getJobExecUrl();
    jobExecution.executionState = ExecutionState.IN_PROGRESS;
    jobExecution.flowExecution = flowExecution;

    logger.debug("Saving job execution" + jobExecution.jobExecId);

    jobExecution.save();

    tuningJobExecution.jobExecution = jobExecution;
    tuningJobExecution.paramSetState = ParamSetStatus.SENT;
    tuningJobExecution.save();
  }

  /**
   * Add new job for tuning
   * @param tuningInput Tuning input parameters
   * @return Job
   */
  private TuningJobDefinition addNewJobForTuning(TuningInput tuningInput) {

    logger.info("Adding new job for tuning, job id: " + tuningInput.getJobDefId());

    JobDefinition job =
        JobDefinition.find.select("*").where().eq(JobDefinition.TABLE.jobDefId, tuningInput.getJobDefId()).findUnique();

    FlowDefinition flowDefinition =
        FlowDefinition.find.where().eq(FlowDefinition.TABLE.flowDefId, tuningInput.getFlowDefId()).findUnique();

    if (flowDefinition == null) {
      flowDefinition = new FlowDefinition();
      flowDefinition.flowDefId = tuningInput.getFlowDefId();
      flowDefinition.flowDefUrl = tuningInput.getFlowDefUrl();
      flowDefinition.save();
    }

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

    String flowExecId = tuningInput.getFlowExecId();
    String jobExecId = tuningInput.getJobExecId();
    String flowExecUrl = tuningInput.getFlowExecUrl();
    String jobExecUrl = tuningInput.getJobExecUrl();
    String client = tuningInput.getClient();
    String defaultParams = tuningInput.getDefaultParams();

    TuningJobDefinition tuningJobDefinition = new TuningJobDefinition();
    tuningJobDefinition.job = job;
    tuningJobDefinition.client = client;
    tuningJobDefinition.tuningAlgorithm = tuningInput.getTuningAlgorithm();
    tuningJobDefinition.tuningEnabled = 1;
    tuningJobDefinition.allowedMaxExecutionTimePercent = tuningInput.getAllowedMaxExecutionTimePercent();
    tuningJobDefinition.allowedMaxResourceUsagePercent = tuningInput.getAllowedMaxResourceUsagePercent();
    tuningJobDefinition.save();

    TuningJobExecution tuningJobExecution =
        insertDefaultJobExecution(job, flowExecId, jobExecId, flowExecUrl, jobExecUrl, flowDefinition,
            tuningInput.getTuningAlgorithm());
    insertDefaultParameters(tuningJobExecution.jobExecution, defaultParams);

    logger.info("Added job: " + tuningInput.getJobDefId() + " for tuning");
    return tuningJobDefinition;
  }

  /**
   * Inserts default job execution in database
   * @param job Job
   * @param flowExecId Flow execution id
   * @param jobExecId Job execution id
   * @param flowExecUrl Flow execution url
   * @param jobExecUrl Job execution url
   * @return default job execution
   */
  private TuningJobExecution insertDefaultJobExecution(JobDefinition job, String flowExecId, String jobExecId,
      String flowExecUrl, String jobExecUrl, FlowDefinition flowDefinition, TuningAlgorithm tuningAlgorithm) {
    logger.debug("Starting insertDefaultJobExecution");

    FlowExecution flowExecution =
        FlowExecution.find.where().eq(FlowExecution.TABLE.flowExecId, flowExecId).findUnique();

    if (flowExecution == null) {
      flowExecution = new FlowExecution();
      flowExecution.flowExecId = flowExecId;
      flowExecution.flowExecUrl = flowExecUrl;
      flowExecution.flowDefinition = flowDefinition;
      flowExecution.save();
    }

    JobExecution jobExecution = JobExecution.find.where().eq(JobExecution.TABLE.jobExecId, jobExecId).findUnique();

    if (jobExecution == null) {
      jobExecution = new JobExecution();
      jobExecution.job = job;
      jobExecution.executionState = ExecutionState.NOT_STARTED;
      jobExecution.jobExecId = jobExecId;
      jobExecution.jobExecUrl = jobExecUrl;
      jobExecution.flowExecution = flowExecution;
      jobExecution.save();
    }

    TuningJobExecution tuningJobExecution = new TuningJobExecution();
    tuningJobExecution.jobExecution = jobExecution;
    tuningJobExecution.tuningAlgorithm = tuningAlgorithm;
    tuningJobExecution.paramSetState = ParamSetStatus.CREATED;
    tuningJobExecution.isDefaultExecution = true;
    tuningJobExecution.save();

    logger.debug("Finishing insertDefaultJobExecution. Job Execution ID " + jobExecution.jobExecId);

    return tuningJobExecution;
  }

  /**
   * Inserts default execution parameters in database
   * @param jobExecution Job Execution
   * @param defaultParams Default parameters map as string
   */
  @SuppressWarnings("unchecked")
  private void insertDefaultParameters(JobExecution jobExecution, String defaultParams) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Double> paramValueMap = null;
    try {
      paramValueMap = (Map<String, Double>) mapper.readValue(defaultParams, Map.class);
    } catch (Exception e) {
      logger.error(e);
    }
    if (paramValueMap != null) {
      for (Map.Entry<String, Double> paramValue : paramValueMap.entrySet()) {
        insertExecutionParameter(jobExecution, paramValue.getKey(), paramValue.getValue());
      }
    } else {
      logger.warn("ParamValueMap is null ");
    }
  }

  /**
   * Inserts parameter of an execution in database
   * @param jobExecution Job execution
   * @param paramName Parameter name
   * @param paramValue Parameter value
   */
  private void insertExecutionParameter(JobExecution jobExecution, String paramName, Double paramValue) {
    logger.debug("Starting insertExecutionParameter");
    JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
    jobSuggestedParamValue.jobExecution = jobExecution;
    TuningParameter tuningParameter =
        TuningParameter.find.where().eq(TuningParameter.TABLE.paramName, paramName).findUnique();
    if (tuningParameter != null) {
      jobSuggestedParamValue.tuningParameter = tuningParameter;
      jobSuggestedParamValue.paramValue = paramValue;
      jobSuggestedParamValue.save();
      logger.debug(
          "Finishing insertDefaultJobExecution. Job Execution ID. Param ID " + jobSuggestedParamValue.tuningParameter.id
              + " Param Name: " + jobSuggestedParamValue.tuningParameter.paramName);
    } else {
      logger.warn("TuningAlgorithm param null " + paramName);
    }
  }
}
