package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.AbstractTuningTypeManager;
import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.Particle;
import com.linkedin.drelephant.tuning.TuningHelper;
import controllers.AutoTuningMetricsController;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSavedState;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;
import org.apache.commons.io.FileUtils;

import static java.lang.Math.*;

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TuningTypeManagerHBT extends AbstractTuningTypeManager {
  private final Logger logger = Logger.getLogger(getClass());

  private Map<String, Map<String, Double>> usageDataGlobal = null;

  public TuningTypeManagerHBT(ExecutionEngine executionEngine) {
    tuningType = "HBT";
    this._executionEngine = executionEngine;
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = _executionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        // .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    logger.info(
        " Number of Pending Jobs for parameter suggestion " + this._executionEngine + " " + pendingParamSetList.size());
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    List<TuningJobDefinition> totalJobs = _executionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();

    logger.info(" Number of Total Jobs " + this._executionEngine + " " + totalJobs.size());
    return totalJobs;
  }

  @Override
  protected void saveJobState(JobTuningInfo jobTuningInfo, JobDefinition job) {
    jobTuningInfo.setTunerState("{}");
  }

  @Override
  public JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo) {
    logger.info("Generating param set for job: " + jobTuningInfo.getTuningJob().jobName);
    String newTunedParameters = generateParamSet(jobTuningInfo.getParametersToTune(), jobTuningInfo.getTuningJob());
    jobTuningInfo.setTunerState(newTunedParameters);
    return jobTuningInfo;
  }

  private String generateParamSet(List<TuningParameter> tuningParameters, JobDefinition job) {
    JobExecution jobExecution = JobExecution.find.select("*")
        .where()
        .eq(JobExecution.TABLE.job, job)
        .order()
        .desc(JobExecution.TABLE.updatedTs)
        .setMaxRows(1)
        .findUnique();
    logger.info("Job Status " + jobExecution.executionState.name());
    if (jobExecution.executionState.name().equals(JobExecution.ExecutionState.IN_PROGRESS.name())
        || jobExecution.executionState.name().equals(JobExecution.ExecutionState.NOT_STARTED.name())) {
      logger.info(" Job is still running , cannot use for param generation ");
      return "";
    }

    List<AppResult> results = getAppResults(jobExecution);
    if (results == null) {
      logger.info(
          " Job is analyzing  , cannot use for param generation " + jobExecution.id + " " + jobExecution.job.id);
      return "";
    }
    String idParameters = this._executionEngine.parameterGenerationsHBT(results, tuningParameters);
    return idParameters.toString();
  }

  private List<AppResult> getAppResults(JobExecution jobExecution) {
    List<AppResult> results = null;
    try {
      results = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, jobExecution.flowExecution.flowExecId)
          .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecution.jobExecId)
          .findList();
    } catch (Exception e) {
      logger.warn(" Job Analysis is not completed . ");
      return results;
    }
    return results;
  }

  /**
   * For every tuning info:
   *    For every new particle:
   *        From the tuner set extract the list of suggested parameters
   *        Check penalty
   *        Save the param in the job execution table by creating execution instance (Create an entry into param_set table)
   *        Update the execution instance in each of the suggested params (Update the param_set_id in each of the prams)
   *        save th suggested parameters
   *        update the paramsetid in the particle and add particle to a particlelist
   *    Update the tunerstate from the updated particles
   *    save the tuning info in db
   *
   * @param jobTuningInfoList JobTuningInfo List
   */
  protected Boolean updateDatabase(List<JobTuningInfo> jobTuningInfoList) {
    logger.info("Updating new parameter suggestion in database HBT");
    if (jobTuningInfoList == null) {
      logger.info("No new parameter suggestion to update");
      return false;
    }

    for (JobTuningInfo jobTuningInfo : jobTuningInfoList) {
      logger.info("Updating new parameter suggestion for job:" + jobTuningInfo.getTuningJob().jobDefId);

      JobDefinition job = jobTuningInfo.getTuningJob();
      List<TuningParameter> paramList = jobTuningInfo.getParametersToTune();
      String stringTunerState = jobTuningInfo.getTunerState();
      if (stringTunerState == null || stringTunerState.length() == 0) {
        logger.error("Suggested parameter suggestion is empty for job id: " + job.jobDefId);
        continue;
      }

      TuningJobDefinition tuningJobDefinition = TuningHelper.getTuningJobDefinition(job);

      List<TuningParameter> derivedParameterList = TuningHelper.getDerivedParameterList(tuningJobDefinition);

      logger.info("No. of derived tuning params for job " + tuningJobDefinition.job.jobName + ": "
          + derivedParameterList.size());

      List<JobSuggestedParamValue> jobSuggestedParamValueList = getParamValueList(stringTunerState);
      _executionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList, jobSuggestedParamValueList);
      JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
      jobSuggestedParamSet.jobDefinition = job;
      jobSuggestedParamSet.tuningAlgorithm = tuningJobDefinition.tuningAlgorithm;
      jobSuggestedParamSet.isParamSetDefault = false;
      jobSuggestedParamSet.isParamSetBest = false;
      if (isParamConstraintViolated(jobSuggestedParamValueList)) {
        penaltyApplication(jobSuggestedParamSet, tuningJobDefinition);
      } else {
        logger.info(" Parameters constraints not violeted ");
        jobSuggestedParamSet.areConstraintsViolated = false;
        processParamSetStatus(jobSuggestedParamSet);
      }
      saveSuggestedParamSet(jobSuggestedParamSet);

      for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
        jobSuggestedParamValue.jobSuggestedParamSet = jobSuggestedParamSet;
      }
      logger.info(" Job Suggested list " + jobSuggestedParamValueList.size());
      saveSuggestedParams(jobSuggestedParamValueList);
    }

    return true;
  }

  private void processParamSetStatus(JobSuggestedParamSet jobSuggestedParamSet) {
    TuningJobDefinition tuningJobDefinition1 = TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, jobSuggestedParamSet.jobDefinition.id)
        .setMaxRows(1)
        .findUnique();
    jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
    //handleDiscarding(tuningJobDefinition1, jobSuggestedParamSet);
  }

 /* private void handleDiscarding(TuningJobDefinition tuningJobDefinition1, JobSuggestedParamSet jobSuggestedParamSet) {
    if (tuningJobDefinition1.autoApply) {
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
    } else {
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.DISCARDED;
    }
    List<JobSuggestedParamSet> tempJobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, tuningJobDefinition1.job.id)
        .findList();
    Boolean isManuallyOverriden = false;
    for (JobSuggestedParamSet jobSuggestedParamSet1 : tempJobSuggestedParamSet) {
      if (jobSuggestedParamSet1.isManuallyOverridenParameter) {
        isManuallyOverriden = true;
      }
    }
    if (isManuallyOverriden) {
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.DISCARDED;
    }
  }*/

  private void penaltyApplication(JobSuggestedParamSet jobSuggestedParamSet, TuningJobDefinition tuningJobDefinition) {
    logger.info("Parameter constraint violated. Applying penalty.");
    int penaltyConstant = 4;
    Double averageResourceUsagePerGBInput =
        tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
    Double maxDesiredResourceUsagePerGBInput =
        averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;

    jobSuggestedParamSet.areConstraintsViolated = true;
    jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
    jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED;
  }

  /**
   * Saves the list of suggested parameter values to database
   * @param jobSuggestedParamValueList Suggested Parameter Values List
   */
  private void saveSuggestedParams(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      jobSuggestedParamValue.save();
    }
  }

  /**
   * Saves the suggested param set in the database and returns the param set id
   * @param jobSuggestedParamSet JobExecution
   * @return Param Set Id
   */
  private Long saveSuggestedParamSet(JobSuggestedParamSet jobSuggestedParamSet) {
    jobSuggestedParamSet.save();
    return jobSuggestedParamSet.id;
  }

  private List<JobSuggestedParamValue> getParamValueList(String tunerState) {
    List<JobSuggestedParamValue> jobSuggestedParamValueList = new ArrayList<JobSuggestedParamValue>();
    for (String parameter : tunerState.split("\n")) {
      logger.info(" Parameter values " + parameter);
      String paramIDValues[] = parameter.split("\t");
      if (paramIDValues.length == 2) {
        JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
        jobSuggestedParamValue.tuningParameter = TuningParameter.find.byId(Integer.parseInt(paramIDValues[0]));
        jobSuggestedParamValue.paramValue = Double.parseDouble(paramIDValues[1]);
        jobSuggestedParamValueList.add(jobSuggestedParamValue);
      }
    }
    logger.info(" Job Suggested Values " + jobSuggestedParamValueList.size());
    return jobSuggestedParamValueList;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList, JobDefinition job) {

  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {

    return _executionEngine.isParamConstraintViolatedHBT(jobSuggestedParamValues);
  }

  @Override
  public String getManagerName() {
    return "TuningTypeManagerHBT";
  }
}
