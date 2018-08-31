package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;
import play.libs.Json;

public abstract class AbstractTuningTypeManager implements Manager {
  protected final String JSON_CURRENT_POPULATION_KEY = "current_population";
  private final Logger logger = Logger.getLogger(getClass());
  protected ExecutionEngine _executionEngine;
  protected String tuningType = null;
  protected String tuningAlgorithm = null;

  /**
   * Fetches the list to job which need new parameter suggestion
   * @return Job list
   */

  protected List<JobTuningInfo> detectJobsForParameterGeneration() {
    List<TuningJobDefinition> jobsForSwarmSuggestion = getJobsForParamSuggestion();
    List<JobTuningInfo> jobTuningInfoList = getJobsTuningInfo(jobsForSwarmSuggestion);
    return jobTuningInfoList;
  }

  /**
   * Fetches the list to job which need new parameter suggestion
   * @return Job list
   */
  private List<TuningJobDefinition> getJobsForParamSuggestion() {
    // Todo: [Important] Change the logic. This is very rigid. Ideally you should look at the param set ids in the saved state,
    // todo: [continuation] if their fitness is computed, pso can generate new params for the job
    logger.info("Checking which jobs need new parameter suggestion");
    List<TuningJobDefinition> jobsForParamSuggestion = new ArrayList<TuningJobDefinition>();
    List<JobSuggestedParamSet> pendingParamSetList = getPendingParamSets();
    //logger.info(" Jobs pending " + pendingParamSetList.size());
    List<JobDefinition> pendingParamJobList = new ArrayList<JobDefinition>();
    for (JobSuggestedParamSet pendingParamSet : pendingParamSetList) {
      if (!pendingParamJobList.contains(pendingParamSet.jobDefinition)) {
        pendingParamJobList.add(pendingParamSet.jobDefinition);
      }
    }

    List<TuningJobDefinition> tuningJobDefinitionList = getTuningJobDefinitions();
    //logger.info("Total Jobs " + tuningJobDefinitionList.size());
    if (tuningJobDefinitionList.size() == 0) {
      logger.error("No auto-tuning enabled jobs found");
    }

    for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitionList) {
      if (!pendingParamJobList.contains(tuningJobDefinition.job)) {
        logger.info("New parameter suggestion needed for job: " + tuningJobDefinition.job.jobName);
        jobsForParamSuggestion.add(tuningJobDefinition);
      }
    }
    logger.info("Number of job(s) which need new parameter suggestion: " + jobsForParamSuggestion.size());
    return jobsForParamSuggestion;
  }

  protected List<JobTuningInfo> generateParameters(List<JobTuningInfo> jobsForParameterSuggestion) {
    List<JobTuningInfo> updatedJobTuningInfoList = new ArrayList<JobTuningInfo>();
    for (JobTuningInfo jobTuningInfo : jobsForParameterSuggestion) {
      JobTuningInfo newJobTuningInfo = generateParamSet(jobTuningInfo);
      updatedJobTuningInfoList.add(newJobTuningInfo);
    }
    return updatedJobTuningInfoList;
  }

  protected abstract JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo);

  protected abstract Boolean updateDatabase(List<JobTuningInfo> tuningJobDefinitions);

  public final Boolean execute() {
    logger.info("Executing Tuning Algorithm");
    Boolean parameterGenerationDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<JobTuningInfo> jobTuningInfo = detectJobsForParameterGeneration();
    if (jobTuningInfo != null && jobTuningInfo.size() >= 1) {
      logger.info("Generating Parameters ");
      List<JobTuningInfo> updatedJobTuningInfoList = generateParameters(jobTuningInfo);
      logger.info("Updating Database");
      databaseUpdateDone = updateDatabase(updatedJobTuningInfoList);
    }
    logger.info("Param Generation Done");
    return databaseUpdateDone;
  }

  protected abstract List<JobSuggestedParamSet> getPendingParamSets();

  protected abstract List<TuningJobDefinition> getTuningJobDefinitions();

  /**
   * Returns the tuning information for the jobs
   * @param tuningJobs Job List
   * @return Tuning information list
   */
  protected List<JobTuningInfo> getJobsTuningInfo(List<TuningJobDefinition> tuningJobs) {
    List<com.linkedin.drelephant.tuning.JobTuningInfo> jobTuningInfoList =
        new ArrayList<com.linkedin.drelephant.tuning.JobTuningInfo>();
    for (TuningJobDefinition tuningJobDefinition : tuningJobs) {
      JobDefinition job = tuningJobDefinition.job;
      List<TuningParameter> tuningParameterList = generateTuningParameterListWithDefaultValues(tuningJobDefinition);
      // updating boundary constraints for the job
      updateBoundryConstraint(tuningParameterList, job);

      com.linkedin.drelephant.tuning.JobTuningInfo jobTuningInfo = new com.linkedin.drelephant.tuning.JobTuningInfo();
      jobTuningInfo.setTuningJob(job);
      jobTuningInfo.setJobType(tuningJobDefinition.tuningAlgorithm.jobType);
      jobTuningInfo.setParametersToTune(tuningParameterList);

      saveJobState(jobTuningInfo, job);

      logger.info("Adding JobTuningInfo " + Json.toJson(jobTuningInfo));
      jobTuningInfoList.add(jobTuningInfo);
    }
    return jobTuningInfoList;
  }

  protected abstract void saveJobState(JobTuningInfo jobTuningInfo, JobDefinition job);

  protected abstract void updateBoundryConstraint(List<TuningParameter> tuningParameterList,JobDefinition job);

  public abstract boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues);


  protected List<TuningParameter> generateTuningParameterListWithDefaultValues(
      TuningJobDefinition tuningJobDefinition) {
    JobDefinition job = tuningJobDefinition.job;
    logger.info("Getting tuning information for job: " + job.jobDefId);
    List<TuningParameter> tuningParameterList = TuningHelper.getTuningParameterList(tuningJobDefinition);

    logger.info("Fetching default parameter values for job " + tuningJobDefinition.job.jobDefId);
    JobSuggestedParamSet defaultJobParamSet = TuningHelper.getDefaultParameterValuesforJob(tuningJobDefinition);
    assignDefaultValues(defaultJobParamSet, tuningParameterList);
    return tuningParameterList;
  }

  protected void assignDefaultValues(JobSuggestedParamSet defaultJobParamSet,
      List<TuningParameter> tuningParameterList) {
    if (defaultJobParamSet != null) {
      logger.info("Fetching default parameter values for job ");
      List<JobSuggestedParamValue> jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
          .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + "." + JobExecution.TABLE.id, defaultJobParamSet.id)
          .findList();

      if (jobSuggestedParamValueList.size() > 0) {
        logger.info("Giving default values ");
        Map<Integer, Double> defaultExecutionParamMap = new HashMap<Integer, Double>();

        for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
          defaultExecutionParamMap.put(jobSuggestedParamValue.tuningParameter.id, jobSuggestedParamValue.paramValue);
        }

        for (TuningParameter tuningParameter : tuningParameterList) {
          logger.info("Giving default values ");
          Integer paramId = tuningParameter.id;
          if (defaultExecutionParamMap.containsKey(paramId)) {
            logger.info("Updating value of param " + tuningParameter.paramName + " to " + defaultExecutionParamMap.get(
                paramId));
            tuningParameter.defaultValue = defaultExecutionParamMap.get(paramId);
          }
        }
      }
    }
  }
}
