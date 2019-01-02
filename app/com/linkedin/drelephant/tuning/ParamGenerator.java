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

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import controllers.AutoTuningMetricsController;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobExecution;
import models.JobSavedState;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import play.libs.Json;


/**
 * This is an abstract class for generating parameter suggestions for jobs
 */
public abstract class ParamGenerator {

  private static final String JSON_CURRENT_POPULATION_KEY = "current_population";
  private final Logger logger = Logger.getLogger(getClass());

  /**
   * Generates the parameters using tuningJobInfo and returns it in updated JobTuningInfo
   * @param jobTuningInfo The tuning job information required to create new params
   * @return The updated job tuning information containing the new params
   */
  public abstract JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo);

  /**
   * Converts a json to list of particles
   * @param jsonParticleList A list of  configurations (particles) in json
   * @return Particle List
   */
  private List<Particle> jsonToParticleList(JsonNode jsonParticleList) {

    List<Particle> particleList = new ArrayList<Particle>();
    if (jsonParticleList == null) {
      logger.info("Null json, empty particle list returned");
    } else {
      for (JsonNode jsonParticle : jsonParticleList) {
        Particle particle;
        particle = Json.fromJson(jsonParticle, Particle.class);
        if (particle != null) {
          particleList.add(particle);
        }
      }
    }
    return particleList;
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

    List<JobSuggestedParamSet> pendingParamSetList = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .or(Expr.or(Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.CREATED),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.SENT)),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.EXECUTED))
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 0).findList();

    List<JobDefinition> pendingParamJobList = new ArrayList<JobDefinition>();
    for (JobSuggestedParamSet pendingParamSet : pendingParamSetList) {
      if (!pendingParamJobList.contains(pendingParamSet.jobDefinition)) {
        pendingParamJobList.add(pendingParamSet.jobDefinition);
      }
    }

    List<TuningJobDefinition> tuningJobDefinitionList = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findList();

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

  /**
   * Converts a list of particles to json
   * @param particleList Particle List
   * @return JsonNode
   */
  private JsonNode particleListToJson(List<Particle> particleList) {
    JsonNode jsonNode;

    if (particleList == null) {
      jsonNode = JsonNodeFactory.instance.objectNode();
      logger.info("Null particleList, returning empty json");
    } else {
      jsonNode = Json.toJson(particleList);
    }
    return jsonNode;
  }

  /**
   * Returns the tuning information for the jobs
   * @param tuningJobs Job List
   * @return Tuning information list
   */
  private List<JobTuningInfo> getJobsTuningInfo(List<TuningJobDefinition> tuningJobs) {

    List<JobTuningInfo> jobTuningInfoList = new ArrayList<JobTuningInfo>();
    for (TuningJobDefinition tuningJobDefinition : tuningJobs) {
      JobDefinition job = tuningJobDefinition.job;
      logger.info("Getting tuning information for job: " + job.jobDefId);
      List<TuningParameter> tuningParameterList = TuningParameter.find.where()
          .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
              tuningJobDefinition.tuningAlgorithm.id)
          .eq(TuningParameter.TABLE.isDerived, 0)
          .findList();

        logger.info("Fetching default parameter values for job " + tuningJobDefinition.job.jobDefId);
        JobSuggestedParamSet defaultJobParamSet = JobSuggestedParamSet.find.where()
            .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, tuningJobDefinition.job.id)
            .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 1)
            .order()
            .desc(JobSuggestedParamSet.TABLE.id)
            .setMaxRows(1)
            .findUnique();

        if (defaultJobParamSet != null) {
          List<JobSuggestedParamValue> jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
              .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + "." + JobExecution.TABLE.id,
                  defaultJobParamSet.id)
              .findList();

          if (jobSuggestedParamValueList.size() > 0) {
            Map<Integer, Double> defaultExecutionParamMap = new HashMap<Integer, Double>();

            for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
              defaultExecutionParamMap.put(jobSuggestedParamValue.tuningParameter.id,
                  jobSuggestedParamValue.paramValue);
            }

            for (TuningParameter tuningParameter : tuningParameterList) {
              Integer paramId = tuningParameter.id;
              if (defaultExecutionParamMap.containsKey(paramId)) {
                logger.info(
                    "Updating value of param " + tuningParameter.paramName + " to " + defaultExecutionParamMap.get(
                        paramId));
                tuningParameter.defaultValue = defaultExecutionParamMap.get(paramId);
              }
            }
          }
        }

      JobTuningInfo jobTuningInfo = new JobTuningInfo();
      jobTuningInfo.setTuningJob(job);
      jobTuningInfo.setJobType(tuningJobDefinition.tuningAlgorithm.jobType);
      jobTuningInfo.setParametersToTune(tuningParameterList);
      JobSavedState jobSavedState = JobSavedState.find.byId(job.id);

      boolean validSavedState = true;
      if (jobSavedState != null && jobSavedState.isValid()) {
        String savedState = new String(jobSavedState.savedState, Charset.forName("UTF-8"));
        ObjectNode jsonSavedState = (ObjectNode) Json.parse(savedState);
        JsonNode jsonCurrentPopulation = jsonSavedState.get(JSON_CURRENT_POPULATION_KEY);
        List<Particle> currentPopulation = jsonToParticleList(jsonCurrentPopulation);
        for (Particle particle : currentPopulation) {
          Long paramSetId = particle.getParamSetId();

          logger.info("Param set id: " + paramSetId.toString());
          JobSuggestedParamSet jobSuggestedParamSet =
              JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, paramSetId).findUnique();

          if (jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
              && jobSuggestedParamSet.fitness != null) {
            particle.setFitness(jobSuggestedParamSet.fitness);
          } else {
            validSavedState = false;
            logger.error("Invalid saved state: Fitness of previous execution not computed.");
            break;
          }
        }

        if (validSavedState) {
          JsonNode updatedJsonCurrentPopulation = particleListToJson(currentPopulation);
          jsonSavedState.set(JSON_CURRENT_POPULATION_KEY, updatedJsonCurrentPopulation);
          savedState = Json.stringify(jsonSavedState);
          jobTuningInfo.setTunerState(savedState);
        }
      } else {
        logger.info("Saved state empty for job: " + job.jobDefId);
        validSavedState = false;
      }

      if (!validSavedState) {
        jobTuningInfo.setTunerState("{}");
      }

      logger.info("Adding JobTuningInfo " + Json.toJson(jobTuningInfo));
      jobTuningInfoList.add(jobTuningInfo);
    }
    return jobTuningInfoList;
  }

  /**
   * Returns list of suggested parameters
   * @param particle Particle (configuration)
   * @param paramList Parameter List
   * @return Suggested Param Value List
   */
  private List<JobSuggestedParamValue> getParamValueList(Particle particle, List<TuningParameter> paramList) {
    logger.debug("Particle is: " + Json.toJson(particle));
    List<JobSuggestedParamValue> jobSuggestedParamValueList = new ArrayList<JobSuggestedParamValue>();

    if (particle != null) {
      List<Double> candidate = particle.getCandidate();

      if (candidate != null) {
        logger.debug("Candidate is:" + Json.toJson(candidate));
        for (int i = 0; i < candidate.size() && i < paramList.size(); i++) {
          logger.info("Candidate is " + candidate);

          JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
          int paramId = paramList.get(i).id;
          jobSuggestedParamValue.tuningParameter = TuningParameter.find.byId(paramId);
          jobSuggestedParamValue.paramValue = candidate.get(i);
          jobSuggestedParamValueList.add(jobSuggestedParamValue);
        }
      } else {
        logger.info("Candidate is null");
      }
    } else {
      logger.info("Particle null");
    }
    return jobSuggestedParamValueList;
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
  private void updateDatabase(List<JobTuningInfo> jobTuningInfoList) {
    logger.info("Updating new parameter suggestion in database");
    if (jobTuningInfoList == null) {
      logger.info("No new parameter suggestion to update");
      return;
    }

    int paramSetNotGeneratedJobs = jobTuningInfoList.size();

    for (JobTuningInfo jobTuningInfo : jobTuningInfoList) {
      logger.info("Updating new parameter suggestion for job:" + jobTuningInfo.getTuningJob().jobDefId);

      JobDefinition job = jobTuningInfo.getTuningJob();
      List<TuningParameter> paramList = jobTuningInfo.getParametersToTune();
      String stringTunerState = jobTuningInfo.getTunerState();
      if (stringTunerState == null) {
        logger.error("Suggested parameter suggestion is empty for job id: " + job.jobDefId);
        continue;
      }

      TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
          .fetch(TuningJobDefinition.TABLE.job, "*")
          .where()
          .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
          .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
          .findUnique();

      List<TuningParameter> derivedParameterList = new ArrayList<TuningParameter>();
        derivedParameterList = TuningParameter.find.where()
            .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id,
                tuningJobDefinition.tuningAlgorithm.id)
            .eq(TuningParameter.TABLE.isDerived, 1)
            .findList();

      logger.info("No. of derived tuning params for job " + tuningJobDefinition.job.jobName + ": "
          + derivedParameterList.size());

      JsonNode jsonTunerState = Json.parse(stringTunerState);
      JsonNode jsonSuggestedPopulation = jsonTunerState.get(JSON_CURRENT_POPULATION_KEY);

      if (jsonSuggestedPopulation == null) {
        continue;
      }

      paramSetNotGeneratedJobs--;

      List<Particle> suggestedPopulation = jsonToParticleList(jsonSuggestedPopulation);

      for (Particle suggestedParticle : suggestedPopulation) {
        AutoTuningMetricsController.markParamSetGenerated();
        List<JobSuggestedParamValue> jobSuggestedParamValueList = getParamValueList(suggestedParticle, paramList);

        Map<String, Double> jobSuggestedParamValueMap = new HashMap<String, Double>();
        for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
          jobSuggestedParamValueMap.put(jobSuggestedParamValue.tuningParameter.paramName,
              jobSuggestedParamValue.paramValue);
        }

        for (TuningParameter derivedParameter : derivedParameterList) {
          logger.info("Computing value of derived param: " + derivedParameter.paramName);
          Double paramValue = null;
          if (derivedParameter.paramName.equals("mapreduce.reduce.java.opts")) {
            String parentParamName = "mapreduce.reduce.memory.mb";
            if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
              paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
            }
          } else if (derivedParameter.paramName.equals("mapreduce.map.java.opts")) {
            String parentParamName = "mapreduce.map.memory.mb";
            if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
              paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
            }
          } else if (derivedParameter.paramName.equals("mapreduce.input.fileinputformat.split.maxsize")) {
            String parentParamName = "pig.maxCombinedSplitSize";
            if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
              paramValue = jobSuggestedParamValueMap.get(parentParamName);
            }
          }

          if (paramValue != null) {
            JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
            jobSuggestedParamValue.paramValue = paramValue;
            jobSuggestedParamValue.tuningParameter = derivedParameter;
            jobSuggestedParamValueList.add(jobSuggestedParamValue);
          }
        }

        JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
        jobSuggestedParamSet.jobDefinition = job;
        jobSuggestedParamSet.tuningAlgorithm = tuningJobDefinition.tuningAlgorithm;
        jobSuggestedParamSet.isParamSetDefault = false;
        jobSuggestedParamSet.isParamSetBest = false;
        if (isParamConstraintViolated(jobSuggestedParamValueList, jobSuggestedParamSet.tuningAlgorithm.jobType)) {
          logger.info("Parameter constraint violated. Applying penalty.");
          int penaltyConstant = 3;
          Double averageResourceUsagePerGBInput =
              tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
          Double maxDesiredResourceUsagePerGBInput =
              averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;

          jobSuggestedParamSet.areConstraintsViolated = true;
          jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
          jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED;
        } else {
          jobSuggestedParamSet.areConstraintsViolated = false;
          jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
        }
        Long paramSetId = saveSuggestedParamSet(jobSuggestedParamSet);

        for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
          jobSuggestedParamValue.jobSuggestedParamSet = jobSuggestedParamSet;
        }
        suggestedParticle.setPramSetId(paramSetId);
        saveSuggestedParams(jobSuggestedParamValueList);
      }

      JsonNode updatedJsonSuggestedPopulation = particleListToJson(suggestedPopulation);

      ObjectNode updatedJsonTunerState = (ObjectNode) jsonTunerState;
      updatedJsonTunerState.put(JSON_CURRENT_POPULATION_KEY, updatedJsonSuggestedPopulation);
      String updatedStringTunerState = Json.stringify(updatedJsonTunerState);
      jobTuningInfo.setTunerState(updatedStringTunerState);
    }
    AutoTuningMetricsController.setParamSetGenerateWaitJobs(paramSetNotGeneratedJobs);
    saveTunerState(jobTuningInfoList);
  }

  /**
   * Check if the parameters violated constraints
   * Constraint 1: sort.mb > 60% of map.memory: To avoid heap memory failure
   * Constraint 2: map.memory - sort.mb < 768: To avoid heap memory failure
   * Constraint 3: pig.maxCombinedSplitSize > 1.8*mapreduce.map.memory.mb
   * @param jobSuggestedParamValueList List of suggested param values
   * @param jobType Job type
   * @return true if the constraint is violated, false otherwise
   */
  private boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList,
      TuningAlgorithm.JobType jobType) {

    logger.info("Checking whether parameter values are within constraints");
    Integer violations = 0;

    if (jobType.equals(TuningAlgorithm.JobType.PIG)) {
      Double mrSortMemory = null;
      Double mrMapMemory = null;
      Double pigMaxCombinedSplitSize = null;

      for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
        if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.task.io.sort.mb")) {
          mrSortMemory = jobSuggestedParamValue.paramValue;
        } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.map.memory.mb")) {
          mrMapMemory = jobSuggestedParamValue.paramValue;
        } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("pig.maxCombinedSplitSize")) {
          pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
        }
      }

      if (mrSortMemory != null && mrMapMemory != null) {
        if (mrSortMemory > 0.6 * mrMapMemory) {
          logger.info("Constraint violated: Sort memory > 60% of map memory");
          violations++;
        }
        if (mrMapMemory - mrSortMemory < 768) {
          logger.info("Constraint violated: Map memory - sort memory < 768 mb");
          violations++;
        }
      }

      if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
        logger.info("Constraint violated: Pig max combined split size > 1.8 * map memory");
        violations++;
      }
    }
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }

  /**
   * Save the tuning info list to the database
   * @param jobTuningInfoList Tuning Info List
   */
  private void saveTunerState(List<JobTuningInfo> jobTuningInfoList) {
    for (JobTuningInfo jobTuningInfo : jobTuningInfoList) {
      if (jobTuningInfo.getTunerState() == null) {
        continue;
      }
      JobSavedState jobSavedState = JobSavedState.find.byId(jobTuningInfo.getTuningJob().id);
      if (jobSavedState == null) {
        jobSavedState = new JobSavedState();
        jobSavedState.jobDefinitionId = jobTuningInfo.getTuningJob().id;
      }
      jobSavedState.savedState = jobTuningInfo.getTunerState().getBytes(Charset.forName("UTF-8"));
      jobSavedState.save();
    }
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

  /**
   * Fetches job which need parameters, generates parameters and stores it in the database
   */
  public void getParams() {
    List<TuningJobDefinition> jobsForSwarmSuggestion = getJobsForParamSuggestion();
    List<JobTuningInfo> jobTuningInfoList = getJobsTuningInfo(jobsForSwarmSuggestion);
    List<JobTuningInfo> updatedJobTuningInfoList = new ArrayList<JobTuningInfo>();
    for (JobTuningInfo jobTuningInfo : jobTuningInfoList) {
      JobTuningInfo newJobTuningInfo = generateParamSet(jobTuningInfo);
      updatedJobTuningInfoList.add(newJobTuningInfo);
    }
    updateDatabase(updatedJobTuningInfoList);
  }
}
