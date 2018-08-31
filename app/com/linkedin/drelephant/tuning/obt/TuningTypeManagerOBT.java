package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractTuningTypeManager;
import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.Particle;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.tuning.TuningHelper;
import controllers.AutoTuningMetricsController;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
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
import org.apache.hadoop.conf.Configuration;


public abstract class TuningTypeManagerOBT extends AbstractTuningTypeManager {

  private static final String PARAMS_TO_TUNE_FIELD_NAME = "parametersToTune";
  private static final String PYTHON_PATH_CONF = "python.path";
  private static final String PSO_DIR_PATH_ENV_VARIABLE = "PSO_DIR_PATH";
  private static final String PYTHON_PATH_ENV_VARIABLE = "PYTHONPATH";
  private String PYTHON_PATH = null;
  private String TUNING_SCRIPT_PATH = null;
  private final Logger logger = Logger.getLogger(getClass());

  public TuningTypeManagerOBT() {
    tuningType = "OBT";
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    PYTHON_PATH = configuration.get(PYTHON_PATH_CONF);
    if (PYTHON_PATH == null) {
      PYTHON_PATH = System.getenv(PYTHON_PATH_ENV_VARIABLE);
    }
    String PSO_DIR_PATH = System.getenv(PSO_DIR_PATH_ENV_VARIABLE);

    if (PSO_DIR_PATH == null) {
      throw new NullPointerException("Couldn't find directory containing PSO scripts");
    }
    if (PYTHON_PATH == null) {
      PYTHON_PATH = "python";
    }
    TUNING_SCRIPT_PATH = PSO_DIR_PATH + "/pso_param_generation.py";
    logger.info("Tuning script path: " + TUNING_SCRIPT_PATH);
    logger.info("Python path: " + PYTHON_PATH);
  }

  @Override
  protected void saveJobState(JobTuningInfo jobTuningInfo, JobDefinition job) {
    boolean validSavedState = true;
    JobSavedState jobSavedState = JobSavedState.find.byId(job.id);
    if (jobSavedState != null && jobSavedState.isValid()) {
      String savedState = new String(jobSavedState.savedState);
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
  }

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
   * Interacts with python scripts to generate new parameter suggestions
   * @param jobTuningInfo Job tuning information
   * @return Updated job tuning information
   */

  @Override
  public JobTuningInfo generateParamSet(JobTuningInfo jobTuningInfo) {
    logger.info("Generating param set for job: " + jobTuningInfo.getTuningJob().jobName);

    JobTuningInfo newJobTuningInfo = new JobTuningInfo();
    newJobTuningInfo.setTuningJob(jobTuningInfo.getTuningJob());
    newJobTuningInfo.setParametersToTune(jobTuningInfo.getParametersToTune());
    newJobTuningInfo.setJobType(jobTuningInfo.getJobType());

    JsonNode jsonJobTuningInfo = Json.toJson(jobTuningInfo);
    String parametersToTune = jsonJobTuningInfo.get(PARAMS_TO_TUNE_FIELD_NAME).toString();
    String stringTunerState = jobTuningInfo.getTunerState();
    stringTunerState = stringTunerState.replaceAll("\\s+", "");
    String jobType = jobTuningInfo.getJobType().toString();

    logger.info(" ID " + jobTuningInfo.getTuningJob().id);

    int swarmSize = getSwarmSize();

    List<String> error = new ArrayList<String>();
    logger.debug("String State " + stringTunerState);
    try {

      Process p = Runtime.getRuntime()
          .exec(PYTHON_PATH + " " + TUNING_SCRIPT_PATH + " " + stringTunerState + " " + parametersToTune + " " + jobType
              + " " + swarmSize);
      logger.info(
          PYTHON_PATH + " " + TUNING_SCRIPT_PATH + " " + stringTunerState + " " + parametersToTune + " " + jobType + " "
              + swarmSize);

      BufferedReader inputStream = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      String updatedStringTunerState = inputStream.readLine();
      logger.info("Param  Generator Testing" + updatedStringTunerState);
      newJobTuningInfo.setTunerState(updatedStringTunerState);
      String errorLine;
      while ((errorLine = errorStream.readLine()) != null) {
        error.add(errorLine);
      }
      if (error.size() != 0) {
        logger.error("Error in python script running PSO: " + error.toString());
      }
    } catch (IOException e) {
      logger.error("Error in generateParamSet()", e);
    }
    return newJobTuningInfo;
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
    logger.info("Updating new parameter suggestion in database");
    if (jobTuningInfoList == null) {
      logger.info("No new parameter suggestion to update");
      return false;
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
        _executionEngine.computeValuesOfDerivedConfigurationParameters(derivedParameterList,
            jobSuggestedParamValueList);
        JobSuggestedParamSet jobSuggestedParamSet = new JobSuggestedParamSet();
        jobSuggestedParamSet.jobDefinition = job;
        jobSuggestedParamSet.tuningAlgorithm = tuningJobDefinition.tuningAlgorithm;
        jobSuggestedParamSet.isParamSetDefault = false;
        jobSuggestedParamSet.isParamSetBest = false;
        if (isParamConstraintViolated(jobSuggestedParamValueList)) {
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
    return true;
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
      jobSavedState.savedState = jobTuningInfo.getTunerState().getBytes();
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

  @Override
  public String getManagerName() {
    return "TuningTypeManagerOBT";
  }

  /*
    Intialize any prequisite require for Optimizer
    Calls once in lifetime of the flow
   */
  public abstract void intializePrerequisite(TuningAlgorithm tuningAlgorithm,
      JobSuggestedParamSet jobSuggestedParamSet);

  /*
    Optimize search space
    call after each execution of flow
   */
  public abstract void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution);



  protected abstract int getSwarmSize();
}
