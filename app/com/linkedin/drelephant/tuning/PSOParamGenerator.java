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

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.drelephant.ElephantContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import play.libs.Json;


/**
 * This class extends ParamGenerator class. It generates parameter suggestion using PSO algorithm,
 */
public class PSOParamGenerator extends ParamGenerator {

  private static final String PARAMS_TO_TUNE_FIELD_NAME = "parametersToTune";
  private static final String PYTHON_PATH_CONF = "python.path";
  private static final String PSO_DIR_PATH_ENV_VARIABLE = "PSO_DIR_PATH";
  private static final String PYTHON_PATH_ENV_VARIABLE = "PYTHONPATH";
  private final Logger logger = Logger.getLogger(PSOParamGenerator.class);
  private String PYTHON_PATH = null;
  private String TUNING_SCRIPT_PATH = null;

  public PSOParamGenerator() {
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

  /**
   * Interacts with python scripts to generate new parameter suggestions
   * @param jobTuningInfo Job tuning information
   * @return Updated job tuning information
   */
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
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, jobTuningInfo.getTuningJob().id)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .findUnique();

    int swarmSize = getSwarmSize(tuningJobDefinition.tuningAlgorithm);

    List<String> error = new ArrayList<String>();
    logger.debug("String State " + stringTunerState);
    try {

      Process p = Runtime.getRuntime()
          .exec(PYTHON_PATH + " " + TUNING_SCRIPT_PATH + " " + stringTunerState + " " + parametersToTune + " " + jobType
              + " " + swarmSize);
      BufferedReader inputStream = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader errorStream = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      String updatedStringTunerState = inputStream.readLine();
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

  private int getSwarmSize(TuningAlgorithm tuningAlgorithm) {
    AutoTuningOptimizeManager optimizeManager = OptimizationAlgoFactory.getOptimizationAlogrithm(tuningAlgorithm);
    if (optimizeManager != null) {
      return optimizeManager.getSwarmSize();
    }
    return 3;
  }
}
