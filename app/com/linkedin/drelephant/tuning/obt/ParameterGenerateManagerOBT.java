package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractParameterGenerateManager;
import com.linkedin.drelephant.tuning.JobTuningInfo;
import com.linkedin.drelephant.tuning.Particle;
import controllers.AutoTuningMetricsController;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
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


public abstract class ParameterGenerateManagerOBT extends AbstractParameterGenerateManager {

  protected final String PARAMS_TO_TUNE_FIELD_NAME = "parametersToTune";

  private final Logger logger = Logger.getLogger(getClass());

  /*
    Intialize any prequisite require for Optimizer
    Calls once in lifetime of the flow
   */
  public abstract void initializePrerequisite(TuningAlgorithm tuningAlgorithm,
      JobDefinition job);

  /*
    Optimize search space
    call after each execution of flow
   */
  public abstract void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution);

  /*
     Saved the Job State , so that it can be used in next execution. In PSO , it saves the state of previous parameter generated
   */
  protected abstract void saveJobState(JobTuningInfo jobTuningInfo, JobDefinition job);

  @Override
  public String getManagerName() {
    return "ParameterGenerateManagerOBT";
  }
}
