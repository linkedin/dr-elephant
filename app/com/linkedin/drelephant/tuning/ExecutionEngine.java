package com.linkedin.drelephant.tuning;

import com.avaje.ebean.ExpressionList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;


public interface ExecutionEngine {

  void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue);

  ExpressionList<JobSuggestedParamSet> getPendingJobs();

  ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion();
  /*
  PSO related methods
   */
  Boolean isParamConstraintViolatedPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);



  Boolean isParamConstraintViolatedIPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  public void parameterOptimizerIPSO(List<AppResult> results, JobExecution jobExecution);



  public String parameterGenerationsHBT(List<AppResult> results, List<TuningParameter> tuningParameters);
  Boolean isParamConstraintViolatedHBT(List<JobSuggestedParamValue> jobSuggestedParamValueList);
}
