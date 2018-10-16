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


/**
 * Exeuction Engine is the interface of different execution engines. Currently it have two implementations
 * MRExecutionEngine : Handle parameters releated to Map Reduce.
 * SparkExecutionEngine : Handle parameters releated to Spark
 */

public interface ExecutionEngine {

  /**
   * This method is used to compute the values of derived parameters . These parameter values have not suggested by tuning algorithm
   *
   * @param derivedParameterList Derived Parameter List
   * @param jobSuggestedParamValue Update job suggested param value with the derived parameter value list.
   *
   */
  void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValue);

  /**
   * This method is used to get the pending JobSuggestedParamSet , param set which are not in FitnessCompute & discarded jobs
   *
   * @return Return pending jobs
   */

  ExpressionList<JobSuggestedParamSet> getPendingJobs();

  /**
   * This method is used to get jobs for which tuning enabled. Subtraction of this set with the above set
   * gives us the jobs for which parameter have to be generated
   *
   * @return Return Tuning enabled jobs
   */
  ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion();

  /**
   * Check if the suggested parameters violets any constraint for PSO .
   */
  Boolean isParamConstraintViolatedPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  /**
   * Check if the suggested parameters violets any constraint for IPSO .
   */
  Boolean isParamConstraintViolatedIPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  /**
   *  Optimizes parameter boundries based on the Heuristics. This is only applicable for IPSO
   * @param results : App Results
   * @param jobExecution : Job Execution on the basis of which parameter boundries will be changed.
   */
  public void parameterOptimizerIPSO(List<AppResult> results, JobExecution jobExecution);

  /**
   *  Parameter Suggestion
   * @param results : Based on the previous execution results
   * @param tuningParameters : Tuning parameters
   * @return : Suggest parameters new values.
   */
  public String parameterGenerationsHBT(List<AppResult> results, List<TuningParameter> tuningParameters);

  /**
   * Check if the suggested parameter violets any constraint.
   * @param jobSuggestedParamValueList
   * @return
   */
  Boolean isParamConstraintViolatedHBT(List<JobSuggestedParamValue> jobSuggestedParamValueList);
}
