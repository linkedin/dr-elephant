package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import java.util.List;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import org.apache.log4j.Logger;

public class ParameterGenerateManagerOBTAlgoPSOImpl extends ParameterGenerateManagerOBTAlgoPSO {
  private final Logger logger = Logger.getLogger(getClass());
  public ParameterGenerateManagerOBTAlgoPSOImpl(ExecutionEngine executionEngine) {
    this._executionEngine=executionEngine;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList, JobDefinition job) {

  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return _executionEngine.isParamConstraintViolatedPSO(jobSuggestedParamValues);
  }



  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = _executionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return _executionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO.name())
        .findList();
  }

  @Override
  public void initializePrerequisite(TuningAlgorithm tuningAlgorithm, JobDefinition job) {

  }

  @Override
  public void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution) {

  }

  @Override
  public int getSwarmSize() {
    return 3;
  }

  public String getManagerName() {
    return "ParameterGenerateManagerOBTAlgoPSOImpl" + this._executionEngine.getClass().getSimpleName();
  }
}
