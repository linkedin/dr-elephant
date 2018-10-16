package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import java.util.ArrayList;
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
import org.apache.log4j.Logger;


public class ParameterGenerateManagerOBTAlgoPSOIPSOImpl extends ParameterGenerateManagerOBTAlgoPSO {

  private final Logger logger = Logger.getLogger(getClass());

  enum UsageCounterSchema {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

  public ParameterGenerateManagerOBTAlgoPSOIPSOImpl(ExecutionEngine executionEngine) {
    this._executionEngine = executionEngine;
  }

  @Override
  protected void updateBoundryConstraint(List<TuningParameter> tuningParameterList, JobDefinition job) {
    applyIntelligenceOnParameter(tuningParameterList, job);
  }

  @Override
  public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValues) {
    return _executionEngine.isParamConstraintViolatedIPSO(jobSuggestedParamValues);
  }

  @Override
  protected List<JobSuggestedParamSet> getPendingParamSets() {
    List<JobSuggestedParamSet> pendingParamSetList = _executionEngine.getPendingJobs()
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, 0)
        .findList();
    return pendingParamSetList;
  }

  @Override
  protected List<TuningJobDefinition> getTuningJobDefinitions() {
    return _executionEngine.getTuningJobDefinitionsForParameterSuggestion()
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();
  }

  @Override
  public void initializePrerequisite(TuningAlgorithm tuningAlgorithm, JobDefinition job) {
    logger.debug(" Intialize Prerequisite ");
    try {
      setDefaultParameterValues(tuningAlgorithm, job);
    } catch (Exception e) {
      logger.debug("Cannot intialize parameter in IPSO " + e.getMessage());
    }
  }

  private void setDefaultParameterValues(TuningAlgorithm tuningAlgorithm, JobDefinition job) throws Exception{
    try {
      List<TuningParameter> tuningParameters =
          TuningParameter.find.where().eq(TuningParameter.TABLE.tuningAlgorithm, tuningAlgorithm).findList();

      List<TuningParameterConstraint> tuningParameterConstrains= TuningParameterConstraint.find.where()
          .eq(TuningParameterConstraint.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, job.id)
          .findList();

      if(tuningParameterConstrains== null || tuningParameterConstrains.size()==0) {
        logger.debug("Parameter constraints not added . Hence adding parameter constraint. ");
        for (TuningParameter tuningParameter : tuningParameters) {
          TuningParameterConstraint tuningParameterConstraint = new TuningParameterConstraint();
          tuningParameterConstraint.jobDefinition = job;
          tuningParameterConstraint.tuningParameter = tuningParameter;
          tuningParameterConstraint.lowerBound = tuningParameter.minValue;
          tuningParameterConstraint.upperBound = tuningParameter.maxValue;
          tuningParameterConstraint.constraintType = TuningParameterConstraint.ConstraintType.BOUNDARY;
          tuningParameterConstraint.save();
        }
      }
      else{
        logger.debug(" Parameter constraints already added . Hence not adding them");
      }
    } catch (Exception e) {
      logger.debug(
          " Error in setting up intial parameter constraint . IPSO will not work in this case ." + e.getMessage(),e);
      throw e;
    }
  }

  @Override
  public void parameterOptimizer(List<AppResult> appResults, JobExecution jobExecution) {
    logger.info(" IPSO Optimizer");
    _executionEngine.parameterOptimizerIPSO(appResults, jobExecution);
  }

  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job) {
    logger.debug(" Apply Intelligence");
    List<TuningParameterConstraint> tuningParameterConstraintList = new ArrayList<TuningParameterConstraint>();
    tuningParameterConstraintList = TuningParameterConstraint.find.where()
        .eq("job_definition_id", job.id)
        .eq(TuningParameterConstraint.TABLE.constraintType, TuningParameterConstraint.ConstraintType.BOUNDARY)
        .findList();

    Map<Integer, Integer> paramConstraintIndexMap = new HashMap<Integer, Integer>();
    int i = 0;
    if (tuningParameterConstraintList != null && tuningParameterConstraintList.size() >= 1) {
      for (TuningParameterConstraint tuningParameterConstraint : tuningParameterConstraintList) {
        paramConstraintIndexMap.put(tuningParameterConstraint.tuningParameter.id, i);
        i += 1;
      }
    } else {
      logger.debug("No boundary constraints found for job: " + job.jobName);
    }

    for (TuningParameter tuningParameter : tuningParameterList) {
      if (paramConstraintIndexMap.containsKey(tuningParameter.id)) {
        int index = paramConstraintIndexMap.get(tuningParameter.id);
        tuningParameter.minValue = tuningParameterConstraintList.get(index).lowerBound;
        tuningParameter.maxValue = tuningParameterConstraintList.get(index).upperBound;
      }
    }
  }

  @Override
  public int getSwarmSize() {
    return 2;
  }

  public String getManagerName() {
    return "ParameterGenerateManagerOBTAlgoPSOIPSOImpl" + this._executionEngine.getClass().getSimpleName();
  }
}
