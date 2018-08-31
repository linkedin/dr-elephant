package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.List;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;


public class FitnessManagerOBTAlgoIPSO extends FitnessManagerOBT {

  private final Logger logger = Logger.getLogger(getClass());

  public FitnessManagerOBTAlgoIPSO() {
    super();
  }

  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    logger.info("Fetching completed executions whose fitness are yet to be computed");
    List<TuningJobExecutionParamSet> completedJobExecutionParamSet = new ArrayList<TuningJobExecutionParamSet>();

    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = TuningJobExecutionParamSet.find.select("*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
        .fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
        .where()
        .or(Expr.or(Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
            JobExecution.ExecutionState.SUCCEEDED),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.FAILED)),
            Expr.eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.executionState,
                JobExecution.ExecutionState.CANCELLED))
        .isNull(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.resourceUsage)
        .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.tuningAlgorithm
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        .findList();

    logger.info("#completed executions whose metrics are not computed: " + tuningJobExecutionParamSets.size());

    getCompletedExecution(tuningJobExecutionParamSets, completedJobExecutionParamSet);

    return completedJobExecutionParamSet;
  }

  protected void computeFitness(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      TuningJobDefinition tuningJobDefinition, List<AppResult> results) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)) {
      if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
        logger.info("Execution id: " + jobExecution.id + " succeeded");
        parameterSpaceOptimization(jobSuggestedParamSet, jobExecution, results);
        updateJobSuggestedParamSetSucceededExecution(jobExecution, jobSuggestedParamSet, tuningJobDefinition);
      } else {
        // Resetting param set to created state because this case captures the scenarios when
        // either the job failed for reasons other than auto tuning or was killed/cancelled/skipped etc.
        // In all the above scenarios, fitness cannot be computed for the param set correctly.
        // Note that the penalty on failures caused by auto tuning is applied when the job execution is retried
        // after failure.
        logger.info("Execution id: " + jobExecution.id + " was not successful for reason other than tuning."
            + "Resetting param set: " + jobSuggestedParamSet.id + " to CREATED state");
        resetParamSetToCreated(jobSuggestedParamSet);
      }
    }
  }

  /*
  Todo : Move this method to TuningType
   */
  private void parameterSpaceOptimization(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      List<AppResult> results) {
    TuningTypeManagerOBT optimizeManager =
        OptimizationAlgoFactory.getOptimizationAlogrithm(jobSuggestedParamSet.tuningAlgorithm);
    if (optimizeManager != null) {
      optimizeManager.parameterOptimizer(results,jobExecution);
    }
  }

  @Override
  public String getManagerName() {
    return "FitnessManagerOBTAlgoIPSO";
  }
}
