package com.linkedin.drelephant.tuning.hbt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;


public class FitnessManagerHBT extends AbstractFitnessManager {
  private final Logger logger = Logger.getLogger(getClass());

  public FitnessManagerHBT() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);

    // Time duration to wait for metrics (resource usage, execution time) of an execution to be computed before
    // discarding it for fitness computation
    ignoreExecutionWaitInterval =
        Utils.getNonNegativeLong(configuration, IGNORE_EXECUTION_WAIT_INTERVAL, 2 * 60 * AutoTuner.ONE_MIN);

    // #executions after which tuning will stop even if parameters don't converge
    maxTuningExecutions = Utils.getNonNegativeInt(configuration, MAX_TUNING_EXECUTIONS, 10);

    // #executions before which tuning cannot stop even if parameters converge
    minTuningExecutions = Utils.getNonNegativeInt(configuration, MIN_TUNING_EXECUTIONS, 2);
  }

  @Override
  protected List<TuningJobExecutionParamSet> detectJobsForFitnessComputation() {
    logger.debug("Fetching completed executions whose fitness are yet to be computed");
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
            + "." + TuningAlgorithm.TABLE.optimizationAlgo, TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();

    logger.debug("#completed executions whose metrics are not computed: " + tuningJobExecutionParamSets.size());

    getCompletedExecution(tuningJobExecutionParamSets, completedJobExecutionParamSet);

    return completedJobExecutionParamSet;
  }

  @Override
  protected void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet) {
    logger.debug("calculateAndUpdateFitness");
    Double totalResourceUsed = 0D;
    Double totalInputBytesInBytes = 0D;
    Double score=0D;
    for (AppResult appResult : results) {
      totalResourceUsed += appResult.resourceUsed;
      totalInputBytesInBytes += getTotalInputBytes(appResult);
      score += appResult.score;
    }

    Long totalRunTime = Utils.getTotalRuntime(results);
    Long totalDelay = Utils.getTotalWaittime(results);
    Long totalExecutionTime = totalRunTime - totalDelay;

    if (totalExecutionTime != 0) {
      jobExecution.score=score;
      updateJobExecution(jobExecution, totalResourceUsed, totalInputBytesInBytes, totalExecutionTime);
    }

    if (tuningJobDefinition.averageResourceUsage == null && totalExecutionTime != 0) {
      updateTuningJobDefinition(tuningJobDefinition, jobExecution);
    }

    //Compute fitness
    computeFitness(jobSuggestedParamSet, jobExecution, tuningJobDefinition, results);
  }

  protected void computeFitness(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      TuningJobDefinition tuningJobDefinition, List<AppResult> results) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
        || !jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
      if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
        logger.debug("Execution id: " + jobExecution.id + " succeeded");
        updateJobSuggestedParamSetSucceededExecution(jobExecution, jobSuggestedParamSet, tuningJobDefinition);
      } else {
        // Resetting param set to created state because this case captures the scenarios when
        // either the job failed for reasons other than auto tuning or was killed/cancelled/skipped etc.
        // In all the above scenarios, fitness cannot be computed for the param set correctly.
        // Note that the penalty on failures caused by auto tuning is applied when the job execution is retried
        // after failure.
        logger.debug("HBT Execution id: " + jobExecution.id + " was not successful for reason other than tuning."
            + "Resetting param set: " + jobSuggestedParamSet.id + " to CREATED state");
        resetParamSetToCreated(jobSuggestedParamSet);
      }
    }
  }

  /**
   * Updates the job suggested param set when the corresponding execution was succeeded
   * @param jobExecution JobExecution: succeeded job execution corresponding to the param set which is to be updated
   * @param jobSuggestedParamSet param set which is to be updated
   * @param tuningJobDefinition TuningJobDefinition of the job to which param set corresponds
   */
  protected void updateJobSuggestedParamSetSucceededExecution(JobExecution jobExecution,
      JobSuggestedParamSet jobSuggestedParamSet, TuningJobDefinition tuningJobDefinition) {
    jobSuggestedParamSet.fitness = jobExecution.score;
    jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED;
    jobSuggestedParamSet.fitnessJobExecution = jobExecution;
    jobSuggestedParamSet = updateBestJobSuggestedParamSet(jobSuggestedParamSet);
    jobSuggestedParamSet.update();
  }
  /**
   * Resets the param set to CREATED state if its fitness is not already computed
   * @param jobSuggestedParamSet Param set which is to be reset
   */
  protected void resetParamSetToCreated(JobSuggestedParamSet jobSuggestedParamSet) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
        && !jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.DISCARDED)) {
      logger.debug("Resetting parameter set to created: " + jobSuggestedParamSet.id);
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
      jobSuggestedParamSet.save();
    }
  }

  @Override
  protected void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
    Long currentTimeBefore = System.currentTimeMillis();
    for (JobDefinition jobDefinition : jobDefinitionSet) {
      List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
          TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
              .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
              .where()
              .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.jobDefinition
                  + '.' + JobDefinition.TABLE.id, jobDefinition.id)
              .order()
              .desc("job_execution_id")
              .findList();

      if (reachToNumberOfThresholdIterations(tuningJobExecutionParamSets, jobDefinition)) {
        disableTuning(jobDefinition, "User Specified Iterations reached");
      }
      if (areHeuristicsPassed(tuningJobExecutionParamSets)) {
        disableTuning(jobDefinition, "All Heuristics Passed1");
      }
    }
    Long currentTimeAfter= System.currentTimeMillis();
    logger.info(" Total time taken by disable tuning " + (currentTimeAfter-currentTimeBefore));
  }

  private boolean areHeuristicsPassed(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    logger.debug(" Testing All Heuristics ");
    if (tuningJobExecutionParamSets != null && tuningJobExecutionParamSets.size() >= 1) {
      logger.debug("tuningJobExecutionParamSets have some values");
      TuningJobExecutionParamSet tuningJobExecutionParamSet = tuningJobExecutionParamSets.get(0);
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      List<AppResult> results = getAppResult(jobExecution);
      if (results != null) {
        logger.debug(" Results are not null ");
        return areAppResultsHaveSeverity(results);
      } else {
        logger.debug(" App Results are null ");
        return false;
      }
    } else {
      logger.debug(" Tuning Job Execution Param Set is null ");
      return false;
    }
  }

  private boolean areAppResultsHaveSeverity(List<AppResult> results) {
    List<String> heuristicsWithHighSeverity = new ArrayList<String>();
    for (AppResult appResult : results) {
      if (appResult.yarnAppHeuristicResults != null) {
        for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
          if (appHeuristicResult.severity.getValue() == 3 || appHeuristicResult.severity.getValue() == 4) {
            heuristicsWithHighSeverity.add(
                appResult.id + "\t" + appHeuristicResult.heuristicName + "\t" + "have high severity" + "\t"
                    + appHeuristicResult.severity.getValue());
          }
        }
      } else {
        logger.debug(appResult.id + " " + appResult.jobDefId + " have yarn app result null ");
        return true;
      }
    }
    return checkHeuriticsforSeverity(heuristicsWithHighSeverity);
  }

  private boolean checkHeuriticsforSeverity(List<String> heuristicsWithHighSeverity) {
    if (heuristicsWithHighSeverity.size() == 0) {
      logger.debug(" No severe heursitics ");
      return true;
    } else {
      for (String failedHeuristics : heuristicsWithHighSeverity) {
        logger.debug(failedHeuristics);
      }
      return false;
    }
  }

  private List<AppResult> getAppResult(JobExecution jobExecution) {
    List<AppResult> results = null;
    try {
      results = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, jobExecution.flowExecution.flowExecId)
          .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecution.jobExecId)
          .findList();
    } catch (Exception e) {
      logger.warn(" Job Analysis is not completed . ");
    }
    return results;
  }

  @Override
  public String getManagerName() {
    return "FitnessManagerHBT";
  }
}
