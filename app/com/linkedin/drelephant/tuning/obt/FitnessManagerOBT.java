package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;
import java.util.List;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

public abstract class FitnessManagerOBT extends AbstractFitnessManager {
  private final Logger logger = Logger.getLogger(getClass());

  public FitnessManagerOBT() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);

    // Time duration to wait for metrics (resource usage, execution time) of an execution to be computed before
    // discarding it for fitness computation
    ignoreExecutionWaitInterval =
        Utils.getNonNegativeLong(configuration, IGNORE_EXECUTION_WAIT_INTERVAL, 2 * 60 * AutoTuner.ONE_MIN);

    // #executions after which tuning will stop even if parameters don't converge
    maxTuningExecutions = Utils.getNonNegativeInt(configuration, MAX_TUNING_EXECUTIONS, 39);

    // #executions before which tuning cannot stop even if parameters converge
    minTuningExecutions = Utils.getNonNegativeInt(configuration, MIN_TUNING_EXECUTIONS, 18);
  }

  @Override
  protected void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet) {
    Double totalResourceUsed = 0D;
    Double totalInputBytesInBytes = 0D;
    for (AppResult appResult : results) {
      totalResourceUsed += appResult.resourceUsed;
      totalInputBytesInBytes += getTotalInputBytes(appResult);
    }

    Long totalRunTime = Utils.getTotalRuntime(results);
    Long totalDelay = Utils.getTotalWaittime(results);
    Long totalExecutionTime = totalRunTime - totalDelay;

    if (totalExecutionTime != 0) {
      updateJobExecution(jobExecution, totalResourceUsed, totalInputBytesInBytes, totalExecutionTime);
    }

    if (tuningJobDefinition.averageResourceUsage == null && totalExecutionTime != 0) {
      updateTuningJobDefinition(tuningJobDefinition, jobExecution);
    }

    //Compute fitness
    computeFitness(jobSuggestedParamSet, jobExecution, tuningJobDefinition, results);
  }



  protected abstract void computeFitness(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      TuningJobDefinition tuningJobDefinition, List<AppResult> results);




}
