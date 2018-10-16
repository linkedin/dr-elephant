package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.util.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
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

  /**
   *  Used to compute the fitness. It will be algorithm dependent.
   * @param jobSuggestedParamSet
   * @param jobExecution
   * @param tuningJobDefinition
   * @param results
   */
  protected abstract void computeFitness(JobSuggestedParamSet jobSuggestedParamSet, JobExecution jobExecution,
      TuningJobDefinition tuningJobDefinition, List<AppResult> results);


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







  /**
   * Checks and disables tuning for the given job definitions.
   * Tuning can be disabled if:
   *  - Number of tuning executions >=  maxTuningExecutions
   *  - or number of tuning executions >= minTuningExecutions and parameters converge
   *  - or number of tuning executions >= minTuningExecutions and median gain (in cost function) in last 6 executions is negative
   * @param jobDefinitionSet Set of jobs to check if tuning can be switched off for them
   */
@Override
  protected void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
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
      if (tuningJobExecutionParamSets.size() >= minTuningExecutions) {
        if (didParameterSetConverge(tuningJobExecutionParamSets)) {
          logger.debug("Parameters converged. Disabling tuning for job: " + jobDefinition.jobName);
          disableTuning(jobDefinition, "Parameters converged");
        } else if (isMedianGainNegative(tuningJobExecutionParamSets)) {
          logger.debug("Unable to get gain while tuning. Disabling tuning for job: " + jobDefinition.jobName);
          disableTuning(jobDefinition, "Unable to get gain");
        } else if (tuningJobExecutionParamSets.size() >= maxTuningExecutions) {
          logger.debug("Maximum tuning executions limit reached. Disabling tuning for job: " + jobDefinition.jobName);
          disableTuning(jobDefinition, "Maximum executions reached");
        }
      }
    }
  }

  /**
   * Checks if the tuning parameters converge
   * @param tuningJobExecutionParamSets List of previous executions and corresponding param sets
   * @return true if the parameters converge, else false
   */
  private boolean didParameterSetConverge(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    boolean result = false;
    int numParamSetForConvergence = 3;

    if (tuningJobExecutionParamSets.size() < numParamSetForConvergence) {
      return false;
    }

    TuningAlgorithm.JobType jobType = tuningJobExecutionParamSets.get(0).jobSuggestedParamSet.tuningAlgorithm.jobType;

    if (jobType == TuningAlgorithm.JobType.PIG) {

      Map<Integer, Set<Double>> paramValueSet = new HashMap<Integer, Set<Double>>();

      for (TuningJobExecutionParamSet tuningJobExecutionParamSet : tuningJobExecutionParamSets) {

        JobSuggestedParamSet jobSuggestedParamSet = tuningJobExecutionParamSet.jobSuggestedParamSet;

        List<JobSuggestedParamValue> jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
            .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id,
                jobSuggestedParamSet.id)
            .or(Expr.eq(JobSuggestedParamValue.TABLE.tuningParameter + '.' + TuningParameter.TABLE.paramName,
                "mapreduce.map.memory.mb"),
                Expr.eq(JobSuggestedParamValue.TABLE.tuningParameter + '.' + TuningParameter.TABLE.paramName,
                    "mapreduce.reduce.memory.mb"))
            .findList();

        // if jobSuggestedParamValueList contains both mapreduce.map.memory.mb and mapreduce.reduce.memory.mb
        // ie, if the size of jobSuggestedParamValueList is 2
        if (jobSuggestedParamValueList != null && jobSuggestedParamValueList.size() == 2) {
          numParamSetForConvergence -= 1;
          for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
            Set<Double> tmp;
            if (paramValueSet.containsKey(jobSuggestedParamValue.id)) {
              tmp = paramValueSet.get(jobSuggestedParamValue.id);
            } else {
              tmp = new HashSet<Double>();
            }
            tmp.add(jobSuggestedParamValue.paramValue);
            paramValueSet.put(jobSuggestedParamValue.id, tmp);
          }
        }

        if (numParamSetForConvergence == 0) {
          break;
        }
      }

      result = true;
      for (Integer paramId : paramValueSet.keySet()) {
        if (paramValueSet.get(paramId).size() > 1) {
          result = false;
        }
      }
    }

    if (result) {
      logger.debug("Switching off tuning for job: " + tuningJobExecutionParamSets.get(
          0).jobSuggestedParamSet.jobDefinition.jobName + " Reason: parameter set converged");
    }
    return result;
  }


  /**
   * Checks if the median gain (from tuning) during the last 6 executions is negative
   * Last 6 executions constitutes 2 iterations of PSO (given the swarm size is three). Negative average gains in
   * latest 2 algorithm iterations (after a fixed number of minimum iterations) imply that either the algorithm hasn't
   * converged or there isn't enough scope for tuning. In both the cases, switching tuning off is desired
   * @param tuningJobExecutionParamSets List of previous executions
   * @return true if the median gain is negative, else false
   */
  private boolean isMedianGainNegative(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets) {
    int numFitnessForMedian = 6;
    Double[] fitnessArray = new Double[numFitnessForMedian];
    int entries = 0;

    if (tuningJobExecutionParamSets.size() < numFitnessForMedian) {
      return false;
    }
    for (TuningJobExecutionParamSet tuningJobExecutionParamSet : tuningJobExecutionParamSets) {
      JobSuggestedParamSet jobSuggestedParamSet = tuningJobExecutionParamSet.jobSuggestedParamSet;
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      if (jobExecution.executionState == JobExecution.ExecutionState.SUCCEEDED
          && jobSuggestedParamSet.paramSetState == JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED) {
        fitnessArray[entries] = jobSuggestedParamSet.fitness;
        entries += 1;
        if (entries == numFitnessForMedian) {
          break;
        }
      }
    }
    Arrays.sort(fitnessArray);
    double medianFitness;
    if (fitnessArray.length % 2 == 0) {
      medianFitness = (fitnessArray[fitnessArray.length / 2] + fitnessArray[fitnessArray.length / 2 - 1]) / 2;
    } else {
      medianFitness = fitnessArray[fitnessArray.length / 2];
    }

    JobDefinition jobDefinition = tuningJobExecutionParamSets.get(0).jobSuggestedParamSet.jobDefinition;
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where().
        eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id).findUnique();
    double baselineFitness =
        tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;

    if (medianFitness > baselineFitness) {
      logger.debug("Switching off tuning for job: " + jobDefinition.jobName + " Reason: unable to tune enough");
      return true;
    } else {
      return false;
    }
  }

}
