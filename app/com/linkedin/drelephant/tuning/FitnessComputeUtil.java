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

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.util.Utils;
import controllers.AutoTuningMetricsController;
import java.util.ArrayList;
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
import models.JobSuggestedParamSet.ParamSetStatus;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class computes the fitness of the suggested parameters after the execution is complete. This uses
 * Dr Elephant's DB to compute the fitness.
 * Fitness is : Resource Usage/(Input Size in GB)
 * In case there is failure or resource usage/execution time goes beyond configured limit, fitness is computed by
 * adding a penalty.
 */
public class FitnessComputeUtil {
  private static final Logger logger = Logger.getLogger(FitnessComputeUtil.class);
  private static final String FITNESS_COMPUTE_WAIT_INTERVAL = "fitness.compute.wait_interval.ms";
  private static final String IGNORE_EXECUTION_WAIT_INTERVAL = "ignore.execution.wait.interval.ms";
  private static final String MAX_TUNING_EXECUTIONS = "max.tuning.executions";
  private static final String MIN_TUNING_EXECUTIONS = "min.tuning.executions";
  private int maxTuningExecutions;
  private int minTuningExecutions;
  private Long fitnessComputeWaitInterval;
  private Long ignoreExecutionWaitInterval;

  public FitnessComputeUtil() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    // Time duration to wait for computing the fitness of a param set once the corresponding execution is completed
    fitnessComputeWaitInterval =
        Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);

    // Time duration to wait for metrics (resource usage, execution time) of an execution to be computed before
    // discarding it for fitness computation
    ignoreExecutionWaitInterval =
        Utils.getNonNegativeLong(configuration, IGNORE_EXECUTION_WAIT_INTERVAL, 2 * 60 * AutoTuner.ONE_MIN);

    // #executions after which tuning will stop even if parameters don't converge
    maxTuningExecutions =
        Utils.getNonNegativeInt(configuration, MAX_TUNING_EXECUTIONS, 39);

    // #executions before which tuning cannot stop even if parameters converge
    minTuningExecutions =
        Utils.getNonNegativeInt(configuration, MIN_TUNING_EXECUTIONS, 18);
  }

  private boolean isTuningEnabled(Integer jobDefinitionId) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinitionId)
        .order()
        // There can be multiple entries in tuningJobDefinition if the job is switch on/off multiple times.
        // The latest entry gives the information regarding whether tuning is enabled or not
        .desc(TuningJobDefinition.TABLE.createdTs)
        .setMaxRows(1)
        .findUnique();

    return tuningJobDefinition != null && tuningJobDefinition.tuningEnabled;
  }

  /**
   * Updates the metrics (execution time, resource usage, cost function) of the completed executions whose metrics are
   * not computed.
   */
  public void updateFitness() {
    logger.info("Computing and updating fitness for completed executions");
    List<TuningJobExecutionParamSet> completedJobExecutionParamSets = getCompletedJobExecutionParamSets();
    updateExecutionMetrics(completedJobExecutionParamSets);
    updateMetrics(completedJobExecutionParamSets);

    Set<JobDefinition> jobDefinitionSet = new HashSet<JobDefinition>();
    for (TuningJobExecutionParamSet completedJobExecutionParamSet : completedJobExecutionParamSets) {
      JobDefinition jobDefinition = completedJobExecutionParamSet.jobSuggestedParamSet.jobDefinition;
      if (isTuningEnabled(jobDefinition.id)) {
        jobDefinitionSet.add(jobDefinition);
      }
    }
    checkToDisableTuning(jobDefinitionSet);
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
      logger.info("Switching off tuning for job: " + tuningJobExecutionParamSets.get(
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
          && jobSuggestedParamSet.paramSetState == ParamSetStatus.FITNESS_COMPUTED) {
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
      logger.info("Switching off tuning for job: " + jobDefinition.jobName + " Reason: unable to tune enough");
      return true;
    } else {
      return false;
    }
  }

  /**
   * Switches off tuning for the given job
   * @param jobDefinition Job for which tuning is to be switched off
   */
  private void disableTuning(JobDefinition jobDefinition, String reason) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .findUnique();
    if (tuningJobDefinition.tuningEnabled) {
      tuningJobDefinition.tuningEnabled = false;
      tuningJobDefinition.tuningDisabledReason = reason;
      tuningJobDefinition.save();
    }
  }

  /**
   * Checks and disables tuning for the given job definitions.
   * Tuning can be disabled if:
   *  - Number of tuning executions >=  maxTuningExecutions
   *  - or number of tuning executions >= minTuningExecutions and parameters converge
   *  - or number of tuning executions >= minTuningExecutions and median gain (in cost function) in last 6 executions is negative
   * @param jobDefinitionSet Set of jobs to check if tuning can be switched off for them
   */
  private void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
    for (JobDefinition jobDefinition : jobDefinitionSet) {
        List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
            TuningJobExecutionParamSet.find.fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*")
                .fetch(TuningJobExecutionParamSet.TABLE.jobExecution, "*")
                .where()
                .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.'
                    + JobSuggestedParamSet.TABLE.jobDefinition + '.' + JobDefinition.TABLE.id, jobDefinition.id)
                .order()
                .desc("job_execution_id")
                .findList();

        if (tuningJobExecutionParamSets.size() >= minTuningExecutions) {
          if (didParameterSetConverge(tuningJobExecutionParamSets)) {
            logger.info("Parameters converged. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Parameters converged");
          } else if (isMedianGainNegative(tuningJobExecutionParamSets)) {
            logger.info("Unable to get gain while tuning. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Unable to get gain");
          } else if (tuningJobExecutionParamSets.size() >= maxTuningExecutions) {
            logger.info("Maximum tuning executions limit reached. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Maximum executions reached");
          }
        }
    }
  }

  /**
   * This method update metrics for auto tuning monitoring for fitness compute daemon
   * @param completedJobExecutionParamSets List of completed tuning job executions
   */
  private void updateMetrics(List<TuningJobExecutionParamSet> completedJobExecutionParamSets) {
    int fitnessNotUpdated = 0;
    for (TuningJobExecutionParamSet completedJobExecutionParamSet : completedJobExecutionParamSets) {
      if (!completedJobExecutionParamSet.jobSuggestedParamSet.paramSetState.equals(ParamSetStatus.FITNESS_COMPUTED)) {
        fitnessNotUpdated++;
      } else {
        AutoTuningMetricsController.markFitnessComputedJobs();
      }
    }
    AutoTuningMetricsController.setFitnessComputeWaitJobs(fitnessNotUpdated);
  }

  /**
   * Returns the list of completed executions whose metrics are not computed
   * @return List of job execution
   */
  private List<TuningJobExecutionParamSet> getCompletedJobExecutionParamSets() {
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
          .findList();

      logger.info("#completed executions whose metrics are not computed: " + tuningJobExecutionParamSets.size());

      for (TuningJobExecutionParamSet tuningJobExecutionParamSet : tuningJobExecutionParamSets) {
        JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
        long diff = System.currentTimeMillis() - jobExecution.updatedTs.getTime();
        logger.info("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
            + jobExecution.updatedTs.getTime());
        if (diff < fitnessComputeWaitInterval) {
          logger.info("Delaying fitness compute for execution: " + jobExecution.jobExecId);
        } else {
          logger.info("Adding execution " + jobExecution.jobExecId + " to fitness computation queue");
          completedJobExecutionParamSet.add(tuningJobExecutionParamSet);
        }
      }
    logger.info(
        "Number of completed execution fetched for fitness computation: " + completedJobExecutionParamSet.size());
    return completedJobExecutionParamSet;
  }

  /**
   * Updates the execution metrics
   * @param completedJobExecutionParamSets List of completed executions
   */
  private void updateExecutionMetrics(List<TuningJobExecutionParamSet> completedJobExecutionParamSets) {
    for (TuningJobExecutionParamSet completedJobExecutionParamSet : completedJobExecutionParamSets) {

      JobExecution jobExecution = completedJobExecutionParamSet.jobExecution;
      JobSuggestedParamSet jobSuggestedParamSet = completedJobExecutionParamSet.jobSuggestedParamSet;
      JobDefinition job = jobExecution.job;

      logger.info("Updating execution metrics and fitness for execution: " + jobExecution.jobExecId);
      try {
        TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
            .fetch(TuningJobDefinition.TABLE.job, "*")
            .where()
            .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
            .order()
            .desc(TuningJobDefinition.TABLE.createdTs)
            .findUnique();

        List<AppResult> results = AppResult.find.select("*")
            .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
            .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
                "*")
            .where()
            .eq(AppResult.TABLE.FLOW_EXEC_ID, jobExecution.flowExecution.flowExecId)
            .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecution.jobExecId)
            .findList();

        if (results != null && results.size() > 0) {
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
            jobExecution.executionTime = totalExecutionTime * 1.0 / (1000 * 60);
            jobExecution.resourceUsage = totalResourceUsed * 1.0 / (1024 * 3600);
            jobExecution.inputSizeInBytes = totalInputBytesInBytes;
            jobExecution.update();
            logger.info(
                "Metric Values for execution " + jobExecution.jobExecId + ": Execution time = " + totalExecutionTime
                    + ", Resource usage = " + totalResourceUsed + " and total input size = " + totalInputBytesInBytes);
          }

          if (tuningJobDefinition.averageResourceUsage == null && totalExecutionTime != 0) {
            tuningJobDefinition.averageResourceUsage = jobExecution.resourceUsage;
            tuningJobDefinition.averageExecutionTime = jobExecution.executionTime;
            tuningJobDefinition.averageInputSizeInBytes = jobExecution.inputSizeInBytes.longValue();
            tuningJobDefinition.update();
          }

          //Compute fitness

          if (!jobSuggestedParamSet.paramSetState.equals(ParamSetStatus.FITNESS_COMPUTED)) {
            if (jobExecution.executionState.equals(JobExecution.ExecutionState.SUCCEEDED)) {
              logger.info("Execution id: " + jobExecution.id + " succeeded");
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
        } else {
          long diff = System.currentTimeMillis() - jobExecution.updatedTs.getTime();
          logger.debug("Current Time in millis: " + System.currentTimeMillis() + ", job execution last updated time "
              + jobExecution.updatedTs.getTime());
          if (diff > ignoreExecutionWaitInterval) {
            logger.info("Fitness of param set " + jobSuggestedParamSet.id  + " corresponding to execution id: " +
                jobExecution.id + " not computed for more than the maximum duration specified to compute fitness. "
                + "Resetting the param set to CREATED state");
            resetParamSetToCreated(jobSuggestedParamSet);
          }
        }
      } catch (Exception e) {
        logger.error("Error updating fitness of execution: " + jobExecution.id + "\n Stacktrace: ", e);
      }
    }
    logger.info("Execution metrics updated");
  }

  /**
   * Resets the param set to CREATED state if its fitness is not already computed
   * @param jobSuggestedParamSet Param set which is to be reset
   */
  private void resetParamSetToCreated(JobSuggestedParamSet jobSuggestedParamSet) {
    if (!jobSuggestedParamSet.paramSetState.equals(ParamSetStatus.FITNESS_COMPUTED)) {
      logger.info("Resetting parameter set to created: " + jobSuggestedParamSet.id);
      jobSuggestedParamSet.paramSetState = ParamSetStatus.CREATED;
      jobSuggestedParamSet.save();
    }
  }

  /**
   * Updates the job suggested param set when the corresponding execution was succeeded
   * @param jobExecution JobExecution: succeeded job execution corresponding to the param set which is to be updated
   * @param jobSuggestedParamSet param set which is to be updated
   * @param tuningJobDefinition TuningJobDefinition of the job to which param set corresponds
   */
  private void updateJobSuggestedParamSetSucceededExecution(JobExecution jobExecution,
      JobSuggestedParamSet jobSuggestedParamSet, TuningJobDefinition tuningJobDefinition) {
    int penaltyConstant = 3;
    Double averageResourceUsagePerGBInput =
        tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
    Double maxDesiredResourceUsagePerGBInput =
        averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;
    Double averageExecutionTimePerGBInput =
        tuningJobDefinition.averageExecutionTime * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
    Double maxDesiredExecutionTimePerGBInput =
        averageExecutionTimePerGBInput * tuningJobDefinition.allowedMaxExecutionTimePercent / 100.0;
    Double resourceUsagePerGBInput = jobExecution.resourceUsage * FileUtils.ONE_GB / jobExecution.inputSizeInBytes;
    Double executionTimePerGBInput = jobExecution.executionTime * FileUtils.ONE_GB / jobExecution.inputSizeInBytes;

    if (resourceUsagePerGBInput > maxDesiredResourceUsagePerGBInput
        || executionTimePerGBInput > maxDesiredExecutionTimePerGBInput) {
      logger.info("Execution " + jobExecution.jobExecId + " violates constraint on resource usage per GB input");
      jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
    } else {
      jobSuggestedParamSet.fitness = resourceUsagePerGBInput;
    }
    jobSuggestedParamSet.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
    jobSuggestedParamSet.fitnessJobExecution = jobExecution;
    jobSuggestedParamSet = updateBestJobSuggestedParamSet(jobSuggestedParamSet);
    jobSuggestedParamSet.update();
  }

  /**
   * Updates the given job suggested param set to be the best param set if its fitness is less than the current best param set
   * (since the objective is to minimize the fitness, the param set with the lowest fitness is the best)
   * @param jobSuggestedParamSet JobSuggestedParamSet
   */
  private JobSuggestedParamSet updateBestJobSuggestedParamSet(JobSuggestedParamSet jobSuggestedParamSet) {
    logger.info("Checking if a new best param set is found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId);
    JobSuggestedParamSet currentBestJobSuggestedParamSet = JobSuggestedParamSet.find.where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id,
            jobSuggestedParamSet.jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 1)
        .findUnique();
    if (currentBestJobSuggestedParamSet != null) {
      if (currentBestJobSuggestedParamSet.fitness > jobSuggestedParamSet.fitness) {
        logger.info("Param set: " + jobSuggestedParamSet.id + " is the new best param set for job: " + jobSuggestedParamSet.jobDefinition.jobDefId);
        currentBestJobSuggestedParamSet.isParamSetBest = false;
        jobSuggestedParamSet.isParamSetBest = true;
        currentBestJobSuggestedParamSet.save();
      }
    } else {
      logger.info("No best param set found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId
          + ". Marking current param set " + jobSuggestedParamSet.id + " as best");
      jobSuggestedParamSet.isParamSetBest = true;
    }
    return jobSuggestedParamSet;
  }

  /**
   * Returns the total input size
   * @param appResult appResult
   * @return total input size
   */
  private Long getTotalInputBytes(AppResult appResult) {
    Long totalInputBytes = 0L;
    if (appResult.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult.heuristicName.equals(CommonConstantsHeuristic.MAPPER_SPEED)) {
          if (appHeuristicResult.yarnAppHeuristicResultDetails != null) {
            for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
              if (appHeuristicResultDetails.name.equals(CommonConstantsHeuristic.TOTAL_INPUT_SIZE_IN_MB)) {
                totalInputBytes += Math.round(Double.parseDouble(appHeuristicResultDetails.value) * FileUtils.ONE_MB);
              }
            }
          }
        }
      }
    }
    return totalInputBytes;
  }
}
