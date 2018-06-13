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
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecution;
import models.TuningJobExecution.ParamSetStatus;
import models.TuningParameter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class computes the fitness of the suggested parameters after the execution is complete. This uses
 * Dr Elephant's DB to compute the fitness.
 * Fitness is : Resource Usage/Input Size in GB
 * In case there is failure or resource usage/execution time goes beyond configured limit, fitness is computed by
 * adding a penalty.
 */
public class FitnessComputeUtil {
  private static final Logger logger = Logger.getLogger(FitnessComputeUtil.class);
  private static final String FITNESS_COMPUTE_WAIT_INTERVAL = "fitness.compute.wait_interval.ms";
  private static final int MAX_TUNING_EXECUTIONS = 39;
  private static final int MIN_TUNING_EXECUTIONS = 18;
  private Long waitInterval;

  public FitnessComputeUtil() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    waitInterval = Utils.getNonNegativeLong(configuration, FITNESS_COMPUTE_WAIT_INTERVAL, 5 * AutoTuner.ONE_MIN);
  }

  /**
   * Updates the metrics (execution time, resource usage, cost function) of the completed executions whose metrics are
   * not computed.
   */
  public void updateFitness() {
    logger.info("Computing and updating fitness for completed executions");
    List<TuningJobExecution> completedExecutions = getCompletedExecutions();
    updateExecutionMetrics(completedExecutions);
    updateMetrics(completedExecutions);

    Set<JobDefinition> jobDefinitionSet = new HashSet<JobDefinition>();
    for (TuningJobExecution tuningJobExecution : completedExecutions) {
      jobDefinitionSet.add(tuningJobExecution.jobExecution.job);
    }
    checkToDisableTuning(jobDefinitionSet);
  }

  /**
   * Checks if the tuning parameters converge
   * @param jobExecutions List of previous executions on which parameter convergence is to be checked
   * @return true if the parameters converge, else false
   */
  private boolean doesParameterSetConverge(List<JobExecution> jobExecutions) {
    boolean result = false;
    int num_param_set_for_convergence = 3;

    TuningJobExecution tuningJobExecution = TuningJobExecution.find.where()
        .eq(TuningJobExecution.TABLE.jobExecution + '.' + JobExecution.TABLE.id, jobExecutions.get(0).id)
        .findUnique();
    TuningAlgorithm.JobType jobType = tuningJobExecution.tuningAlgorithm.jobType;

    if (jobType == TuningAlgorithm.JobType.PIG) {
      Map<Integer, Set<Double>> paramValueSet = new HashMap<Integer, Set<Double>>();
      for (JobExecution jobExecution : jobExecutions) {
        List<JobSuggestedParamValue> jobSuggestedParamValueList = new ArrayList<JobSuggestedParamValue>();
        try {
          jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
              .eq(JobSuggestedParamValue.TABLE.jobExecution + '.' + JobExecution.TABLE.id, jobExecution.id)
              .or(Expr.eq(JobSuggestedParamValue.TABLE.tuningParameter + '.' + TuningParameter.TABLE.id, 2),
                  Expr.eq(JobSuggestedParamValue.TABLE.tuningParameter + '.' + TuningParameter.TABLE.id, 5))
              .findList();
        } catch (NullPointerException e) {
          logger.info("Checking param convergence: Map memory and reduce memory parameter not found");
        }
        if (jobSuggestedParamValueList.size() > 0) {
          num_param_set_for_convergence -= 1;
          for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
            Set tmp;
            if (paramValueSet.containsKey(jobSuggestedParamValue.id)) {
              tmp = paramValueSet.get(jobSuggestedParamValue.id);
            } else {
              tmp = new HashSet();
            }
            tmp.add(jobSuggestedParamValue.paramValue);
            paramValueSet.put(jobSuggestedParamValue.id, tmp);
          }
        }

        if (num_param_set_for_convergence == 0) {
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
      logger.info(
          "Switching off tuning for job: " + jobExecutions.get(0).job.jobName + " Reason: parameter set converged");
    }
    return result;
  }

  /**
   * Checks if the median gain (from tuning) during the last 6 executions is negative
   * Last 6 executions constitutes 2 iterations of PSO (given the swarm size is three). Negative average gains in
   * latest 2 algorithm iterations (after a fixed number of minimum iterations) imply that either the algorithm hasn't
   * converged or there isn't enough scope for tuning. In both the cases, switching tuning off is desired
   * @param jobExecutions List of previous executions
   * @return true if the median gain is negative, else false
   */
  private boolean isMedianGainNegative(List<JobExecution> jobExecutions) {
    int num_fitness_for_median = 6;
    Double[] fitnessArray = new Double[num_fitness_for_median];
    int entries = 0;
    for (JobExecution jobExecution : jobExecutions) {
      TuningJobExecution tuningJobExecution = TuningJobExecution.find.where()
          .eq(TuningJobExecution.TABLE.jobExecution + '.' + JobExecution.TABLE.id, jobExecution.id)
          .findUnique();
      if (jobExecution.executionState == JobExecution.ExecutionState.SUCCEEDED
          && tuningJobExecution.paramSetState == ParamSetStatus.FITNESS_COMPUTED) {
        fitnessArray[entries] = tuningJobExecution.fitness;
        entries += 1;
        if (entries == num_fitness_for_median) {
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

    JobDefinition jobDefinition = jobExecutions.get(0).job;
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where().
        eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id).findUnique();
    double baselineFitness =
        tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;

    if (medianFitness > baselineFitness) {
      logger.info(
          "Switching off tuning for job: " + jobExecutions.get(0).job.jobName + " Reason: unable to tune enough");
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
    if (tuningJobDefinition.tuningEnabled == 1) {
      logger.info("Disabling tuning for job: " + tuningJobDefinition.job.jobDefId);
      tuningJobDefinition.tuningEnabled = 0;
      tuningJobDefinition.tuningDisabledReason = reason;
      tuningJobDefinition.save();
    }
  }

  /**
   * Checks and disables tuning for the given job definitions.
   * Tuning can be disabled if:
   *  - Number of tuning executions >=  MAX_TUNING_EXECUTIONS
   *  - or number of tuning executions >= MIN_TUNING_EXECUTIONS and parameters converge
   *  - or number of tuning executions >= MIN_TUNING_EXECUTIONS and median gain (in cost function) in last 6 executions is negative
   * @param jobDefinitionSet Set of jobs to check if tuning can be switched off for them
   */
  private void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet) {
    for (JobDefinition jobDefinition : jobDefinitionSet) {
      try {
        List<JobExecution> jobExecutions = JobExecution.find.where()
            .eq(JobExecution.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
            .isNotNull(JobExecution.TABLE.jobExecId)
            .orderBy("id desc")
            .findList();
        if (jobExecutions.size() >= MIN_TUNING_EXECUTIONS) {
          if (doesParameterSetConverge(jobExecutions)) {
            logger.info("Parameters converged. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Parameters converged");
          } else if (isMedianGainNegative(jobExecutions)) {
            logger.info("Unable to get gain while tuning. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Unable to get gain");
          } else if (jobExecutions.size() >= MAX_TUNING_EXECUTIONS) {
            logger.info("Maximum tuning executions limit reached. Disabling tuning for job: " + jobDefinition.jobName);
            disableTuning(jobDefinition, "Maximum executions reached");
          }
        }
      } catch (NullPointerException e) {
        logger.info("No execution found for job: " + jobDefinition.jobName);
      }
    }
  }

  /**
   * This method update metrics for auto tuning monitoring for fitness compute daemon
   * @param completedExecutions List of completed tuning job executions
   */
  private void updateMetrics(List<TuningJobExecution> completedExecutions) {
    int fitnessNotUpdated = 0;
    for (TuningJobExecution tuningJobExecution : completedExecutions) {
      if (!tuningJobExecution.paramSetState.equals(ParamSetStatus.FITNESS_COMPUTED)) {
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
  private List<TuningJobExecution> getCompletedExecutions() {
    logger.info("Fetching completed executions whose fitness are yet to be computed");
    List<TuningJobExecution> jobExecutions = new ArrayList<TuningJobExecution>();
    List<TuningJobExecution> outputJobExecutions = new ArrayList<TuningJobExecution>();

    try {
      jobExecutions = TuningJobExecution.find.select("*")
          .where()
          .eq(TuningJobExecution.TABLE.paramSetState, ParamSetStatus.EXECUTED)
          .findList();

      for (TuningJobExecution tuningJobExecution : jobExecutions) {
        long diff = System.currentTimeMillis() - tuningJobExecution.jobExecution.updatedTs.getTime();
        logger.debug("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
            + tuningJobExecution.jobExecution.updatedTs.getTime());
        if (diff < waitInterval) {
          logger.debug("Delaying fitness compute for execution: " + tuningJobExecution.jobExecution.jobExecId);
        } else {
          logger.debug("Adding execution " + tuningJobExecution.jobExecution.jobExecId + " for fitness computation");
          outputJobExecutions.add(tuningJobExecution);
        }
      }
    } catch (NullPointerException e) {
      logger.error("No completed execution found for which fitness is to be computed", e);
    }
    logger.info("Number of completed execution fetched for fitness computation: " + outputJobExecutions.size());
    logger.debug("Finished fetching completed executions for fitness computation");
    return outputJobExecutions;
  }

  /**
   * Updates the execution metrics
   * @param completedExecutions List of completed executions
   */
  private void updateExecutionMetrics(List<TuningJobExecution> completedExecutions) {

    //To artificially increase the cost function value 3 times (as a penalty) in case of metric value violation
    Integer penaltyConstant = 3;

    for (TuningJobExecution tuningJobExecution : completedExecutions) {
      logger.info("Updating execution metrics and fitness for execution: " + tuningJobExecution.jobExecution.jobExecId);
      try {
        JobExecution jobExecution = tuningJobExecution.jobExecution;
        JobDefinition job = jobExecution.job;

        // job id match and tuning enabled
        TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
            .fetch(TuningJobDefinition.TABLE.job, "*")
            .where()
            .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
            .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
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
          Long totalExecutionTime = 0L;
          Double totalResourceUsed = 0D;
          Double totalInputBytesInBytes = 0D;

          Map<String, Double> counterValuesMap = new HashMap<String, Double>();

          for (AppResult appResult : results) {
            totalResourceUsed += appResult.resourceUsed;
            totalInputBytesInBytes += getTotalInputBytes(appResult);
          }

          Long totalRunTime = Utils.getTotalRuntime(results);
          Long totalDelay = Utils.getTotalWaittime(results);
          totalExecutionTime = totalRunTime - totalDelay;

          if (totalExecutionTime != 0) {
            jobExecution.executionTime = totalExecutionTime * 1.0 / (1000 * 60);
            jobExecution.resourceUsage = totalResourceUsed * 1.0 / (1024 * 3600);
            jobExecution.inputSizeInBytes = totalInputBytesInBytes;

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
          Double averageResourceUsagePerGBInput =
              tuningJobDefinition.averageResourceUsage * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
          Double maxDesiredResourceUsagePerGBInput =
              averageResourceUsagePerGBInput * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0;
          Double averageExecutionTimePerGBInput =
              tuningJobDefinition.averageExecutionTime * FileUtils.ONE_GB / tuningJobDefinition.averageInputSizeInBytes;
          Double maxDesiredExecutionTimePerGBInput =
              averageExecutionTimePerGBInput * tuningJobDefinition.allowedMaxExecutionTimePercent / 100.0;
          Double resourceUsagePerGBInput =
              jobExecution.resourceUsage * FileUtils.ONE_GB / jobExecution.inputSizeInBytes;
          Double executionTimePerGBInput =
              jobExecution.executionTime * FileUtils.ONE_GB / jobExecution.inputSizeInBytes;

          if (resourceUsagePerGBInput > maxDesiredResourceUsagePerGBInput
              || executionTimePerGBInput > maxDesiredExecutionTimePerGBInput) {
            logger.info("Execution " + jobExecution.jobExecId + " violates constraint on resource usage per GB input");
            tuningJobExecution.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
          } else {
            tuningJobExecution.fitness = resourceUsagePerGBInput;
          }
          tuningJobExecution.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
          jobExecution.update();
          tuningJobExecution.update();
        }

        TuningJobExecution currentBestTuningJobExecution;
        try {
          currentBestTuningJobExecution =
              TuningJobExecution.find.where().eq("jobExecution.job.id", tuningJobExecution.jobExecution.job.id).
                  eq(TuningJobExecution.TABLE.isParamSetBest, 1).findUnique();
          if (currentBestTuningJobExecution.fitness > tuningJobExecution.fitness) {
            currentBestTuningJobExecution.isParamSetBest = false;
            tuningJobExecution.isParamSetBest = true;
            currentBestTuningJobExecution.save();
            tuningJobExecution.save();
          }
        } catch (NullPointerException e) {
          tuningJobExecution.isParamSetBest = true;
          tuningJobExecution.save();
        }
      } catch (Exception e) {
        logger.error("Error updating fitness of execution: " + tuningJobExecution.jobExecution.id + "\n Stacktrace: ",
            e);
      }
    }
    logger.info("Execution metrics updated");
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
