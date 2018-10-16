package com.linkedin.drelephant.tuning;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import controllers.AutoTuningMetricsController;
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


/**
 * This class computes the fitness of the suggested parameters after the execution is complete. This uses
 * Dr Elephant's DB to compute the fitness.
 * Fitness is : Resource Usage/(Input Size in GB)
 * In case there is failure or resource usage/execution time goes beyond configured limit, fitness is computed by
 * adding a penalty.
 */

public abstract class AbstractFitnessManager implements Manager {
  private final Logger logger = Logger.getLogger(getClass());
  boolean debugEnabled = logger.isDebugEnabled();
  protected final String FITNESS_COMPUTE_WAIT_INTERVAL = "fitness.compute.wait_interval.ms";
  protected final String IGNORE_EXECUTION_WAIT_INTERVAL = "ignore.execution.wait.interval.ms";
  protected final String MAX_TUNING_EXECUTIONS = "max.tuning.executions";
  protected final String MIN_TUNING_EXECUTIONS = "min.tuning.executions";
  private static final int MIN_TO_MS = (1000 * 60);
  private static final int HOUR_TO_SEC = 3600;
  //private stati
  protected int maxTuningExecutions;
  protected int minTuningExecutions;
  protected Long fitnessComputeWaitInterval;
  protected Long ignoreExecutionWaitInterval;
  private Map<Integer, TuningJobDefinition> tuningDefintionCollection = new HashMap<Integer, TuningJobDefinition>();

  /**
   * Detects  & return the jobs for which fitness computation have to be done.
   * @return
   */
  protected abstract List<TuningJobExecutionParamSet> detectJobsForFitnessComputation();

  /**
   * This method is used to calculate the fitness and update into DB.
   * @param jobExecution : Job Execution for which fitness need to be computed
   * @param results : Heuristics App results for the same.
   * @param tuningJobDefinition : Tuning job defination of the job.
   * @param jobSuggestedParamSet : Status of the suggested param set , so that fitness can be computed.
   */
  protected abstract void calculateAndUpdateFitness(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet);

  /**
   *  This methods disable tuning , if certain prereuistes matched.
   * @param jobDefinitionSet
   */
  protected abstract void checkToDisableTuning(Set<JobDefinition> jobDefinitionSet);

  protected boolean calculateFitness(List<TuningJobExecutionParamSet> completedJobExecutionParamSets) {
    for (TuningJobExecutionParamSet completedJobExecutionParamSet : completedJobExecutionParamSets) {
      JobExecution jobExecution = completedJobExecutionParamSet.jobExecution;
      JobSuggestedParamSet jobSuggestedParamSet = completedJobExecutionParamSet.jobSuggestedParamSet;
      JobDefinition job = jobExecution.job;
      logger.debug("Updating execution metrics and fitness for execution: " + jobExecution.jobExecId);
      try {
        TuningJobDefinition tuningJobDefinition = getTuningJobDefinition(job);
        List<AppResult> results = getAppResults(jobExecution);
        handleFitnessCalculation(jobExecution, results, tuningJobDefinition, jobSuggestedParamSet);
      } catch (Exception e) {
        logger.error("Error updating fitness of execution: " + jobExecution.id + "\n Stacktrace: ", e);
        return false;
      }
    }

    logger.debug("Execution metrics updated");
    return true;
  }

  private TuningJobDefinition getTuningJobDefinition(JobDefinition job) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
        .order()
        .desc(TuningJobDefinition.TABLE.createdTs)
        .setMaxRows(1)
        .findUnique();
    tuningDefintionCollection.put(tuningJobDefinition.job.id, tuningJobDefinition);
    return tuningJobDefinition;
  }

  private List<AppResult> getAppResults(JobExecution jobExecution) {
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, jobExecution.flowExecution.flowExecId)
        .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecution.jobExecId)
        .findList();
    return results;
  }

  private void handleFitnessCalculation(JobExecution jobExecution, List<AppResult> results,
      TuningJobDefinition tuningJobDefinition, JobSuggestedParamSet jobSuggestedParamSet) {
    if (results != null && results.size() > 0) {
      calculateAndUpdateFitness(jobExecution, results, tuningJobDefinition, jobSuggestedParamSet);
    } else {
      handleEmptyResultScenario(jobExecution, jobSuggestedParamSet);
    }
  }

  protected void updateJobExecution(JobExecution jobExecution, Double totalResourceUsed, Double totalInputBytesInBytes,
      Long totalExecutionTime) {
    jobExecution.executionTime = totalExecutionTime * 1.0 / MIN_TO_MS;
    jobExecution.resourceUsage = totalResourceUsed * 1.0 / (FileUtils.ONE_KB * HOUR_TO_SEC);
    jobExecution.inputSizeInBytes = totalInputBytesInBytes;
    jobExecution.update();
    logger.debug("Metric Values for execution " + jobExecution.jobExecId + ": Execution time = " + totalExecutionTime
        + ", Resource usage = " + totalResourceUsed + " and total input size = " + totalInputBytesInBytes);
  }

  protected void updateTuningJobDefinition(TuningJobDefinition tuningJobDefinition, JobExecution jobExecution) {
    tuningJobDefinition.averageResourceUsage = jobExecution.resourceUsage;
    tuningJobDefinition.averageExecutionTime = jobExecution.executionTime;
    tuningJobDefinition.averageInputSizeInBytes = jobExecution.inputSizeInBytes.longValue();
    tuningJobDefinition.update();
  }

  private void handleEmptyResultScenario(JobExecution jobExecution, JobSuggestedParamSet jobSuggestedParamSet) {
    long diff = System.currentTimeMillis() - jobExecution.updatedTs.getTime();
    logger.debug("Current Time in millis: " + System.currentTimeMillis() + ", job execution last updated time "
        + jobExecution.updatedTs.getTime());
    if (diff > ignoreExecutionWaitInterval) {
      logger.debug(
          "Fitness of param set " + jobSuggestedParamSet.id + " corresponding to execution id: " + jobExecution.id
              + " not computed for more than the maximum duration specified to compute fitness. "
              + "Resetting the param set to CREATED state");
      resetParamSetToCreated(jobSuggestedParamSet);
    }
  }

  /**
   * Returns the total input size
   * @param appResult appResult
   * @return total input size
   */
  protected Long getTotalInputBytes(AppResult appResult) {
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

  /**
   * Resets the param set to CREATED state if its fitness is not already computed
   * @param jobSuggestedParamSet Param set which is to be reset
   */
  protected void resetParamSetToCreated(JobSuggestedParamSet jobSuggestedParamSet) {
    if (!jobSuggestedParamSet.paramSetState.equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)) {
      logger.debug("Resetting parameter set to created: " + jobSuggestedParamSet.id);
      jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
      jobSuggestedParamSet.save();
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
      logger.debug("Execution " + jobExecution.jobExecId + " violates constraint on resource usage per GB input");
      jobSuggestedParamSet.fitness = penaltyConstant * maxDesiredResourceUsagePerGBInput;
    } else {
      jobSuggestedParamSet.fitness = resourceUsagePerGBInput;
    }
    jobSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED;
    jobSuggestedParamSet.fitnessJobExecution = jobExecution;
    jobSuggestedParamSet = updateBestJobSuggestedParamSet(jobSuggestedParamSet);
    jobSuggestedParamSet.update();
  }

  /**
   * Updates the given job suggested param set to be the best param set if its fitness is less than the current best param set
   * (since the objective is to minimize the fitness, the param set with the lowest fitness is the best)
   * @param jobSuggestedParamSet JobSuggestedParamSet
   */
  protected JobSuggestedParamSet updateBestJobSuggestedParamSet(JobSuggestedParamSet jobSuggestedParamSet) {
    logger.debug("Checking if a new best param set is found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId);
    JobSuggestedParamSet currentBestJobSuggestedParamSet = JobSuggestedParamSet.find.where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id,
            jobSuggestedParamSet.jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 1)
        .findUnique();
    if (currentBestJobSuggestedParamSet != null) {
      if (currentBestJobSuggestedParamSet.fitness > jobSuggestedParamSet.fitness) {
        logger.debug("Param set: " + jobSuggestedParamSet.id + " is the new best param set for job: "
            + jobSuggestedParamSet.jobDefinition.jobDefId);
        currentBestJobSuggestedParamSet.isParamSetBest = false;
        jobSuggestedParamSet.isParamSetBest = true;
        currentBestJobSuggestedParamSet.save();
      }
    } else {
      logger.debug("No best param set found for job: " + jobSuggestedParamSet.jobDefinition.jobDefId
          + ". Marking current param set " + jobSuggestedParamSet.id + " as best");
      jobSuggestedParamSet.isParamSetBest = true;
    }
    return jobSuggestedParamSet;
  }

  /*
  Currently update in database is happening in calculate phase . It should be seperated out
   */
  protected boolean updateDataBase(List<TuningJobExecutionParamSet> jobExecutionParamSets) {
    return true;
  }

  /**
   * This method update metrics for auto tuning monitoring for fitness compute daemon
   * @param completedJobExecutionParamSets List of completed tuning job executions
   */
  private boolean updateMetrics(List<TuningJobExecutionParamSet> completedJobExecutionParamSets) {
    int fitnessNotUpdated = 0;
    for (TuningJobExecutionParamSet completedJobExecutionParamSet : completedJobExecutionParamSets) {
      if (!completedJobExecutionParamSet.jobSuggestedParamSet.paramSetState.equals(
          JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)) {
        fitnessNotUpdated++;
      } else {
        AutoTuningMetricsController.markFitnessComputedJobs();
      }
    }
    AutoTuningMetricsController.setFitnessComputeWaitJobs(fitnessNotUpdated);
    return true;
  }

  private boolean isTuningEnabled(Integer jobDefinitionId) {
    TuningJobDefinition tuningJobDefinition = tuningDefintionCollection.get(jobDefinitionId);
    if (tuningJobDefinition == null) {
      TuningJobDefinition tuningJobDefinitionNew = TuningJobDefinition.find.where()
          .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinitionId)
          .order()
          // There can be multiple entries in tuningJobDefinition if the job is switch on/off multiple times.
          // The latest entry gives the information regarding whether tuning is enabled or not
          .desc(TuningJobDefinition.TABLE.createdTs)
          .setMaxRows(1)
          .findUnique();

      return tuningJobDefinitionNew != null && tuningJobDefinitionNew.tuningEnabled;
    } else {
      return tuningJobDefinition != null && tuningJobDefinition.tuningEnabled;
    }
  }

  public boolean reachToNumberOfThresholdIterations(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets,
      JobDefinition jobDefinition) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .findUnique();
    if (tuningJobExecutionParamSets != null
        && tuningJobExecutionParamSets.size() == tuningJobDefinition.numberOfIterations) {
      return true;
    }
    return false;
  }

  /**
   * Switches off tuning for the given job
   * @param jobDefinition Job for which tuning is to be switched off
   */
  public void disableTuning(JobDefinition jobDefinition, String reason) {
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .findUnique();
    if (tuningJobDefinition.tuningEnabled) {
      tuningJobDefinition.tuningEnabled = false;
      tuningJobDefinition.tuningDisabledReason = reason;
      tuningJobDefinition.save();
    }
  }

  public final boolean execute() {
    logger.info("Executing Fitness Manager");
    boolean calculateFitnessDone = false, databaseUpdateDone = false, updateMetricsDone = false;
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSet = detectJobsForFitnessComputation();
    if (tuningJobExecutionParamSet != null && tuningJobExecutionParamSet.size() >= 1) {
      logger.info("Calculating  Fitness");
      calculateFitnessDone = calculateFitness(tuningJobExecutionParamSet);
    }
    /*
    Update database is not implemented , since data base update is currently occuring in calculate fitnesss
     */
    if (calculateFitnessDone) {
      logger.info("Updating Database");
      databaseUpdateDone = updateDataBase(tuningJobExecutionParamSet);
    }
    if (databaseUpdateDone) {
      logger.info("Updating Metrics");
      updateMetricsDone = updateMetrics(tuningJobExecutionParamSet);
    }
    logger.info(
        "Disable Tuning if Required . Disabled based on number of iteration reached or other terminating conditions for algorithms");
    if (tuningJobExecutionParamSet != null && tuningJobExecutionParamSet.size() >= 1) {
      Set<JobDefinition> jobDefinitionSet = new HashSet<JobDefinition>();
      for (TuningJobExecutionParamSet completedJobExecutionParamSet : tuningJobExecutionParamSet) {
        JobDefinition jobDefinition = completedJobExecutionParamSet.jobSuggestedParamSet.jobDefinition;
        if (isTuningEnabled(jobDefinition.id)) {
          jobDefinitionSet.add(jobDefinition);
        }
      }
      checkToDisableTuning(jobDefinitionSet);
    }
    logger.info("Fitness Computed");
    return true;
  }

  protected void getCompletedExecution(List<TuningJobExecutionParamSet> tuningJobExecutionParamSets,
      List<TuningJobExecutionParamSet> completedJobExecutionParamSet) {
    for (TuningJobExecutionParamSet tuningJobExecutionParamSet : tuningJobExecutionParamSets) {
      JobExecution jobExecution = tuningJobExecutionParamSet.jobExecution;
      long diff = System.currentTimeMillis() - jobExecution.updatedTs.getTime();
      logger.debug("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
          + jobExecution.updatedTs.getTime());
      if (diff < fitnessComputeWaitInterval) {
        logger.debug("Delaying fitness compute for execution: " + jobExecution.jobExecId);
      } else {
        logger.debug("Adding execution " + jobExecution.jobExecId + " to fitness computation queue");
        completedJobExecutionParamSet.add(tuningJobExecutionParamSet);
      }
    }
    logger.debug(
        "Number of completed execution fetched for fitness computation: " + completedJobExecutionParamSet.size());
  }
}
