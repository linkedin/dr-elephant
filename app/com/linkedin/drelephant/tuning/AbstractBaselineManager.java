package com.linkedin.drelephant.tuning;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import java.util.List;
import models.JobExecution;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import controllers.AutoTuningMetricsController;
import org.apache.hadoop.conf.Configuration;


/**
 * This class is abstract class for BaselineManager. Each tuning type will have its own specific implementation.
 * for e.g BaselineManagerOBT, BaselineManagerHBT
 */
public abstract class AbstractBaselineManager implements Manager {
  protected final String BASELINE_EXECUTION_COUNT = "baseline.execution.count";
  protected Integer NUM_JOBS_FOR_BASELINE_DEFAULT = 30;
  protected String baseLineCalculationSQL =
      "SELECT AVG(resource_used) AS resource_used, AVG(execution_time) AS execution_time FROM "
          + "(SELECT job_exec_id, SUM(resource_used/(1024 * 3600)) AS resource_used, "
          + "SUM((finish_time - start_time - total_delay)/(1000 * 60))  AS execution_time, "
          + "MAX(start_time) AS start_time " + "FROM yarn_app_result WHERE job_def_id=:jobDefId "
          + "GROUP BY job_exec_id " + "ORDER BY start_time DESC " + "LIMIT :num) temp";
  protected String avgInputSizeSQL = "SELECT AVG(inputSizeInBytes) as avgInputSizeInMB FROM "
      + "(SELECT job_exec_id, SUM(cast(value as decimal)) inputSizeInBytes, MAX(start_time) AS start_time "
      + "FROM yarn_app_result yar INNER JOIN yarn_app_heuristic_result yahr " + "ON yar.id=yahr.yarn_app_result_id "
      + "INNER JOIN yarn_app_heuristic_result_details yahrd " + "ON yahr.id=yahrd.yarn_app_heuristic_result_id "
      + "WHERE job_def_id=:jobDefId AND yahr.heuristic_name='" + CommonConstantsHeuristic.MAPPER_SPEED + "' "
      + "AND yahrd.name='" + CommonConstantsHeuristic.TOTAL_INPUT_SIZE_IN_MB + "' "
      + "GROUP BY job_exec_id ORDER BY start_time DESC LIMIT :num ) temp";
  private final Logger logger = Logger.getLogger(getClass());
  boolean debugEnabled = logger.isDebugEnabled();
  protected Integer _numJobsForBaseline = null;
  protected Configuration configuration = null;

  public AbstractBaselineManager() {
    configuration = ElephantContext.instance().getAutoTuningConf();
  }

  /**
   * 1) Get the jobs for which Baseline have to be calculated
   * 2) Calculate the baseline
   * 3) Update the base line
   * 4) Update the metrics
   * @return true if method executes successfully  . Otherwise false
   */
  @Override
  public final boolean execute() {
    try {
      logger.info("Executing BaseLine");
      boolean baseLineComputationDone = false, databaseUpdateDone = false, updateMetricsDone = false;
      List<TuningJobDefinition> tuningJobDefinitions = detectJobsForBaseLineComputation();
      if (tuningJobDefinitions != null && tuningJobDefinitions.size() >= 1) {
        logger.info("Computing BaseLine");
        baseLineComputationDone = calculateBaseLine(tuningJobDefinitions);
      }
      if (baseLineComputationDone) {
        logger.info("Updating Database");
        databaseUpdateDone = updateDataBase(tuningJobDefinitions);
      }
      if (databaseUpdateDone) {
        logger.info("Updating Metrics");
        updateMetricsDone = updateMetrics(tuningJobDefinitions);
      }
      logger.info("Baseline Done ");
      return updateMetricsDone;
    } catch (Exception e) {
      logger.error(" Execution of the base line manager is failed " + e.getMessage(),e);
      return false;
    }
  }

  /**
   * Fetches the jobs whose baseline is to be computed.
   * This is done by returning the jobs with null average resource usage
   * @return List of jobs whose baseline needs to be added
   */
  protected abstract List<TuningJobDefinition> detectJobsForBaseLineComputation();

  /**
   * Adds baseline metric values for a job
   * @param tuningJobDefinitions Job for which baseline is to be computed
   */
  protected boolean calculateBaseLine(List<TuningJobDefinition> tuningJobDefinitions) {
    for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
      try {
        if(debugEnabled) {
          logger.debug("Computing and updating baseline metric values for job: " + tuningJobDefinition.job.jobName);
          logger.debug("Running query for baseline computation " + baseLineCalculationSQL);
        }
        SqlRow baseline = Ebean.createSqlQuery(baseLineCalculationSQL)
            .setParameter("jobDefId", tuningJobDefinition.job.jobDefId)
            .setParameter("num", _numJobsForBaseline)
            .findUnique();
        Double avgResourceUsage = 0D;
        Double avgExecutionTime = 0D;
        avgResourceUsage = baseline.getDouble("resource_used");
        avgExecutionTime = baseline.getDouble("execution_time");
        tuningJobDefinition.averageExecutionTime = avgExecutionTime;
        tuningJobDefinition.averageResourceUsage = avgResourceUsage;
        tuningJobDefinition.averageInputSizeInBytes = getAvgInputSizeInBytes(tuningJobDefinition.job.jobDefId);
        logger.debug(
            "Baseline metric values: Average resource usage=" + avgResourceUsage + " and Average execution time="
                + avgExecutionTime);
      } catch (Exception e) {
        logger.error("Error in computing baseline for job: " + tuningJobDefinition.job.jobName, e);
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the average input size in bytes of a job (over last _numJobsForBaseline executions)
   * @param jobDefId job definition id of the job
   * @return average input size in bytes as long
   */
  private Long getAvgInputSizeInBytes(String jobDefId) {
    logger.debug(
        "Running query for average input size computation " + avgInputSizeSQL + " Job definintion ID " + jobDefId);

    SqlRow baseline = Ebean.createSqlQuery(avgInputSizeSQL)
        .setParameter("jobDefId", jobDefId)
        .setParameter("num", _numJobsForBaseline)
        .findUnique();
    Double avgInputSizeInBytes = baseline.getDouble("avgInputSizeInMB") * FileUtils.ONE_MB;
    return avgInputSizeInBytes.longValue();
  }

  /**
   * This method update database  for auto tuning monitoring for baseline computation
   * @param tuningJobDefinitions
   */
  protected boolean updateDataBase(List<TuningJobDefinition> tuningJobDefinitions) {
    try {
      for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
        tuningJobDefinition.update();
          logger.debug("Updated baseline metric value for job: " + tuningJobDefinition.job.jobName);
      }
    } catch (Exception e) {
      logger.error(" Error Updating Database " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * This method update metrics for auto tuning monitoring for baseline computation
   * @param tuningJobDefinitions
   */
  protected boolean updateMetrics(List<TuningJobDefinition> tuningJobDefinitions) {
    try {
      int baselineComputeWaitJobs = 0;
      for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
        if (tuningJobDefinition.averageResourceUsage == null) {
          baselineComputeWaitJobs++;
        } else {
          AutoTuningMetricsController.markBaselineComputed();
        }
      }
      AutoTuningMetricsController.setBaselineComputeWaitJobs(baselineComputeWaitJobs);
    } catch (Exception e) {
      logger.error(" Error Updating Metrics in Ingraph" + e.getMessage(),e);
      return false;
    }
    return true;
  }
}
