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

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.util.Utils;
import controllers.AutoTuningMetricsController;
import java.util.ArrayList;
import java.util.List;
import models.TuningJobDefinition;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class does baseline computation once the job is enabled for auto tuning.
 * It takes average resource usage and execution time for last 30 jobs to get the baseline.
 */
public class BaselineComputeUtil {

  private static final Integer NUM_JOBS_FOR_BASELINE_DEFAULT = 30;
  private final Logger logger = Logger.getLogger(getClass());
  private static final String BASELINE_EXECUTION_COUNT = "baseline.execution.count";

  private Integer _numJobsForBaseline = null;

  public BaselineComputeUtil() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    _numJobsForBaseline =
        Utils.getNonNegativeInt(configuration, BASELINE_EXECUTION_COUNT, NUM_JOBS_FOR_BASELINE_DEFAULT);
  }

  /**
   * Computes baseline for the jobs new to auto tuning
   * @return tuningJobDefinition
   */
  public List<TuningJobDefinition> computeBaseline() {
    logger.info("Starting baseline computation");
    List<TuningJobDefinition> tuningJobDefinitions = getJobForBaselineComputation();
    for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
      try {
        updateBaselineForJob(tuningJobDefinition);
      } catch (Exception e) {
        logger.error("Error in computing baseline for job: " + tuningJobDefinition.job.jobName, e);
      }
    }
    updateMetrics(tuningJobDefinitions);
    logger.info("Baseline computation complete");
    return tuningJobDefinitions;
  }

  /**
   * This method update metrics for auto tuning monitoring for baseline computation
   * @param tuningJobDefinitions
   */
  private void updateMetrics(List<TuningJobDefinition> tuningJobDefinitions) {
    int baselineComputeWaitJobs = 0;
    for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
      if (tuningJobDefinition.averageResourceUsage == null) {
        baselineComputeWaitJobs++;
      } else {
        AutoTuningMetricsController.markBaselineComputed();
      }
    }
    AutoTuningMetricsController.setBaselineComputeWaitJobs(baselineComputeWaitJobs);
  }

  /**
   * Fetches the jobs whose baseline is to be computed.
   * This is done by returning the jobs with null average resource usage
   * @return List of jobs whose baseline needs to be added
   */
  private List<TuningJobDefinition> getJobForBaselineComputation() {
    logger.info("Fetching jobs for which baseline metrics need to be computed");
    List<TuningJobDefinition> tuningJobDefinitions = new ArrayList<TuningJobDefinition>();
    try {
      tuningJobDefinitions =
          TuningJobDefinition.find.where().eq(TuningJobDefinition.TABLE.averageResourceUsage, null).findList();
    } catch (NullPointerException e) {
      logger.info("There are no jobs for which baseline has to be computed", e);
    }
    return tuningJobDefinitions;
  }

  /**
   * Adds baseline metric values for a job
   * @param tuningJobDefinition Job for which baseline is to be added
   */
  private void updateBaselineForJob(TuningJobDefinition tuningJobDefinition) {

    logger.info("Computing and updating baseline metric values for job: " + tuningJobDefinition.job.jobName);

    String sql = "SELECT AVG(resource_used) AS resource_used, AVG(execution_time) AS execution_time FROM "
        + "(SELECT job_exec_id, SUM(resource_used/(1024 * 3600)) AS resource_used, "
        + "SUM((finish_time - start_time - total_delay)/(1000 * 60))  AS execution_time, "
        + "MAX(start_time) AS start_time " + "FROM yarn_app_result WHERE job_def_id=:jobDefId "
        + "GROUP BY job_exec_id " + "ORDER BY start_time DESC " + "LIMIT :num) temp";

    logger.debug("Running query for baseline computation " + sql);

    SqlRow baseline = Ebean.createSqlQuery(sql)
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

    logger.debug("Baseline metric values: Average resource usage=" + avgResourceUsage + " and Average execution time="
        + avgExecutionTime);
    tuningJobDefinition.update();
    logger.info("Updated baseline metric value for job: " + tuningJobDefinition.job.jobName);
  }

  /**
   * Returns the average input size in bytes of a job (over last _numJobsForBaseline executions)
   * @param jobDefId job definition id of the job
   * @return average input size in bytes as long
   */
  private Long getAvgInputSizeInBytes(String jobDefId) {
    String sql = "SELECT AVG(inputSizeInBytes) as avgInputSizeInMB FROM "
        + "(SELECT job_exec_id, SUM(cast(value as decimal)) inputSizeInBytes, MAX(start_time) AS start_time "
        + "FROM yarn_app_result yar INNER JOIN yarn_app_heuristic_result yahr " + "ON yar.id=yahr.yarn_app_result_id "
        + "INNER JOIN yarn_app_heuristic_result_details yahrd " + "ON yahr.id=yahrd.yarn_app_heuristic_result_id "
        + "WHERE job_def_id=:jobDefId AND yahr.heuristic_name='" + CommonConstantsHeuristic.MAPPER_SPEED + "' "
        + "AND yahrd.name='Total input size in MB' "
        + "GROUP BY job_exec_id ORDER BY start_time DESC LIMIT :num ) temp";

    logger.debug("Running query for average input size computation " + sql);

    SqlRow baseline = Ebean.createSqlQuery(sql)
        .setParameter("jobDefId", jobDefId)
        .setParameter("num", _numJobsForBaseline)
        .findUnique();
    Double avgInputSizeInBytes = baseline.getDouble("avgInputSizeInMB") * FileUtils.ONE_MB;
    return avgInputSizeInBytes.longValue();
  }
}
