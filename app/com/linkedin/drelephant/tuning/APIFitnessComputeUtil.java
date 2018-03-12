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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Random;

import models.JobDefinition;
import models.JobExecution;
import models.TuningJobDefinition;
import models.TuningJobExecution;
import models.TuningJobExecution.ParamSetStatus;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.math.Statistics;

import play.libs.Json;


/**
 * This is another version of FitnessComputeUtil which uses Dr Elephant API to compute the fitness.
 * This can be used during test when Dr Elephant DB is not getting data properly to compute the fitness
 */
public class APIFitnessComputeUtil extends FitnessComputeUtil {

  private static final Logger logger = Logger.getLogger(APIFitnessComputeUtil.class);

  private static final String DR_ELEPHANT_URL = "dr.elephant.api.url";
  private static final String JOB_HISTORY_SERVER_URL = "mapreduce.jobhistory.webapp.address";

  private String _drElephantURL;
  private String _jobHistoryServerURL;
  private ObjectMapper _objectMapper = new ObjectMapper();

  private AuthenticatedURL.Token _token;
  private AuthenticatedURL _authenticatedURL;
  private long _currentTime = 0;
  private long _tokenUpdatedTime = 0;

  private static final long TOKEN_UPDATE_INTERVAL =
      Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS);
  private static final long FETCH_DELAY = 60000;

  public APIFitnessComputeUtil() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    _drElephantURL = configuration.get(DR_ELEPHANT_URL);
    _jobHistoryServerURL = configuration.get(JOB_HISTORY_SERVER_URL);
  }

  /**
   * Updates the execution metrics
   * @param completedExecutions List of completed executions
   */
  protected void updateExecutionMetrics(List<TuningJobExecution> completedExecutions) {
    logger.debug("Updating execution metrics");
    updateAuthToken();
    for (TuningJobExecution tuningJobExecution : completedExecutions) {
      logger.debug("Completed executions before updating metric: " + Json.toJson(tuningJobExecution));
      try {

        JobExecution jobExecution = tuningJobExecution.jobExecution;
        JobDefinition job = jobExecution.job;

        URL jobExecURL = new URL(new URL(_drElephantURL),
            String.format("/rest/jobexec?id=%s", URLEncoder.encode(jobExecution.jobExecId)));
        HttpURLConnection conn = (HttpURLConnection) jobExecURL.openConnection();
        JsonNode allApps = _objectMapper.readTree(conn.getInputStream());

        // job id match and tuning enabled
        TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
            .fetch(TuningJobDefinition.TABLE.job, "*")
            .where()
            .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, job.id)
            .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
            .findUnique();

        if (allApps != null && allApps.size() > 0) {
          Long totalExecutionTime = 0L;
          Double totalResourceUsed = 0D;
          Double totalInputBytesInBytes = 0D;

          for (JsonNode app : allApps) {
            logger.info("Job Execution Update: ApplicationID " + app.get("id").getTextValue());
            Long executionTime =
                app.get("finishTime").getLongValue() - app.get("startTime").getLongValue() - app.get("totalDelay")
                    .getLongValue();
            totalExecutionTime += executionTime;
            totalResourceUsed += app.get("resourceUsed").getDoubleValue();
            totalInputBytesInBytes += getTotalInputBytes(app.get("id").getTextValue());
          }

          if (totalExecutionTime != 0) {
            jobExecution.executionTime = totalExecutionTime * 1.0 / (1000 * 60);
            jobExecution.resourceUsage = totalResourceUsed * 1.0 / (1024 * 3600);
            jobExecution.inputSizeInBytes = totalInputBytesInBytes;
            logger.info("Job Execution Update: UpdatedValue " + totalExecutionTime + ":" + totalResourceUsed + ":"
                + totalInputBytesInBytes);
          }

          logger.debug("Job execution " + jobExecution.resourceUsage);
          logger.debug("Job details: AvgResourceUsage " + tuningJobDefinition.averageResourceUsage
              + ", allowedMaxResourceUsagePercent: " + tuningJobDefinition.allowedMaxResourceUsagePercent);
          if (jobExecution.executionState.equals(JobExecution.ExecutionState.FAILED)
              || jobExecution.executionState.equals(JobExecution.ExecutionState.CANCELLED)) {
            // Todo: Check if the reason of failure is auto tuning and  handle cancelled cases
            tuningJobExecution.fitness =
                3 * tuningJobDefinition.averageResourceUsage * tuningJobDefinition.allowedMaxResourceUsagePercent
                    * FileUtils.ONE_GB / (100.0 * tuningJobDefinition.averageInputSizeInBytes);
          } else if (jobExecution.resourceUsage > (
              tuningJobDefinition.averageResourceUsage * tuningJobDefinition.allowedMaxResourceUsagePercent / 100.0)) {
            tuningJobExecution.fitness =
                3 * tuningJobDefinition.averageResourceUsage * tuningJobDefinition.allowedMaxResourceUsagePercent
                    * FileUtils.ONE_GB / (100.0 * totalInputBytesInBytes);
          } else {
            tuningJobExecution.fitness = jobExecution.resourceUsage * FileUtils.ONE_GB / totalInputBytesInBytes;
          }
          tuningJobExecution.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
          jobExecution.update();
          tuningJobExecution.update();

          logger.debug("Completed executions after updating metrics: " + Json.toJson(tuningJobExecution));
        } else {
          if (jobExecution.executionState.equals(JobExecution.ExecutionState.FAILED)
              || jobExecution.executionState.equals(JobExecution.ExecutionState.CANCELLED)) {
            // Todo: Check if the reason of failure is auto tuning and  handle cancelled cases
            tuningJobExecution.fitness =
                3 * tuningJobDefinition.averageResourceUsage * tuningJobDefinition.allowedMaxResourceUsagePercent
                    * FileUtils.ONE_GB / (100.0 * tuningJobDefinition.averageInputSizeInBytes);
            jobExecution.executionTime = 0D;
            jobExecution.resourceUsage = 0D;
            jobExecution.inputSizeInBytes = 0D;
            tuningJobExecution.paramSetState = ParamSetStatus.FITNESS_COMPUTED;
            jobExecution.update();
            tuningJobExecution.update();
          }
        }
      } catch (Exception e) {
        logger.error("Error updating fitness of job_exec_id: " + tuningJobExecution.jobExecution.id + "\n Stacktrace: ",
            e);
      }
    }
    logger.debug("Execution metrics updated");
  }

  private Long getTotalInputBytes(String applicationID) throws IOException, AuthenticationException {
    applicationID = applicationID.replace("application_", "job_");
    URL applicationURL = new URL(new URL(_jobHistoryServerURL),
        String.format("/ws/v1/history/mapreduce/jobs/%s/counters", applicationID));
    HttpURLConnection conn = (HttpURLConnection) _authenticatedURL.openConnection(applicationURL, _token);
    JsonNode rootNode = _objectMapper.readTree(conn.getInputStream());
    return rootNode.get("jobCounters")
        .get("counterGroup")
        .get(0)
        .get("counter")
        .get(5)
        .get("totalCounterValue")
        .getLongValue();
  }

  /**
   * Authenticate and update the token
   */
  private void updateAuthToken() {
    _currentTime = System.currentTimeMillis() - FETCH_DELAY;
    if (_currentTime - _tokenUpdatedTime > TOKEN_UPDATE_INTERVAL) {
      logger.info("AnalysisProvider updating its Authenticate Token...");
      _token = new AuthenticatedURL.Token();
      _authenticatedURL = new AuthenticatedURL();
      _tokenUpdatedTime = _currentTime;
    }
  }
}
