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

package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;


/**
 * This class provides methods to load information specific to the Oozie scheduler.
 */
public class OozieScheduler implements Scheduler {

  private static final Logger logger = Logger.getLogger(OozieScheduler.class);

  private static final String OOZIE_JOB_ID = "oozie.job.id";
  private static final String OOZIE_ACTION_ID = "oozie.action.id";

  private static final String OOZIE_API_URL = "oozie_api_url";
  private static final String OOZIE_AUTH_OPTION = "oozie_auth_option";
  private static final String OOZIE_JOB_DEF_URL_TEMPLATE = "oozie_job_url_template";
  private static final String OOZIE_JOB_EXEC_URL_TEMPLATE = "oozie_job_exec_url_template";
  private static final String OOZIE_WORKFLOW_DEF_URL_TEMPLATE = "oozie_workflow_url_template";
  private static final String OOZIE_WORKFLOW_EXEC_URL_TEMPLATE = "oozie_workflow_exec_url_template";

  private String schedulerName;
  private String jobName;
  private String jobExecId;
  private String workflowExecId;
  private String workflowName;
  private int workflowDepth;

  private OozieClient oozieClient;
  private String jobDefUrlTemplate;
  private String jobExecUrlTemplate;
  private String workflowDefUrlTemplate;
  private String workflowExecUrlTemplate;

  public OozieScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData) {
    this(appId, properties, schedulerConfData, null);
  }

  public OozieScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData, OozieClient oozieClient) {
    schedulerName = schedulerConfData.getSchedulerName();

    if (properties != null && properties.getProperty(OOZIE_ACTION_ID) != null) {
      this.oozieClient = oozieClient == null ? makeOozieClient(schedulerConfData) : oozieClient ;
      jobDefUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_JOB_DEF_URL_TEMPLATE);
      jobExecUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_JOB_EXEC_URL_TEMPLATE);
      workflowDefUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_WORKFLOW_DEF_URL_TEMPLATE);
      workflowExecUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_WORKFLOW_EXEC_URL_TEMPLATE);

      loadInfo(appId, properties);
    }

    // Use default value of data type
  }

  private void loadInfo(String appId, Properties properties) {
    // 0004167-160629080632562-oozie-oozi-W@some-action
    jobExecId = properties.getProperty(OOZIE_ACTION_ID);
    if (jobExecId.contains("@")) {
      String[] pair = jobExecId.split("@", 2);
      workflowExecId = pair[0];
      jobName = pair[1];

      try {
        logger.info("Fetching Oozie workflow info for " + workflowExecId);
        WorkflowJob workflow = oozieClient.getJobInfo(workflowExecId);
        logger.info("Oozie workflow for " + workflowExecId + ": " + workflow);
        workflowName = workflow.getAppName();
      } catch (OozieClientException e) {
        throw new RuntimeException("Failed fetching Oozie workflow " + workflowExecId + " info", e);
      }
    }

    workflowDepth = 0; // TODO: Add sub-workflow support
  }

  private OozieClient makeOozieClient(SchedulerConfigurationData schedulerConfData) {
    String oozieApiUrl = schedulerConfData.getParamMap().get(OOZIE_API_URL);
    String authOption = schedulerConfData.getParamMap().get(OOZIE_AUTH_OPTION);
    if (oozieApiUrl == null) {
      throw new RuntimeException("Missing " + OOZIE_API_URL + " param for Oozie Scheduler");
    }

    return new AuthOozieClient(oozieApiUrl, authOption);
  }

  @Override
  public String getSchedulerName() {
    return schedulerName;
  }

  @Override
  public boolean isEmpty() {
    return schedulerName == null || jobName == null || jobExecId == null || workflowName == null || workflowExecId == null;
  }

  @Override
  public String getJobDefId() {
    return Utils.formatStringOrNull("%s", jobName);
  }

  @Override
  public String getJobExecId() {
    return Utils.formatStringOrNull("%s", jobExecId);
  }

  @Override
  public String getFlowDefId() {
    return Utils.formatStringOrNull("%s", workflowName);
  }

  @Override
  public String getFlowExecId() {
    return Utils.formatStringOrNull("%s", workflowExecId);
  }

  @Override
  public String getJobDefUrl() {
    if (jobDefUrlTemplate != null) {
      return Utils.formatStringOrNull(jobDefUrlTemplate, jobName);
    } else {
      logger.warn("Missing " + OOZIE_JOB_DEF_URL_TEMPLATE + " param for Oozie Scheduler");
      return jobName;
    }
  }

  @Override
  public String getJobExecUrl() {
    if (jobExecUrlTemplate != null) {
      return Utils.formatStringOrNull(jobExecUrlTemplate, jobExecId);
    } else {
      logger.warn("Missing " + OOZIE_JOB_EXEC_URL_TEMPLATE + " param for Oozie Scheduler");
      return jobExecId;
    }
  }

  @Override
  public String getFlowDefUrl() {
    if (workflowDefUrlTemplate != null) {
      return Utils.formatStringOrNull(workflowDefUrlTemplate, workflowName);
    } else {
      logger.warn("Missing " + OOZIE_WORKFLOW_DEF_URL_TEMPLATE + " param for Oozie Scheduler");
      return workflowName;
    }
  }

  @Override
  public String getFlowExecUrl() {
    if (workflowExecUrlTemplate != null) {
      return Utils.formatStringOrNull(workflowExecUrlTemplate, workflowExecId);
    } else {
      logger.warn("Missing " + OOZIE_WORKFLOW_EXEC_URL_TEMPLATE + " param for Oozie Scheduler");
      return workflowExecId;
    }
  }

  @Override
  public int getWorkflowDepth() {
    return workflowDepth;
  }

  @Override
  public String getJobName() {
    return jobName;
  }
}
