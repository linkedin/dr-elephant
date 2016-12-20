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
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;
import org.apache.commons.lang.StringUtils;
import java.util.Properties;


/**
 * This class provides methods to load information specific to the Oozie scheduler.
 */
public class OozieScheduler implements Scheduler {

    private static final Logger logger = Logger.getLogger(OozieScheduler.class);

    private static final String OOZIE_ACTION_ID = "oozie.action.id";

    private static final String OOZIE_API_URL = "oozie_api_url";
    private static final String OOZIE_AUTH_OPTION = "oozie_auth_option";
    private static final String OOZIE_JOB_DEF_URL_TEMPLATE = "oozie_job_url_template";
    private static final String OOZIE_JOB_EXEC_URL_TEMPLATE = "oozie_job_exec_url_template";
    private static final String OOZIE_WORKFLOW_DEF_URL_TEMPLATE = "oozie_workflow_url_template";
    private static final String OOZIE_WORKFLOW_EXEC_URL_TEMPLATE = "oozie_workflow_exec_url_template";
    private static final String OOZIE_APP_NAME_UNIQUENESS = "oozie_app_name_uniqueness";
    private boolean appNameUniqueness;

    private String schedulerName;
    private String jobDefId;
    private String jobExecId;
    private String flowExecId;
    private String flowDefId;
    private String jobDefIdUrl;
    private String jobExecIdUrl;
    private String flowExecIdUrl;
    private String flowDefIdUrl;
    private int workflowDepth;

    private OozieClient oozieClient;
    private String jobDefUrlTemplate;
    private String jobExecUrlTemplate;
    private String workflowDefUrlTemplate;
    private String workflowExecUrlTemplate;
    private String flowDefName;

    public OozieScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData) {
        this(appId, properties, schedulerConfData, null);
    }

    public OozieScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData, OozieClient oozieClient) {
        schedulerName = schedulerConfData.getSchedulerName();

        if (properties != null && properties.getProperty(OOZIE_ACTION_ID) != null) {
            this.oozieClient = oozieClient == null ? makeOozieClient(schedulerConfData) : oozieClient;
            jobDefUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_JOB_DEF_URL_TEMPLATE);
            jobExecUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_JOB_EXEC_URL_TEMPLATE);
            workflowDefUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_WORKFLOW_DEF_URL_TEMPLATE);
            workflowExecUrlTemplate = schedulerConfData.getParamMap().get(OOZIE_WORKFLOW_EXEC_URL_TEMPLATE);
            String appNameUniquenessStr = schedulerConfData.getParamMap().get(OOZIE_APP_NAME_UNIQUENESS);
            appNameUniqueness = appNameUniquenessStr != null && Boolean.parseBoolean(appNameUniquenessStr);

            loadInfo(properties);
        }

        // Use default value of data type
    }

    private void loadInfo(Properties properties) {
        // 0004167-160629080632562-oozie-oozi-W@some-action
        String actionId = properties.getProperty(OOZIE_ACTION_ID);

        if (actionId.contains("@")) {
            String workflowId = extractId(actionId);

            WorkflowJob workflow;
            try {
                logger.info("Fetching Oozie workflow info for " + workflowId);

                workflow = oozieClient.getJobInfo(workflowId);
                logger.info("Oozie workflow for " + workflowId + ": " + workflow);

                String superParentId = getSuperParentId(workflow);
                logger.info("Oozie super parent for: " + workflowId + ": " + superParentId);

                jobExecId = workflow.getId();
                jobExecIdUrl = workflow.getConsoleUrl();
                jobDefIdUrl = workflow.getConsoleUrl();
                flowExecId = superParentId;

                if (isCoordinatorJob(superParentId)) {
                    coordinatedJobInfo(workflow, actionId, superParentId);
                } else {
                    manualCommittedJob(workflow, actionId, superParentId);
                }
            } catch (OozieClientException e) {
                throw new RuntimeException("Failed fetching Oozie workflow " + workflowId + " info", e);
            }
        }
    }

    private void manualCommittedJob(WorkflowJob workflow, String actionId, String superParentId) throws OozieClientException {
        logger.info("Oozie workflow " + actionId + " was manually submitted");
        WorkflowJob flowDefWorkflow = oozieClient.getJobInfo(extractId(superParentId));
        flowDefIdUrl = flowDefWorkflow.getConsoleUrl();
        flowExecIdUrl = flowDefWorkflow.getConsoleUrl();
        if (appNameUniqueness) {
            jobDefId = workflow.getAppName() + "-" + extractAction(actionId);
            flowDefId = superParentId;
            flowDefName = flowDefWorkflow.getAppName();
        } else {
            jobDefId = workflow.getId();
            flowDefId = superParentId;
        }
    }

    private void coordinatedJobInfo(WorkflowJob workflow, String actionId, String superParentId) throws OozieClientException {
        logger.info("Oozie workflow " + actionId + " is scheduled with coordinator");
        CoordinatorJob flowDefCoordinator = oozieClient.getCoordJobInfo(extractId(superParentId));
        flowDefIdUrl = flowDefCoordinator.getConsoleUrl();
        flowExecIdUrl = flowDefCoordinator.getConsoleUrl();
        if (appNameUniqueness) {
            jobDefId = workflow.getAppName() + "-" + extractAction(actionId);
            flowDefId = extractId(superParentId);
            flowDefName = flowDefCoordinator.getAppName();
        } else {
            jobDefId = extractId(superParentId) + "-" + extractAction(actionId) + "-" + workflowDepth;
            flowDefId = extractId(superParentId);
        }
    }

    private String extractId(String idAndAction) {
        return idAndAction.split("@")[0];
    }

    private String extractAction(String idAndAction) {
        return idAndAction.split("@")[1];
    }

    private String getSuperParentId(WorkflowJob workflow) throws OozieClientException {

        WorkflowJob current = workflow;
        workflowDepth = 0;

        while (hasParent(current)) {
            if (isCoordinatorJob(current.getParentId())) {
                return current.getParentId();
            }
            current = oozieClient.getJobInfo(current.getParentId());

            workflowDepth++;
        }

        return current.getId();
    }

    private boolean hasParent(WorkflowJob workflow) {
        return StringUtils.isNotEmpty(workflow.getParentId());
    }

    private boolean isCoordinatorJob(String workflowId) {
        return workflowId != null && extractId(workflowId).endsWith("C");
    }

    private OozieClient makeOozieClient(SchedulerConfigurationData schedulerConfData) {
        String oozieApiUrl = schedulerConfData.getParamMap().get(OOZIE_API_URL);
        String authOption = schedulerConfData.getParamMap().get(OOZIE_AUTH_OPTION);
        if (oozieApiUrl == null) {
            throw new RuntimeException("Missing " + OOZIE_API_URL + " param for Oozie Scheduler");
        }

        return new AuthOozieClient(oozieApiUrl, authOption);
    }

    private String getUrl(String idUrl, String id, String urlTemplate, String propertyName) {
        String url;
        if (urlTemplate != null) {
            url = Utils.formatStringOrNull(urlTemplate, id);
        } else if (idUrl != null) {
            url = idUrl;
        } else {
            logger.warn("Missing " + propertyName + " param for Oozie Scheduler");
            url = id;
        }

        return url;
    }

    @Override
    public String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public boolean isEmpty() {
        return schedulerName == null || jobDefId == null || jobExecId == null || flowDefId == null || flowExecId == null;
    }

    @Override
    public String getJobDefId() {
        return Utils.formatStringOrNull("%s", jobDefId);
    }

    @Override
    public String getJobExecId() {
        return Utils.formatStringOrNull("%s", jobExecId);
    }

    @Override
    public String getFlowDefId() {
        return Utils.formatStringOrNull("%s", appNameUniqueness ? flowDefName : flowDefId);
    }

    @Override
    public String getFlowExecId() {
        return Utils.formatStringOrNull("%s", flowExecId);
    }

    @Override
    public String getJobDefUrl() {
        return getUrl(jobDefIdUrl, jobDefId, jobDefUrlTemplate, OOZIE_JOB_DEF_URL_TEMPLATE);
    }

    @Override
    public String getJobExecUrl() {
        return getUrl(jobExecIdUrl, jobExecId, jobExecUrlTemplate, OOZIE_JOB_EXEC_URL_TEMPLATE);
    }

    @Override
    public String getFlowDefUrl() {
        return getUrl(flowDefIdUrl, flowDefId, workflowDefUrlTemplate, OOZIE_WORKFLOW_DEF_URL_TEMPLATE);
    }

    @Override
    public String getFlowExecUrl() {
        return getUrl(flowExecIdUrl, flowExecId, workflowExecUrlTemplate, OOZIE_WORKFLOW_EXEC_URL_TEMPLATE);
    }

    @Override
    public int getWorkflowDepth() {
        return workflowDepth;
    }

    @Override
    public String getJobName() {
        return jobDefId;
    }
}
