package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import org.apache.oozie.client.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OozieSchedulerTest {
    private static final String parentJobInfo = "0143705-160828184536493-oozie-oozi-W";
    private static final String jobAppName = "some-workflow-name";
    private static final String parentJobAppName = jobAppName + "parent";
    private static final String jobInfo = "0004167-160629080632562-oozie-oozi-W";
    private static final String actionName = "some-action";
    private static final String actionInfo = jobInfo + "@" + actionName;
    private static final String oozieUrl = "http://localhost:11000/oozie?job=";
    private static final String coordinatorJobInfo = "0163255-160828184536493-oozie-oozie-C";
    private static final String coordinatorActionInfo = coordinatorJobInfo + "@1537";
    private static final String coordinatorName = "some-coordinator";
    private static final String applicationUrl = "http://localhost:8088/proxy/application_1478790851061_4847/";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private OozieClient oozieClient;

    @Mock
    private WorkflowJob workflowJob;

    @Mock
    private OozieClient manualCommittedJobClient;

    @Mock
    private OozieClient scheduledJobClient;

    @Mock
    private WorkflowAction manualChildAction;

    @Mock
    private WorkflowJob manualChildJob;

    @Mock
    private WorkflowJob manualParentJob;

    @Mock
    private WorkflowAction scheduledChildAction;

    @Mock
    private WorkflowJob scheduledChildJob;

    @Mock
    private CoordinatorJob oozieCoordinatorJobInfo;

    private static Properties getNonOozieProperties() {
        return new Properties();
    }

    private static Properties getOozieProperties() {
        Properties properties = new Properties();

        properties.put("oozie.action.id", actionInfo);
        properties.put("oozie.job.id", jobInfo);

        return properties;
    }

    private static SchedulerConfigurationData makeConfig(String name, Map<String, String> params) {
        return new SchedulerConfigurationData(name, OozieScheduler.class.getName(), params);
    }

    private static Map<String, String> getDefaultSchedulerParams() {
        Map<String, String> paramMap = new HashMap<String, String>();

        paramMap.put("oozie_api_url", "http://oozie.api/");
        paramMap.put("oozie_job_url_template", "http://oozie/search?workflow=%s");
        paramMap.put("oozie_job_exec_url_template", "http://oozie/workflows/%s");
        paramMap.put("oozie_workflow_url_template", "http://oozie/search?workflow=%s");
        paramMap.put("oozie_workflow_exec_url_template", "http://oozie/workflows/%s");

        return paramMap;
    }

    private static Map<String, String> getSchedulerConfigWithout(String... keys) {
        Map<String, String> params = getDefaultSchedulerParams();

        for (String key : keys) {
            params.remove(key);
        }

        return params;
    }

    @Before
    public void setUp() throws OozieClientException {
        when(workflowJob.getAppName()).thenReturn(jobAppName);
        when(workflowJob.getId()).thenReturn(jobInfo);
        when(oozieClient.getJobInfo(eq(jobInfo))).thenReturn(workflowJob);

        //Manual committed job
        when(manualChildAction.getConsoleUrl()).thenReturn("-");
        when(manualCommittedJobClient.getWorkflowActionInfo(actionInfo)).thenReturn(manualChildAction);

        when(manualChildJob.getId()).thenReturn(jobInfo);
        when(manualChildJob.getAppName()).thenReturn(jobAppName);
        when(manualChildJob.getParentId()).thenReturn(parentJobInfo);
        when(manualChildJob.getConsoleUrl()).thenReturn(oozieUrl + jobInfo);
        when(manualCommittedJobClient.getJobInfo(jobInfo)).thenReturn(manualChildJob);

        when(manualParentJob.getId()).thenReturn(parentJobInfo);
        when(manualParentJob.getAppName()).thenReturn(parentJobAppName);
        when(manualParentJob.getParentId()).thenReturn(null);
        when(manualParentJob.getConsoleUrl()).thenReturn(oozieUrl + parentJobInfo);
        when(manualCommittedJobClient.getJobInfo(parentJobInfo)).thenReturn(manualParentJob);

        //Oozie coordinated job
        when(scheduledChildAction.getConsoleUrl()).thenReturn(applicationUrl);
        when(scheduledJobClient.getWorkflowActionInfo(actionInfo)).thenReturn(scheduledChildAction);

        when(scheduledChildJob.getId()).thenReturn(jobInfo);
        when(scheduledChildJob.getAppName()).thenReturn(jobAppName);
        when(scheduledChildJob.getParentId()).thenReturn(coordinatorActionInfo);
        when(scheduledChildJob.getConsoleUrl()).thenReturn(oozieUrl + jobInfo);
        when(scheduledJobClient.getJobInfo(jobInfo)).thenReturn(scheduledChildJob);

        when(oozieCoordinatorJobInfo.getConsoleUrl()).thenReturn(null);
        when(oozieCoordinatorJobInfo.getAppName()).thenReturn(coordinatorName);
        when(scheduledJobClient.getCoordJobInfo(coordinatorJobInfo)).thenReturn(oozieCoordinatorJobInfo);
    }

    /*
    Job Reference ID: Oozie Job ID
    Job Execution ID: Oozie Job ID
    Flow Reference ID: Super Parent Oozie job ID
    Flow Execution ID: Super Parent Oozie job ID
     */
    @Test
    public void testManualCommittedJob() throws Exception {
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", new HashMap<String, String>());
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, manualCommittedJobClient);

        assertEquals(jobInfo, scheduler.getJobDefId());
        assertEquals(jobInfo, scheduler.getJobExecId());
        assertEquals(parentJobInfo, scheduler.getFlowDefId());
        assertEquals(parentJobInfo, scheduler.getFlowExecId());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobDefUrl());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobExecUrl());
        assertEquals(oozieUrl + parentJobInfo, scheduler.getFlowDefUrl());
        assertEquals(oozieUrl + parentJobInfo, scheduler.getFlowExecUrl());
        assertEquals(1, scheduler.getWorkflowDepth());
        assertEquals(jobInfo, scheduler.getJobName());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    /*
    Job Reference ID: Job AppName-ActionName
    Job Execution ID: Oozie Job ID
    Flow Reference ID: Super Parent Job AppName
    Flow Execution ID: Super Parent Oozie job ID
     */
    @Test
    public void testManualCommittedJobAppNameUnique() throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put("oozie_app_name_uniqueness", "true");
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", params);
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, manualCommittedJobClient);

        assertEquals(oozieUrl + jobInfo, scheduler.getJobDefUrl());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobExecUrl());
        assertEquals(oozieUrl + parentJobInfo, scheduler.getFlowDefUrl());
        assertEquals(oozieUrl + parentJobInfo, scheduler.getFlowExecUrl());
        assertEquals(1, scheduler.getWorkflowDepth());
        assertEquals(jobAppName + "-" + actionName, scheduler.getJobName());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    /*
    Job Reference ID: C_ID-ActionName-Depth
    Job Execution ID: Oozie Job ID
    Flow Reference ID: Coordinator ID = C_ID
    Flow Execution ID: Coordinator Action ID = C_ID@1
     */
    @Test
    public void testOozieScheduledJob() throws Exception {
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", new HashMap<String, String>());
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, scheduledJobClient);

        assertEquals(coordinatorJobInfo + "-" + actionName + "-0", scheduler.getJobDefId());
        assertEquals(jobInfo, scheduler.getJobExecId());
        assertEquals(coordinatorJobInfo, scheduler.getFlowDefId());
        assertEquals(coordinatorActionInfo, scheduler.getFlowExecId());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobDefUrl());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobExecUrl());
        assertEquals(coordinatorJobInfo, scheduler.getFlowDefUrl());
        assertEquals(coordinatorActionInfo, scheduler.getFlowExecUrl());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals(coordinatorJobInfo + "-" + actionName + "-0", scheduler.getJobName());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    /*
    Job Reference ID: Job AppName-ActionName
    Job Execution ID: Oozie Job ID
    Flow Reference ID: Coordinator Job name
    Flow Execution ID: Coordinator Action ID = C_ID@1
     */
    @Test
    public void tesOozieScheduledJobAppNameUnique() throws Exception {
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("oozie_app_name_uniqueness", "true");
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", params);
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, scheduledJobClient);

        assertEquals(jobAppName + "-" + actionName, scheduler.getJobDefId());
        assertEquals(jobInfo, scheduler.getJobExecId());
        assertEquals(coordinatorName, scheduler.getFlowDefId());
        assertEquals(coordinatorActionInfo, scheduler.getFlowExecId());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobDefUrl());
        assertEquals(oozieUrl + jobInfo, scheduler.getJobExecUrl());
        assertEquals(coordinatorJobInfo, scheduler.getFlowDefUrl());
        assertEquals(coordinatorActionInfo, scheduler.getFlowExecUrl());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals(jobAppName + "-" + actionName, scheduler.getJobName());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    @Test
    public void testUserGivenTemplateArePreferredUrl() throws Exception {
        Map<String, String> params = getDefaultSchedulerParams();
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", params);
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, manualCommittedJobClient);

        assertEquals("http://oozie/search?workflow=" + jobInfo, scheduler.getJobDefUrl());
        assertEquals("http://oozie/workflows/" + jobInfo, scheduler.getJobExecUrl());
        assertEquals("http://oozie/search?workflow=" + parentJobInfo, scheduler.getFlowDefUrl());
        assertEquals("http://oozie/workflows/" + parentJobInfo, scheduler.getFlowExecUrl());
    }

    @Test
    public void testDepthCalculation() throws Exception {
        when(workflowJob.getParentId()).thenReturn(jobInfo, jobInfo, jobInfo, null);
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", new HashMap<String, String>());
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, oozieClient);

        assertEquals(1, scheduler.getWorkflowDepth());

    }

    @Test
    public void testOozieLoadInfoWithOozieClientException() throws Exception {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Failed fetching Oozie workflow " + jobInfo + " info");

        doThrow(new OozieClientException("500 Internal server error", "BOOM")).when(oozieClient).getJobInfo(anyString());
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
        new OozieScheduler("id", getOozieProperties(), schedulerConfig, oozieClient);
    }

    @Test
    public void testOozieLoadInfoWithMissingProperty() {
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
        OozieScheduler scheduler = new OozieScheduler("id", getNonOozieProperties(), schedulerConfig);

        assertEquals(null, scheduler.getFlowDefUrl());
        assertEquals(null, scheduler.getFlowDefId());
        assertEquals(null, scheduler.getFlowExecUrl());
        assertEquals(null, scheduler.getFlowExecId());

        assertEquals(null, scheduler.getJobDefUrl());
        assertEquals(null, scheduler.getJobDefId());
        assertEquals(null, scheduler.getJobExecUrl());
        assertEquals(null, scheduler.getJobExecId());

        assertEquals(null, scheduler.getJobName());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    @Test
    public void testOozieLoadInfoWithNullProperty() {
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
        OozieScheduler scheduler = new OozieScheduler("id", null, schedulerConfig);

        assertEquals(null, scheduler.getFlowDefUrl());
        assertEquals(null, scheduler.getFlowDefId());
        assertEquals(null, scheduler.getFlowExecId());
        assertEquals(null, scheduler.getFlowExecUrl());

        assertEquals(null, scheduler.getJobDefId());
        assertEquals(null, scheduler.getJobDefUrl());
        assertEquals(null, scheduler.getJobExecId());
        assertEquals(null, scheduler.getJobExecUrl());

        assertEquals(null, scheduler.getJobName());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    @Test
    public void testOozieLoadsNameFromConfData() {
        SchedulerConfigurationData schedulerConfig = makeConfig("othername", getDefaultSchedulerParams());
        OozieScheduler scheduler = new OozieScheduler("id", null, schedulerConfig);
        assertEquals("othername", scheduler.getSchedulerName());
    }
}
