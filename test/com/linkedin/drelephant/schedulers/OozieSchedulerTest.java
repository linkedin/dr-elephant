package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
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
    public static final String jobInfo = "0004167-160629080632562-oozie-oozi-W";
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Mock
    OozieClient oozieClient;
    @Mock
    WorkflowJob workflowJob;

    private static Properties getNonOozieProperties() {
        return new Properties();
    }

    private static Properties getOozieProperties() {
        Properties properties = new Properties();

        properties.put("oozie.action.id", jobInfo + "@some-action");
        properties.put("oozie.job.id", jobInfo);

        return properties;
    }

    private static SchedulerConfigurationData makeConfig(String name, Map<String, String> params) {
        return new SchedulerConfigurationData(name, null, params);
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

    @Test
    public void testOozieLoadInfoWithCompleteConf() throws Exception {
        when(workflowJob.getAppName()).thenReturn("some-workflow-name");
        doReturn(workflowJob).when(oozieClient).getJobInfo(eq(jobInfo));

        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, oozieClient);

        assertEquals("http://oozie/search?workflow=some-workflow-name", scheduler.getFlowDefUrl());
        assertEquals("some-workflow-name", scheduler.getFlowDefId());
        assertEquals("http://oozie/workflows/" + jobInfo, scheduler.getFlowExecUrl());
        assertEquals(jobInfo, scheduler.getFlowExecId());

        assertEquals("http://oozie/search?workflow=some-action", scheduler.getJobDefUrl());
        assertEquals("some-action", scheduler.getJobDefId());
        assertEquals("http://oozie/workflows/" + jobInfo + "@some-action", scheduler.getJobExecUrl());
        assertEquals(jobInfo + "@some-action", scheduler.getJobExecId());

        assertEquals("some-action", scheduler.getJobName());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals("oozie", scheduler.getSchedulerName());
    }

    @Before
    public void setUp() throws OozieClientException {
        when(workflowJob.getAppName()).thenReturn("some-workflow-name");
        when(oozieClient.getJobInfo(eq(jobInfo))).thenReturn(workflowJob);
    }

    @Test
    public void testOozieLoadInfoWithOozieClientException() throws Exception {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Failed fetching Oozie workflow " + jobInfo + " info");

        doThrow(new OozieClientException("500 Internal server error", "BOOM")).when(oozieClient).getJobInfo(anyString());
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, oozieClient);
    }

    @Test
    public void testOozieLoadInfoWithoutUrlsInConf() throws Exception {
        SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getSchedulerConfigWithout(
                "oozie_job_url_template",
                "oozie_job_exec_url_template",
                "oozie_workflow_url_template",
                "oozie_workflow_exec_url_template"
        ));
        OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig, oozieClient);

        assertEquals("some-workflow-name", scheduler.getFlowDefUrl());
        assertEquals("some-workflow-name", scheduler.getFlowDefId());
        assertEquals(jobInfo, scheduler.getFlowExecUrl());
        assertEquals(jobInfo, scheduler.getFlowExecId());

        assertEquals("some-action", scheduler.getJobDefUrl());
        assertEquals("some-action", scheduler.getJobDefId());
        assertEquals(jobInfo + "@some-action", scheduler.getJobExecUrl());
        assertEquals(jobInfo + "@some-action", scheduler.getJobExecId());

        assertEquals("some-action", scheduler.getJobName());
        assertEquals(0, scheduler.getWorkflowDepth());
        assertEquals("oozie", scheduler.getSchedulerName());
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
