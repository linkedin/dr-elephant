package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import static org.junit.Assert.assertEquals;


public class OozieSchedulerTest {
  @Mocked
  OozieClient oozieClient;

  @Mocked
  WorkflowJob workflowJob;

  @Test
  public void testOozieLoadInfoWithCompleteConf() throws Exception {
    new Expectations() {{
      workflowJob.getAppName();
      result = "some-workflow-name";

      oozieClient.getJobInfo("0004167-160629080632562-oozie-oozi-W");
      result = workflowJob;
    }};

    SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
    OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig);

    assertEquals("http://oozie/search?workflow=some-workflow-name", scheduler.getFlowDefUrl());
    assertEquals("some-workflow-name", scheduler.getFlowDefId());
    assertEquals("http://oozie/workflows/0004167-160629080632562-oozie-oozi-W", scheduler.getFlowExecUrl());
    assertEquals("0004167-160629080632562-oozie-oozi-W", scheduler.getFlowExecId());

    assertEquals("http://oozie/search?workflow=some-action", scheduler.getJobDefUrl());
    assertEquals("some-action", scheduler.getJobDefId());
    assertEquals("http://oozie/workflows/0004167-160629080632562-oozie-oozi-W@some-action", scheduler.getJobExecUrl());
    assertEquals("0004167-160629080632562-oozie-oozi-W@some-action", scheduler.getJobExecId());

    assertEquals("some-action", scheduler.getJobName());
    assertEquals(0, scheduler.getWorkflowDepth());
    assertEquals("oozie", scheduler.getSchedulerName());
  }

  @Test(expected = RuntimeException.class)
  public void testOozieLoadInfoWithOozieClientException() throws Exception {
    new Expectations() {{
      workflowJob.getAppName();
      result = new OozieClientException("500 Internal server error", "BOOM");
    }};

    SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getDefaultSchedulerParams());
    OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig);
  }

  @Test
  public void testOozieLoadInfoWithoutUrlsInConf() throws Exception {
    new Expectations() {{
      workflowJob.getAppName();
      result = "some-workflow-name";

      oozieClient.getJobInfo("0004167-160629080632562-oozie-oozi-W");
      result = workflowJob;
    }};

    SchedulerConfigurationData schedulerConfig = makeConfig("oozie", getSchedulerConfigWithout(
      "oozie_job_url_template",
      "oozie_job_exec_url_template",
      "oozie_workflow_url_template",
      "oozie_workflow_exec_url_template"
    ));
    OozieScheduler scheduler = new OozieScheduler("id", getOozieProperties(), schedulerConfig);

    assertEquals("some-workflow-name", scheduler.getFlowDefUrl());
    assertEquals("some-workflow-name", scheduler.getFlowDefId());
    assertEquals("0004167-160629080632562-oozie-oozi-W", scheduler.getFlowExecUrl());
    assertEquals("0004167-160629080632562-oozie-oozi-W", scheduler.getFlowExecId());

    assertEquals("some-action", scheduler.getJobDefUrl());
    assertEquals("some-action", scheduler.getJobDefId());
    assertEquals("0004167-160629080632562-oozie-oozi-W@some-action", scheduler.getJobExecUrl());
    assertEquals("0004167-160629080632562-oozie-oozi-W@some-action", scheduler.getJobExecId());

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

  private static Properties getNonOozieProperties() {
    return new Properties();
  }

  private static Properties getOozieProperties() {
    Properties properties = new Properties();

    properties.put("oozie.action.id", "0004167-160629080632562-oozie-oozi-W@some-action");
    properties.put("oozie.job.id", "0004167-160629080632562-oozie-oozi-W");

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

  private static Map<String, String> getSchedulerConfigWithout(String ... keys) {
    Map<String, String> params = getDefaultSchedulerParams();

    for (String key : keys) {
      params.remove(key);
    }

    return params;
  }
}
