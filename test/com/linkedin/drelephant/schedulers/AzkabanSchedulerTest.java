package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;

import java.util.Properties;
import org.junit.Test;

import static com.linkedin.drelephant.schedulers.AzkabanScheduler.AZKABAN_JOB_URL;
import static com.linkedin.drelephant.schedulers.AzkabanScheduler.AZKABAN_ATTEMPT_URL;
import static com.linkedin.drelephant.schedulers.AzkabanScheduler.AZKABAN_EXECUTION_URL;
import static com.linkedin.drelephant.schedulers.AzkabanScheduler.AZKABAN_WORKFLOW_URL;
import static com.linkedin.drelephant.schedulers.AzkabanScheduler.AZKABAN_JOB_NAME;
import static org.junit.Assert.assertEquals;


public class AzkabanSchedulerTest {

  @Test
  public void testAzkabanLoadInfoWithCompleteConf() {

    AzkabanScheduler azkScheduler = new AzkabanScheduler("id", getAzkabanProperties(), getSchedulerConfData());

    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name", azkScheduler.getFlowDefUrl());
    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name", azkScheduler.getFlowDefId());
    assertEquals("https://host:9000/executor?execid=123456", azkScheduler.getFlowExecId());
    assertEquals("https://host:9000/executor?execid=123456", azkScheduler.getFlowExecUrl());

    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name&job=job-name", azkScheduler.getJobDefId());
    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name&job=job-name", azkScheduler.getJobDefUrl());
    assertEquals("https://host:9000/executor?execid=123456&job=job-name&attempt=0", azkScheduler.getJobExecId());
    assertEquals("https://host:9000/executor?execid=123456&job=job-name&attempt=0", azkScheduler.getJobExecUrl());

    assertEquals("job-name", azkScheduler.getJobName());
    assertEquals(0, azkScheduler.getWorkflowDepth());
    assertEquals("azkaban", azkScheduler.getSchedulerName());
  }

  @Test
  public void testAzkabanLoadInfoWithMissingProperty() {

    AzkabanScheduler azkScheduler = new AzkabanScheduler("id", getPropertiesAndRemove(AZKABAN_JOB_URL), getSchedulerConfData());

    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name", azkScheduler.getFlowDefUrl());
    assertEquals("https://host:9000/manager?project=project-name&flow=flow-name", azkScheduler.getFlowDefId());
    assertEquals("https://host:9000/executor?execid=123456", azkScheduler.getFlowExecId());
    assertEquals("https://host:9000/executor?execid=123456", azkScheduler.getFlowExecUrl());

    assertEquals(null, azkScheduler.getJobDefId());
    assertEquals(null, azkScheduler.getJobDefUrl());
    assertEquals("https://host:9000/executor?execid=123456&job=job-name&attempt=0", azkScheduler.getJobExecId());
    assertEquals("https://host:9000/executor?execid=123456&job=job-name&attempt=0", azkScheduler.getJobExecUrl());

    assertEquals("job-name", azkScheduler.getJobName());
    assertEquals(0, azkScheduler.getWorkflowDepth());
    assertEquals("azkaban", azkScheduler.getSchedulerName());
  }

  @Test
  public void testAzkabanLoadInfoWithNullProperty() {

    AzkabanScheduler azkScheduler = new AzkabanScheduler("id", null, getSchedulerConfData());

    assertEquals(null, azkScheduler.getFlowDefUrl());
    assertEquals(null, azkScheduler.getFlowDefId());
    assertEquals(null, azkScheduler.getFlowExecId());
    assertEquals(null, azkScheduler.getFlowExecUrl());

    assertEquals(null, azkScheduler.getJobDefId());
    assertEquals(null, azkScheduler.getJobDefUrl());
    assertEquals(null, azkScheduler.getJobExecId());
    assertEquals(null, azkScheduler.getJobExecUrl());

    assertEquals(null, azkScheduler.getJobName());
    assertEquals(0, azkScheduler.getWorkflowDepth());
    assertEquals("azkaban", azkScheduler.getSchedulerName());
  }

  @Test
  public void testAzkabanLoadsNameFromConfData() {

    AzkabanScheduler azkScheduler = new AzkabanScheduler("id", null, getSchedulerConfData("othername"));
    assertEquals("othername", azkScheduler.getSchedulerName());

  }

  private static Properties getAzkabanProperties() {
    Properties properties = new Properties();
    properties.put(AZKABAN_JOB_URL, "https://host:9000/manager?project=project-name&flow=flow-name&job=job-name");
    properties.put(AZKABAN_ATTEMPT_URL, "https://host:9000/executor?execid=123456&job=job-name&attempt=0");
    properties.put(AZKABAN_WORKFLOW_URL, "https://host:9000/manager?project=project-name&flow=flow-name");
    properties.put(AZKABAN_EXECUTION_URL, "https://host:9000/executor?execid=123456");
    properties.put(AZKABAN_JOB_NAME, "job-name");

    return properties;
  }

  private static Properties getPropertiesAndRemove(String key) {
    Properties properties = getAzkabanProperties();
    properties.remove(key);
    return properties;
  }

  private static SchedulerConfigurationData getSchedulerConfData() {
    return getSchedulerConfData("azkaban");
  }

  private static SchedulerConfigurationData getSchedulerConfData(String name) {
    return new SchedulerConfigurationData(name, null, null);
  }
}
