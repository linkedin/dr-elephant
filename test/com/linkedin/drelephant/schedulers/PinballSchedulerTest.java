package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;

import java.util.Properties;
import org.junit.Test;

import static com.linkedin.drelephant.schedulers.PinballScheduler.PINBALL_WORKFLOW;
import static com.linkedin.drelephant.schedulers.PinballScheduler.PINBALL_INSTANCE;
import static com.linkedin.drelephant.schedulers.PinballScheduler.PINBALL_JOB;
import static com.linkedin.drelephant.schedulers.PinballScheduler.PINBALL_EXECUTION;
import static com.linkedin.drelephant.schedulers.PinballScheduler.PINBALL_BASE_URL;

import static org.junit.Assert.assertEquals;


public class PinballSchedulerTest {

  @Test
  public void testPinballLoadInfoWithCompleteConf() {
    PinballScheduler pinballScheduler = new PinballScheduler("id", getPinballProperties(), getSchedulerConfData());

    assertEquals("http://localhost:8080/instances/?workflow=workflow_name", pinballScheduler.getFlowDefUrl());
    assertEquals("workflow_name", pinballScheduler.getFlowDefId());
    assertEquals("http://localhost:8080/jobs/?workflow=workflow_name&instance=workflow_instance", pinballScheduler.getFlowExecUrl());
    assertEquals("workflow_name/workflow_instance", pinballScheduler.getFlowExecId());

    assertEquals("http://localhost:8080/executions/?workflow=workflow_name&instance=workflow_instance&job=job_name", pinballScheduler.getJobDefUrl());
    assertEquals("workflow_name/job_name", pinballScheduler.getJobDefId());
    assertEquals("http://localhost:8080/execution/?workflow=workflow_name&instance=workflow_instance&job=job_name&execution=job_execution", pinballScheduler.getJobExecUrl());
    assertEquals("workflow_name/workflow_instance/job_name/job_execution", pinballScheduler.getJobExecId());

    assertEquals("job_name", pinballScheduler.getJobName());
    assertEquals(0, pinballScheduler.getWorkflowDepth());
    assertEquals("pinball", pinballScheduler.getSchedulerName());
  }

  @Test
  public void testPinballLoadInfoWithMissingProperty() {
    PinballScheduler pinballScheduler = new PinballScheduler("id", getPropertiesAndRemove(PINBALL_JOB), getSchedulerConfData());

    assertEquals("http://localhost:8080/instances/?workflow=workflow_name", pinballScheduler.getFlowDefUrl());
    assertEquals("workflow_name", pinballScheduler.getFlowDefId());
    assertEquals("http://localhost:8080/jobs/?workflow=workflow_name&instance=workflow_instance", pinballScheduler.getFlowExecUrl());
    assertEquals("workflow_name/workflow_instance", pinballScheduler.getFlowExecId());

    assertEquals(null, pinballScheduler.getJobDefUrl());
    assertEquals(null, pinballScheduler.getJobDefId());
    assertEquals(null, pinballScheduler.getJobExecUrl());
    assertEquals(null, pinballScheduler.getJobExecId());

    assertEquals(null, pinballScheduler.getJobName());
    assertEquals(0, pinballScheduler.getWorkflowDepth());
    assertEquals("pinball", pinballScheduler.getSchedulerName());
  }

  @Test
  public void testPinballLoadInfoWithNullProperty() {
    PinballScheduler pinballScheduler = new PinballScheduler("id", null, getSchedulerConfData());

    assertEquals(null, pinballScheduler.getFlowDefUrl());
    assertEquals(null, pinballScheduler.getFlowDefId());
    assertEquals(null, pinballScheduler.getFlowExecId());
    assertEquals(null, pinballScheduler.getFlowExecUrl());

    assertEquals(null, pinballScheduler.getJobDefId());
    assertEquals(null, pinballScheduler.getJobDefUrl());
    assertEquals(null, pinballScheduler.getJobExecId());
    assertEquals(null, pinballScheduler.getJobExecUrl());

    assertEquals(null, pinballScheduler.getJobName());
    assertEquals(0, pinballScheduler.getWorkflowDepth());
    assertEquals("pinball", pinballScheduler.getSchedulerName());
  }

  @Test
  public void testPinballLoadsNameFromConfData() {
    PinballScheduler pinballScheduler = new PinballScheduler("id", null, getSchedulerConfData("othername"));
    assertEquals("othername", pinballScheduler.getSchedulerName());
  }

  private static Properties getPinballProperties() {
    Properties properties = new Properties();
    properties.put(PINBALL_WORKFLOW, "workflow_name");
    properties.put(PINBALL_INSTANCE, "workflow_instance");
    properties.put(PINBALL_JOB, "job_name");
    properties.put(PINBALL_EXECUTION, "job_execution");
    properties.put(PINBALL_BASE_URL, "http://localhost:8080");

    return properties;
  }

  private static Properties getPropertiesAndRemove(String key) {
    Properties properties = getPinballProperties();
    properties.remove(key);
    return properties;
  }

  private static SchedulerConfigurationData getSchedulerConfData() {
    return getSchedulerConfData("pinball");
  }

  private static SchedulerConfigurationData getSchedulerConfData(String name) {
    return new SchedulerConfigurationData(name, null, null);
  }
}
