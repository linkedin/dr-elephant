package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static com.linkedin.drelephant.schedulers.AirflowScheduler.AIRFLOW_DAG_ID;
import static com.linkedin.drelephant.schedulers.AirflowScheduler.AIRFLOW_DAG_RUN_EXECUTION_DATE;
import static com.linkedin.drelephant.schedulers.AirflowScheduler.AIRFLOW_TASK_ID;
import static com.linkedin.drelephant.schedulers.AirflowScheduler.AIRFLOW_TASK_INSTANCE_EXECUTION_DATE;

import static org.junit.Assert.assertEquals;


public class AirflowSchedulerTest {

  @Test
  public void testAirflowLoadInfoWithCompleteConf() {

    AirflowScheduler airScheduler = new AirflowScheduler("id", getAirflowProperties(), getSchedulerConfData());

    assertEquals("http://localhost:1717/admin/airflow/graph?dag_id=dag_id", airScheduler.getFlowDefUrl());
    assertEquals("dag_id", airScheduler.getFlowDefId());
    assertEquals("http://localhost:1717/admin/airflow/graph?dag_id=dag_id&execution_date=dag_run_execution_date", airScheduler.getFlowExecUrl());
    assertEquals("dag_id/dag_run_execution_date", airScheduler.getFlowExecId());

    assertEquals("http://localhost:1717/admin/airflow/code?dag_id=dag_id&task_id=task_id", airScheduler.getJobDefUrl());
    assertEquals("dag_id/task_id", airScheduler.getJobDefId());
    assertEquals("http://localhost:1717/admin/airflow/log?dag_id=dag_id&task_id=task_id&execution_date=task_instance_execution_date", airScheduler.getJobExecUrl());
    assertEquals("dag_id/dag_run_execution_date/task_id/task_instance_execution_date", airScheduler.getJobExecId());

    assertEquals("task_id", airScheduler.getJobName());
    assertEquals(0, airScheduler.getWorkflowDepth());
    assertEquals("airflow", airScheduler.getSchedulerName());

  }

  @Test
  public void testAirflowLoadInfoWithMissingProperty() {

    AirflowScheduler airScheduler = new AirflowScheduler("id", getPropertiesAndRemove(AIRFLOW_TASK_ID), getSchedulerConfData());

    assertEquals("http://localhost:1717/admin/airflow/graph?dag_id=dag_id", airScheduler.getFlowDefUrl());
    assertEquals("dag_id", airScheduler.getFlowDefId());
    assertEquals("http://localhost:1717/admin/airflow/graph?dag_id=dag_id&execution_date=dag_run_execution_date", airScheduler.getFlowExecUrl());
    assertEquals("dag_id/dag_run_execution_date", airScheduler.getFlowExecId());

    assertEquals(null, airScheduler.getJobDefUrl());
    assertEquals(null, airScheduler.getJobDefId());
    assertEquals(null, airScheduler.getJobExecUrl());
    assertEquals(null, airScheduler.getJobExecId());

    assertEquals(null, airScheduler.getJobName());
    assertEquals(0, airScheduler.getWorkflowDepth());
    assertEquals("airflow", airScheduler.getSchedulerName());
  }

  @Test
  public void testAirflowLoadInfoWithNullProperty() {

    AirflowScheduler airScheduler = new AirflowScheduler("id", null, getSchedulerConfData());

    assertEquals(null, airScheduler.getFlowDefUrl());
    assertEquals(null, airScheduler.getFlowDefId());
    assertEquals(null, airScheduler.getFlowExecId());
    assertEquals(null, airScheduler.getFlowExecUrl());

    assertEquals(null, airScheduler.getJobDefId());
    assertEquals(null, airScheduler.getJobDefUrl());
    assertEquals(null, airScheduler.getJobExecId());
    assertEquals(null, airScheduler.getJobExecUrl());

    assertEquals(null, airScheduler.getJobName());
    assertEquals(0, airScheduler.getWorkflowDepth());
    assertEquals("airflow", airScheduler.getSchedulerName());
  }

  @Test
  public void testAirflowLoadsNameFromConfData() {

    AirflowScheduler airScheduler = new AirflowScheduler("id", null, getSchedulerConfData("othername"));
    assertEquals("othername", airScheduler.getSchedulerName());

  }

  private static Properties getAirflowProperties() {
    Properties properties = new Properties();
    properties.put(AIRFLOW_DAG_ID, "dag_id");
    properties.put(AIRFLOW_DAG_RUN_EXECUTION_DATE, "dag_run_execution_date");
    properties.put(AIRFLOW_TASK_ID, "task_id");
    properties.put(AIRFLOW_TASK_INSTANCE_EXECUTION_DATE, "task_instance_execution_date");

    return properties;
  }

  private static Properties getPropertiesAndRemove(String key) {
    Properties properties = getAirflowProperties();
    properties.remove(key);
    return properties;
  }

  private static SchedulerConfigurationData getSchedulerConfData() {
    return getSchedulerConfData("airflow");
  }

  private static SchedulerConfigurationData getSchedulerConfData(String name) {
    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put("airflowbaseurl", "http://localhost:1717");
    return new SchedulerConfigurationData(name, null, paramMap);
  }
}
