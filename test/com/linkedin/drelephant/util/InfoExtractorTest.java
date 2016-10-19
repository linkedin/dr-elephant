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

package com.linkedin.drelephant.util;

import com.linkedin.drelephant.schedulers.AirflowScheduler;
import com.linkedin.drelephant.schedulers.AzkabanScheduler;
import com.linkedin.drelephant.schedulers.Scheduler;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import play.test.FakeApplication;
import play.test.Helpers;

import static org.junit.Assert.assertEquals;


public class InfoExtractorTest {

  private FakeApplication app;

  @Before
  public void startApp() throws Exception {
    app = Helpers.fakeApplication(Helpers.inMemoryDatabase());
    Helpers.start(app);
  }

  @After
  public void stopApp() throws Exception {
    Helpers.stop(app);
  }

  @Test
  public void testGetSchedulerInstanceAzkaban() {
    Properties properties = new Properties();
    properties.put(AzkabanScheduler.AZKABAN_WORKFLOW_URL, "azkaban_workflow_url");
    properties.put(AzkabanScheduler.AZKABAN_JOB_URL, "azkaba_job_url");
    properties.put(AzkabanScheduler.AZKABAN_EXECUTION_URL, "azkaban_execution_url");
    properties.put(AzkabanScheduler.AZKABAN_ATTEMPT_URL, "azkaba_attempt_url");
    properties.put(AzkabanScheduler.AZKABAN_JOB_NAME, "azkaba_job_name");

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(true, scheduler instanceof AzkabanScheduler);
    assertEquals("azkaban_workflow_url", scheduler.getFlowDefId());
    assertEquals("azkaba_job_url", scheduler.getJobDefId());
    assertEquals("azkaban_execution_url", scheduler.getFlowExecId());
    assertEquals("azkaba_attempt_url", scheduler.getJobExecId());
    assertEquals("azkaba_job_name", scheduler.getJobName());
    assertEquals("azkaban", scheduler.getSchedulerName());
  }

  @Test
  public void testGetSchedulerInstanceAirflow() {
    Properties properties = new Properties();
    properties.put(AirflowScheduler.AIRFLOW_DAG_ID, "airflow_dag_id");
    properties.put(AirflowScheduler.AIRFLOW_DAG_RUN_EXECUTION_DATE, "airflow_dag_run_execution_date");
    properties.put(AirflowScheduler.AIRFLOW_TASK_ID, "airflow_task_id");
    properties.put(AirflowScheduler.AIRFLOW_TASK_INSTANCE_EXECUTION_DATE, "airflow_task_instance_execution_date");

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(true, scheduler instanceof AirflowScheduler);
    assertEquals("airflow_dag_id", scheduler.getFlowDefId());
    assertEquals("airflow_dag_id/airflow_dag_run_execution_date", scheduler.getFlowExecId());
    assertEquals("airflow_dag_id/airflow_task_id", scheduler.getJobDefId());
    assertEquals("airflow_dag_id/airflow_dag_run_execution_date/airflow_task_id/airflow_task_instance_execution_date", scheduler.getJobExecId());
    assertEquals("airflow_task_id", scheduler.getJobName());
    assertEquals("airflow", scheduler.getSchedulerName());
  }

  @Test
  public void testGetSchedulerInstanceNull() {
    Properties properties = new Properties();

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(null, scheduler);
  }

}
