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

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.schedulers.AirflowScheduler;
import com.linkedin.drelephant.schedulers.AzkabanScheduler;
import com.linkedin.drelephant.schedulers.OozieScheduler;
import com.linkedin.drelephant.schedulers.Scheduler;

import com.linkedin.drelephant.spark.data.SparkApplicationData;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ApplicationAttemptInfoImpl;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ApplicationInfoImpl;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummaryImpl;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.JobDataImpl;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageDataImpl;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ApplicationInfo;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ApplicationAttemptInfo;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.JobData;
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData;
import java.util.ArrayList;
import java.util.Properties;
import models.AppResult;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import play.test.FakeApplication;
import play.test.Helpers;

import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(JMockit.class)
public class InfoExtractorTest {
  @Mocked
  OozieClient oozieClient;

  @Mocked
  WorkflowJob workflowJob;

  @Mocked
  WorkflowJob parentWorkflowJob;

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
  public void testGetSchedulerInstanceOozie() throws Exception {
    final String jobInfo = "0004167-160629080632562-oozie-oozi-W";
    final String jobParentInfo = "0004166-160629080632562-oozie-oozi-W";
    Properties properties = new Properties();
    properties.put("oozie.action.id", jobInfo + "@some-action");
    properties.put("oozie.job.id", jobInfo);

    new Expectations() {{
      workflowJob.getId();
      result = jobInfo;

      workflowJob.getParentId();
      result = jobParentInfo;

      oozieClient.getJobInfo(jobInfo);
      result = workflowJob;

      parentWorkflowJob.getId();
      result = jobParentInfo;

      parentWorkflowJob.getParentId();
      result = null;

      oozieClient.getJobInfo(jobParentInfo);
      result = parentWorkflowJob;
    }};

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(true, scheduler instanceof OozieScheduler);
    assertEquals("oozie", scheduler.getSchedulerName());
    assertEquals(jobParentInfo, scheduler.getFlowDefId());
    assertEquals(jobParentInfo, scheduler.getFlowExecId());
    assertEquals(jobInfo, scheduler.getJobDefId());
    assertEquals(jobInfo, scheduler.getJobExecId());
    assertEquals(jobInfo, scheduler.getJobName());
  }

  @Test
  public void testGetSchedulerInstanceNull() {
    Properties properties = new Properties();

    Scheduler scheduler = InfoExtractor.getSchedulerInstance("id", properties);
    assertEquals(null, scheduler);
  }

  @Test
  public void testLoadSchedulerInfo() {
    Properties properties = new Properties();
    properties.put(AzkabanScheduler.AZKABAN_JOB_URL,
                   "https://grid.example.com:9000/manager?project=project-name&flow=flow-name&job=job-name");
    properties.put(AzkabanScheduler.AZKABAN_ATTEMPT_URL,
                   "https://grid.example.com:9000/executor?execid=123456&job=job-name&attempt=0");
    properties.put(AzkabanScheduler.AZKABAN_WORKFLOW_URL,
                   "https://grid.example.com:9000/manager?project=project-name&flow=flow-name");
    properties.put(AzkabanScheduler.AZKABAN_EXECUTION_URL,
                   "https://grid.example.com:9000/executor?execid=123456");
    properties.put(AzkabanScheduler.AZKABAN_JOB_NAME, "job-name");

    SchedulerConfigurationData schedulerConfigurationData = new SchedulerConfigurationData("azkaban", null, null);

    Scheduler scheduler = new AzkabanScheduler("id", properties, schedulerConfigurationData);

    AppResult result = new AppResult();

    HadoopApplicationData data =
      new HadoopApplicationData() {
        String appId = "application_5678";
        Properties conf = new Properties();
        ApplicationType applicationType = new ApplicationType("foo");

        @Override
        public String getAppId() {
          return appId;
        }

        @Override
        public Properties getConf() {
          return conf;
        }

        @Override
        public ApplicationType getApplicationType() {
          return applicationType;
        }

        @Override
        public boolean isEmpty() {
          return false;
        }
      };

    InfoExtractor.loadSchedulerInfo(result, data, scheduler);

    assertEquals(result.scheduler, "azkaban");

    // CHECKME
    // assertFalse(StringUtils.isEmpty(result.getJobExecId()));
    // assertFalse(StringUtils.isEmpty(result.getJobDefId()));
    // assertFalse(StringUtils.isEmpty(result.getFlowExecId()));
    // assertFalse(StringUtils.isEmpty(result.getFlowDefId()));
    // assertFalse(StringUtils.isEmpty(result.getJobExecUrl()));
    // assertFalse(StringUtils.isEmpty(result.getJobDefUrl()));
    // assertFalse(StringUtils.isEmpty(result.getFlowExecUrl()));
    // assertFalse(StringUtils.isEmpty(result.getFlowDefUrl()));
  }

  @Test
  public void testLoadInfoMapReduce() {
    final String JOB_DEF_URL = "https://grid.example.com:9000/manager?project=project-name&flow=flow-name&job=job-name";
    final String JOB_EXEC_URL =  "https://grid.example.com:9000/executor?execid=123456&job=job-name&attempt=0";
    final String FLOW_DEF_URL = "https://grid.example.com:9000/manager?project=project-name&flow=flow-name";
    final String FLOW_EXEC_URL = "https://grid.example.com:9000/executor?execid=123456";
    final String JOB_NAME = "job-name";
    Properties properties = new Properties();
    properties.put(AzkabanScheduler.AZKABAN_JOB_URL, JOB_DEF_URL);
    properties.put(AzkabanScheduler.AZKABAN_ATTEMPT_URL, JOB_EXEC_URL );
    properties.put(AzkabanScheduler.AZKABAN_WORKFLOW_URL, FLOW_DEF_URL);
    properties.put(AzkabanScheduler.AZKABAN_EXECUTION_URL, FLOW_EXEC_URL);
    properties.put(AzkabanScheduler.AZKABAN_JOB_NAME, JOB_NAME);

    AppResult result = new AppResult();

    HadoopApplicationData data =
        (new MapReduceApplicationData()).setAppId("application_5678").setJobConf(properties);

    InfoExtractor.loadInfo(result, data);

    assertTrue(result.jobDefId.equals(JOB_DEF_URL));
    assertTrue(result.jobExecId.equals(JOB_EXEC_URL));
    assertTrue(result.flowDefId.equals(FLOW_DEF_URL));
    assertTrue(result.flowExecId.equals(FLOW_EXEC_URL));
  }

  @Test
  public void testLoadInfoSpark() {
    final String JOB_DEF_URL = "https://grid.example.com:9000/manager?project=project-name&flow=flow-name&job=job-name";
    final String JOB_EXEC_URL =  "https://grid.example.com:9000/executor?execid=123456&job=job-name&attempt=0";
    final String FLOW_DEF_URL = "https://grid.example.com:9000/manager?project=project-name&flow=flow-name";
    final String FLOW_EXEC_URL = "https://grid.example.com:9000/executor?execid=123456";
    final String JAVA_EXTRA_OPTIONS = "spark.driver.extraJavaOptions";
    Map<String,String> properties = new HashMap<String,String>();
    properties = properties.$plus(new Tuple2<String, String>(JAVA_EXTRA_OPTIONS, "-Dazkaban.link.workflow.url=" + FLOW_DEF_URL +
        " -Dazkaban.link.job.url=" + JOB_DEF_URL +
        " -Dazkaban.link.execution.url=" + FLOW_EXEC_URL +
        " -Dazkaban.link.attempt.url=" + JOB_EXEC_URL));

    AppResult result = new AppResult();

    HadoopApplicationData data = new SparkApplicationData("application_5678",
            properties,
        new ApplicationInfoImpl("", "", new Vector<ApplicationAttemptInfoImpl>(0,1,0)),
            new Vector<JobData>(0,1,0),
            new Vector<StageData>(0,1,0),
            new Vector<ExecutorSummary>(0,1,0));

    InfoExtractor.loadInfo(result, data);

    assertTrue(result.jobDefId.equals(JOB_DEF_URL));
    assertTrue(result.jobExecId.equals(JOB_EXEC_URL));
    assertTrue(result.flowDefId.equals(FLOW_DEF_URL));
    assertTrue(result.flowExecId.equals(FLOW_EXEC_URL));
  }

  @Test
  public void testLoadInfoSparkNoConfig() {
    Map<String,String> properties = new HashMap<String,String>();

    AppResult result = new AppResult();

    HadoopApplicationData data = new SparkApplicationData("application_5678",
        properties,
        new ApplicationInfoImpl("", "", new Vector<ApplicationAttemptInfoImpl>(0,1,0)),
        new Vector<JobData>(0,1,0),
        new Vector<StageData>(0,1,0),
        new Vector<ExecutorSummary>(0,1,0));

    // test to make sure loadInfo does not throw exception if properties are not defined
    InfoExtractor.loadInfo(result, data);

    assertTrue(result.jobDefId.isEmpty());
    assertTrue(result.jobExecId.isEmpty());
    assertTrue(result.flowDefId.isEmpty());
    assertTrue(result.flowExecId.isEmpty());
  }
}
