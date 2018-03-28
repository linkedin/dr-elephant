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

package rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.DrElephant;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.tuning.BaselineComputeUtil;
import com.linkedin.drelephant.tuning.FitnessComputeUtil;
import com.linkedin.drelephant.tuning.JobCompleteDetector;
import com.linkedin.drelephant.util.Utils;

import common.DBTestUtil;
import controllers.AutoTuningMetricsController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import models.JobDefinition;
import models.JobExecution;
import models.JobExecution.ExecutionState;
import models.TuningJobDefinition;
import models.TuningJobExecution;
import models.TuningJobExecution.ParamSetStatus;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.Application;
import play.GlobalSettings;
import play.libs.Json;
import play.libs.WS;
import play.test.FakeApplication;

import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;


/**
 * <p>
 * Class aims to exercise all the rest end points exposed by Dr.Elephant
 * </p>
 * <p>
 * A fake application connecting to an in-memory H2 DB is started inside<br>
 * the test server which runs the test code. The global class is overridden<br>
 * so that we don't have to go through the regular application start flow.
 * </p>
 */
public class RestAPITest {

  private static final Logger logger = LoggerFactory.getLogger(RestAPITest.class);
  private static FakeApplication fakeApp;

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<String, String>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        logger.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(dbConn, gs);
  }

  /**
   * <p>
   * Rest API - Performs search by job ID
   * <br>
   * API provides information on the specific job
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>Job id</ul>
   *   <ul>Job name</ul>
   *   <ul>Job type</ul>
   * </p>
   */
  @Test
  public void testrestAppResult() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_APP_RESULT_PATH).
            setQueryParameter("id", TEST_JOB_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        final JsonNode jsonResponse = response.asJson();
        assertTrue("Job id did not match", TEST_JOB_ID1.equals(jsonResponse.path("id").asText()));
        assertTrue("Job name did not match", TEST_JOB_NAME.equals(jsonResponse.path("name").asText()));
        assertTrue("Job type did not match", TEST_JOB_TYPE.equals(jsonResponse.path("jobType").asText()));
      }
    });
  }

  @Test
  public void testRestGetCurrentRunParameters() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Boolean autoTuningEnabled = configuration.getBoolean(DrElephant.AUTO_TUNING_ENABLED, false);
    org.junit.Assume.assumeTrue(autoTuningEnabled);

    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateAutoTuningTestData1();

        JsonNode jsonNode = getTestGetCurrentRunParameterData();
        final WS.Response response = WS.url(BASE_URL + REST_GET_CURRENT_RUN_PARAMETERS).
            post(jsonNode).get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);

        final JsonNode jsonResponse = response.asJson();

        logger.info("Output of getCurrentRunParameters ");
        logger.info(jsonResponse.toString());

        assertTrue("Get current run param output did not match",
            jsonResponse.path("mapreduce.map.memory.mb").asDouble() > 0);
        assertTrue("Get current run param output did not match",
            jsonResponse.path("mapreduce.reduce.memory.mb").asDouble() > 0);
        assertTrue("Get current run param output size did not match", jsonResponse.size() == 9);

        TuningJobExecution tuningJobExecution = TuningJobExecution.find.select("*")
            .fetch(TuningJobExecution.TABLE.jobExecution, "*")
            .fetch(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.job, "*")
            .where()
            .eq(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.jobExecId,
                "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmall_countByCountry&attempt=0")
            .findUnique();

        tuningJobExecution.paramSetState = ParamSetStatus.EXECUTED;
        tuningJobExecution.jobExecution.executionState = ExecutionState.SUCCEEDED;
        tuningJobExecution.update();

        FitnessComputeUtil fitnessComputeUtil = new FitnessComputeUtil();
        fitnessComputeUtil.updateFitness();

        tuningJobExecution = TuningJobExecution.find.select("*")
            .fetch(TuningJobExecution.TABLE.jobExecution, "*")
            .fetch(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.job, "*")
            .where()
            .eq(TuningJobExecution.TABLE.jobExecution + "." + JobExecution.TABLE.jobExecId,
                "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmall_countByCountry&attempt=0")
            .findUnique();

        assertTrue("Fitness not computed", tuningJobExecution.paramSetState == ParamSetStatus.FITNESS_COMPUTED);
        assertTrue("Fitness not computed and have zero value", tuningJobExecution.fitness > 0);
      }
    });
  }

  @Test
  public void testRestGetCurrentRunParametersNewJob() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Boolean autoTuningEnabled = configuration.getBoolean(DrElephant.AUTO_TUNING_ENABLED, false);
    org.junit.Assume.assumeTrue(autoTuningEnabled);

    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateAutoTuningTestData1();

        JsonNode jsonNode = getTestGetCurrentRunParameterNewData();
        final WS.Response response = WS.url(BASE_URL + REST_GET_CURRENT_RUN_PARAMETERS).
            post(jsonNode).get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);

        final JsonNode jsonResponse = response.asJson();

        assertTrue("Get current run param output did not match",
            jsonResponse.path("mapreduce.map.memory.mb").asDouble() == 2048D);
        assertTrue("Get current run param output did not match",
            jsonResponse.path("mapreduce.reduce.memory.mb").asDouble() == 2048D);
        assertTrue("Get current run param output size did not match", jsonResponse.size() == 2);

        TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
            .where()
            .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId,
                "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew&job=countByCountryFlowSmallNew_countByCountry")
            .findUnique();

        assertTrue("New Job Not created  ",
            tuningJobDefinition.job.jobName.equals("countByCountryFlowSmallNew_countByCountry"));

        BaselineComputeUtil baselineComputeUtil = new BaselineComputeUtil();
        baselineComputeUtil.computeBaseline();

        tuningJobDefinition = TuningJobDefinition.find.select("*")
            .where()
            .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.jobDefId,
                "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew&job=countByCountryFlowSmallNew_countByCountry")
            .findUnique();

        assertTrue("Baseline not computed:averageResourceUsage  ", tuningJobDefinition.averageResourceUsage > 0);
        assertTrue("Baseline not computed:averageInputSizeInBytes  ", tuningJobDefinition.averageInputSizeInBytes > 0);
        assertTrue("Baseline not computed:averageExecutionTime  ", tuningJobDefinition.averageExecutionTime > 0);
      }
    });
  }

  private JsonNode getTestGetCurrentRunParameterNewData() {
    Map<String, String> params = new HashMap<String, String>();
    Map<String, Double> paramValueMap = new HashMap<String, Double>();
    paramValueMap.put("mapreduce.map.memory.mb", 2048D);
    paramValueMap.put("mapreduce.reduce.memory.mb", 2048D);
    Gson gson = new Gson();
    String jobParamsJson = gson.toJson(paramValueMap);

    params.put("projectName", "AzkabanHelloPigTest");
    params.put("flowDefId",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew");
    params.put("jobDefId",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew&job=countByCountryFlowSmallNew_countByCountry");
    params.put("flowExecId", "https://elephant.linkedin.com:8443/executor?execid=5221700");
    params.put("jobExecId",
        "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmallNew_countByCountry&attempt=0");
    params.put("flowDefUrl",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew");
    params.put("jobDefUrl",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmallNew&job=countByCountryFlowSmallNew_countByCountry");
    params.put("flowExecUrl", "https://elephant.linkedin.com:8443/executor?execid=5221700");
    params.put("jobExecUrl",
        "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmallNew_countByCountry&attempt=0");
    params.put("jobName", "countByCountryFlowSmallNew_countByCountry");
    params.put("defaultParams", jobParamsJson);
    params.put("scheduler", "azkaban");
    params.put("client", "azkaban");
    params.put("autoTuningJobType", "PIG");
    params.put("optimizationMetric", "RESOURCE");
    params.put("userName", "mkumar1");
    params.put("isRetry", "false");
    params.put("skipExecutionForOptimization", "false");
    JsonNode jsonNode = new ObjectMapper().valueToTree(params);
    return jsonNode;
  }

  private JsonNode getTestGetCurrentRunParameterData() {
    Map<String, String> params = new HashMap<String, String>();
    Map<String, Double> paramValueMap = new HashMap<String, Double>();
    paramValueMap.put("mapreduce.map.memory.mb", 2048D);
    paramValueMap.put("mapreduce.reduce.memory.mb", 2048D);
    Gson gson = new Gson();
    String jobParamsJson = gson.toJson(paramValueMap);

    params.put("projectName", "AzkabanHelloPigTest");
    params.put("flowDefId",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmall");
    params.put("jobDefId",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmall&job=countByCountryFlowSmall_countByCountry");
    params.put("flowExecId", "https://elephant.linkedin.com:8443/executor?execid=5221700");
    params.put("jobExecId",
        "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmall_countByCountry&attempt=0");
    params.put("flowDefUrl",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmall");
    params.put("jobDefUrl",
        "https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlowSmall&job=countByCountryFlowSmall_countByCountry");
    params.put("flowExecUrl", "https://elephant.linkedin.com:8443/executor?execid=5221700");
    params.put("jobExecUrl",
        "https://elephant.linkedin.com:8443/executor?execid=5221700&job=countByCountryFlowSmall_countByCountry&attempt=0");
    params.put("jobName", "countByCountryFlowSmall_countByCountry");
    params.put("defaultParams", jobParamsJson);
    params.put("scheduler", "azkaban");
    params.put("client", "azkaban");
    params.put("autoTuningJobType", "PIG");
    params.put("optimizationMetric", "RESOURCE");
    params.put("userName", "mkumar1");
    params.put("isRetry", "false");
    params.put("skipExecutionForOptimization", "false");
    JsonNode jsonNode = new ObjectMapper().valueToTree(params);
    return jsonNode;
  }

  /**
   * <p>
   * Rest API - Performs search by job execution ID
   * <br>
   * API returns all jobs triggered by a particular Scheduler Job
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>Job id</ul>
   *   <ul>Job execution id</ul>
   * </p>
   */
  @Test
  public void testrestJobExecResult() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOB_EXEC_RESULT_PATH).
            setQueryParameter("id", TEST_JOB_EXEC_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        final JsonNode jsonResponse = response.asJson().get(0);
        assertTrue("Job id did not match", TEST_JOB_ID1.equals(jsonResponse.path("id").asText()));
        assertTrue("Job execution id did not match", TEST_JOB_EXEC_ID1.equals(jsonResponse.path("jobExecId").asText()));
      }
    });
  }

  /**
   * <p>
   * Rest API - Performs search by flow execution ID
   * <br>
   * API returns all jobs under a particular flow execution
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>Job id</ul>
   *   <ul>Flow execution id</ul>
   * </p>
   */
  @Test
  public void testrestFlowExecResult() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_FLOW_EXEC_RESULT_PATH).
            setQueryParameter("id", TEST_FLOW_EXEC_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        final JsonNode jsonResponse = response.asJson();
        assertTrue("Job id did not match", TEST_JOB_ID1.equals(jsonResponse.findValue("id").asText()));
        assertTrue("Flow execution id did not match",
            TEST_FLOW_EXEC_ID1.equals(jsonResponse.findValue("flowExecId").asText()));
      }
    });
  }

  /**
   * <p>
   * Rest API - Perform a generic search or search by filter criteria
   * <br>
   * Test verifies if all available flows are returned
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>First job id</ul>
   *   <ul>Second job id</ul>
   * </p>
   */
  @Test
  public void testrestSearch() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_PATH).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        List<String> jobList = response.asJson().findValuesAsText("id");
        assertTrue("Job id1 missing in list", jobList.contains(TEST_JOB_ID1));
        assertTrue("Job id2 missing in list", jobList.contains(TEST_JOB_ID2));
      }
    });
  }

  /**
   * <p>
   * Rest API - Perform a search with additional params
   * <br>
   * Test verifies if specific flow is returned
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>No of jobs returned</ul>
   *   <ul>Job id</ul>
   *   <ul>Username</ul>
   *   <ul>Job type</ul>
   * </p>
   */
  @Test
  public void testrestSearchWithUsernameAndJobType() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_PATH).
            setQueryParameter("username", TEST_USERNAME).
            setQueryParameter("", TEST_JOB_TYPE).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode reponseJson = response.asJson();
        List<String> jobList = reponseJson.findValuesAsText("id");
        assertTrue("More than one row returned", jobList.size() == 1);
        assertTrue("Job id missing in response", TEST_JOB_ID1.equals(reponseJson.findValue("id").asText()));
        assertTrue("Username incorrect", TEST_USERNAME.equals(reponseJson.findValue("username").asText()));
        assertTrue("Job type incorrect", TEST_JOB_TYPE.equals(reponseJson.findValue("jobType").asText()));
      }
    });
  }

  /**
   * <p>
   * Rest API - Compares two flow executions by flow execution ID
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>Second job ID</ul>
   * </p>
   */
  @Test
  public void testrestCompare() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_COMPARE_PATH).
            setQueryParameter("flow-exec-id1", TEST_FLOW_EXEC_ID1).
            setQueryParameter("flow-exec-id2", TEST_FLOW_EXEC_ID2).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("Job id did not match", TEST_JOB_ID2.equals(response.asJson().findValue("id").asText()));
      }
    });
  }

  /**
   * <p>
   * Rest API - Provides data for plotting the flow history graph
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>First job execution ID</ul>
   *   <ul>Second job execution ID</ul>
   * </p>
   */
  @Test
  public void testrestFlowGraphData() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_FLOW_GRAPH_DATA_PATH).
            setQueryParameter("id", TEST_FLOW_DEF_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        List<String> jobList = response.asJson().findValuesAsText("jobexecurl");
        assertTrue("Job exec url1 missing in list", jobList.contains(TEST_JOB_EXEC_ID1));
        assertTrue("Job exec url2 missing in list", jobList.contains(TEST_JOB_EXEC_ID2));
      }
    });
  }

  /**
   * <p>
   * Rest API - Provides data for plotting the job history graph
   * </p>
   * <p>
   * Following assertions are made in the response json
   *   <ul>First job id</ul>
   *   <ul>Second job id</ul>
   * </p>
   */
  @Test
  public void testrestJobGraphData() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOB_GRAPH_DATA_PATH).
            setQueryParameter("id", TEST_JOB_DEF_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        List<String> jobList = response.asJson().findValuesAsText("stageid");
        assertTrue("Job id 1 missing in list", jobList.contains(TEST_JOB_ID1));
        assertTrue("Job id 2 missing in list", jobList.contains(TEST_JOB_ID2));
      }
    });
  }

  /**
   * <p>
   *   Rest API - Provides data for plotting the job history graph for time and resources
   * </p>
   */
  public void testrestJobMetricsGraphData() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOB_METRICS_GRAPH_DATA_PATH).
            setQueryParameter("id", TEST_JOB_DEF_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        List<String> jobList = response.asJson().findValuesAsText("stageid");
        assertTrue("Job id 1 missing in list", jobList.contains(TEST_JOB_ID1));
        assertTrue("Job id 2 missing in list", jobList.contains(TEST_JOB_ID2));
      }
    });
  }

  /**
   * <p>
   * Rest API  - Provides data for plotting the flow history graph for time and resources
   * </p>
   */
  @Test
  public void testrestFlowMetricsGraphData() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_FLOW_METRICS_GRAPH_DATA_PATH).
            setQueryParameter("id", TEST_FLOW_DEF_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        List<String> jobList = response.asJson().findValuesAsText("jobexecurl");
        assertTrue("Job exec url1 missing in list", jobList.contains(TEST_JOB_EXEC_ID1));
        assertTrue("Job exec url2 missing in list", jobList.contains(TEST_JOB_EXEC_ID2));
      }
    });
  }

  @Test
  public void testRestUserResourceUsage() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_USER_RESOURCE_USAGE_PATH).
            setQueryParameter("startTime", TEST_START_TIME1).
            setQueryParameter("endTime", TEST_END_TIME1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> userResources = response.asJson().elements();
        while (userResources.hasNext()) {
          JsonNode userResourceUsage = userResources.next();
          if (userResourceUsage.findValue("user").asText().equals("growth")) {
            assertTrue("Wrong resourceusage for user growth",
                userResourceUsage.findValue("resourceUsed").asDouble() == Utils.MBSecondsToGBHours(100));
            assertTrue("Wrong wastedResources for user growth",
                userResourceUsage.findValue("resourceWasted").asDouble() == Utils.MBSecondsToGBHours(30));
          } else if (userResourceUsage.findValue("user").asText().equals("metrics")) {
            assertTrue("Wrong resourceusage for user metrics",
                userResourceUsage.findValue("resourceUsed").asDouble() == Utils.MBSecondsToGBHours(200));
            assertTrue("Wrong wastedResources for user metrics",
                userResourceUsage.findValue("resourceWasted").asDouble() == Utils.MBSecondsToGBHours(40));
          } else {
            assertTrue("Unexpected user" + userResourceUsage.findValue("user").asText(), false);
          }
        }
      }
    });
  }

  @Test
  public void testRestUserResourceUsageBadInput() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_USER_RESOURCE_USAGE_PATH).
            setQueryParameter("startTime", TEST_START_TIME1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("Invalid input test failed", response.getStatus() == 400);
      }
    });
  }

  @Test
  public void testRestWorkflowForuser() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_WORKFLOW_SUMMARIES_PATH).
            setQueryParameter("username", TEST_USERNAME).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> workflowSummaries = response.asJson().elements();
        while (workflowSummaries.hasNext()) {
          JsonNode workflowSummary = workflowSummaries.next();
          Iterator<JsonNode> workflowObjects = workflowSummary.elements();
          while (workflowObjects.hasNext()) {
            JsonNode node = workflowObjects.next();
            Assert.assertEquals(node.findValue("username").asText(), "growth");
            Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
            Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
            Assert.assertEquals(node.findValue("waittime").asLong(), 20);
            Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
            Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
            Assert.assertEquals(node.findValue("severity").asText(), "None");
            Assert.assertEquals(node.findValue("queue").asText(), "misc_default");

            Iterator<JsonNode> jobs = node.findValue("jobsseverity").elements();
            while (jobs.hasNext()) {
              JsonNode job = jobs.next();
              Assert.assertEquals(job.findValue("severity").asText(), "None");
              Assert.assertEquals(job.findValue("count").asInt(), 1);
            }
          }
        }
      }
    });
  }

  @Test
  public void testRestJobForUser() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOB_SUMMARIES_PATH).
            setQueryParameter("username", TEST_USERNAME).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> jobSummaries = response.asJson().elements();
        while (jobSummaries.hasNext()) {
          JsonNode jobSummary = jobSummaries.next();
          Iterator<JsonNode> jobObjects = jobSummary.elements();
          while (jobObjects.hasNext()) {
            JsonNode node = jobObjects.next();
            Assert.assertEquals(node.findValue("username").asText(), "growth");
            Assert.assertEquals(node.findValue("jobname").asText(), "overwriter-reminder2");
            Assert.assertEquals(node.findValue("jobtype").asText(), "HadoopJava");
            Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
            Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
            Assert.assertEquals(node.findValue("waittime").asLong(), 20);
            Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
            Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
            Assert.assertEquals(node.findValue("severity").asText(), "None");
            Assert.assertEquals(node.findValue("queue").asText(), "misc_default");

            Iterator<JsonNode> tasks = node.findValue("tasksseverity").elements();
            while (tasks.hasNext()) {
              JsonNode job = tasks.next();
              Assert.assertEquals(job.findValue("severity").asText(), "None");
              Assert.assertEquals(job.findValue("count").asInt(), 1);
            }
          }
        }
      }
    });
  }

  @Test
  public void testRestApplicationForUser() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_APPLICATION_SUMMARIES_PATH).
            setQueryParameter("username", TEST_USERNAME).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);

        Iterator<JsonNode> taskSummaries = response.asJson().elements();
        while (taskSummaries.hasNext()) {
          JsonNode taskSummary = taskSummaries.next();
          Iterator<JsonNode> jobObjects = taskSummary.elements();
          while (jobObjects.hasNext()) {
            JsonNode node = jobObjects.next();
            Assert.assertEquals(node.findValue("username").asText(), "growth");
            Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
            Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
            Assert.assertEquals(node.findValue("waittime").asLong(), 20);
            Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
            Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
            Assert.assertEquals(node.findValue("severity").asText(), "None");
            Assert.assertEquals(node.findValue("queue").asText(), "misc_default");

            Iterator<JsonNode> heuristicsSummary = node.findValue("heuristicsummary").elements();

            HashMap<String, String> expectedHeuristics = new LinkedHashMap<String, String>();
            expectedHeuristics.put("Mapper Skew", "None");
            expectedHeuristics.put("Mapper GC", "None");
            expectedHeuristics.put("Mapper Time", "None");
            expectedHeuristics.put("Mapper Speed", "None");
            expectedHeuristics.put("Mapper Spill", "None");
            expectedHeuristics.put("Mapper Memory", "None");
            expectedHeuristics.put("Reducer Skew", "None");
            expectedHeuristics.put("Reducer Time", "None");
            expectedHeuristics.put("Reducer GC", "None");
            expectedHeuristics.put("Reducer Memory", "None");
            expectedHeuristics.put("Shuffle & Sort", "None");

            Iterator<String> keyIterator = expectedHeuristics.keySet().iterator();
            while (heuristicsSummary.hasNext() && keyIterator.hasNext()) {
              JsonNode job = heuristicsSummary.next();
              String key = keyIterator.next().toString();
              Assert.assertEquals(key, job.findValue("name").asText());
              Assert.assertEquals(expectedHeuristics.get(key), job.findValue("severity").asText());
            }
          }
        }
      }
    });
  }

  @Test
  public void testRestWorkflowFromId() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_WORKFLOWS_PATH).
            setQueryParameter("workflowid", TEST_FLOW_EXEC_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> workflows = response.asJson().elements();
        while (workflows.hasNext()) {
          JsonNode node = workflows.next();
          Assert.assertEquals(node.findValue("username").asText(), "growth");
          Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
          Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
          Assert.assertEquals(node.findValue("waittime").asLong(), 20);
          Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
          Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
          Assert.assertEquals(node.findValue("severity").asText(), "None");
          Assert.assertEquals(node.findValue("queue").asText(), "misc_default");
        }
      }
    });
  }

  @Test
  public void testRestWorkflowFromIdIsEmpty() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_WORKFLOWS_PATH).
            setQueryParameter("workflowid", "this_is_a_random_id").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode workflows = response.asJson();
        Assert.assertEquals(workflows.get("username"), null);
        Assert.assertEquals(workflows.get("starttime"), null);
        Assert.assertEquals(workflows.get("finishtime"), null);
        Assert.assertEquals(workflows.get("waittime"), null);
        Assert.assertEquals(workflows.get("resourceused"), null);
        Assert.assertEquals(workflows.get("resourcewasted"), null);
        Assert.assertEquals(workflows.get("severity"), null);
        Assert.assertEquals(workflows.get("queue"), null);
      }
    });
  }

  @Test
  public void testRestJobFromId() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOBS_PATH).
            setQueryParameter("jobid", TEST_JOB_EXEC_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> jobs = response.asJson().elements();
        while (jobs.hasNext()) {
          JsonNode node = jobs.next();
          Assert.assertEquals(node.findValue("username").asText(), "growth");
          Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
          Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
          Assert.assertEquals(node.findValue("waittime").asLong(), 20);
          Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
          Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
          Assert.assertEquals(node.findValue("severity").asText(), "None");
          Assert.assertEquals(node.findValue("queue").asText(), "misc_default");
        }
      }
    });
  }

  @Test
  public void testRestJobFromIdIsEmpty() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_JOBS_PATH).
            setQueryParameter("jobid", "this_is_a_random_job_id").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode jobs = response.asJson();
        Assert.assertEquals(jobs.get("username"), null);
        Assert.assertEquals(jobs.get("starttime"), null);
        Assert.assertEquals(jobs.get("finishtime"), null);
        Assert.assertEquals(jobs.get("waittime"), null);
        Assert.assertEquals(jobs.get("resourceused"), null);
        Assert.assertEquals(jobs.get("resourcewasted"), null);
        Assert.assertEquals(jobs.get("severity"), null);
        Assert.assertEquals(jobs.get("queue"), null);
      }
    });
  }

  @Test
  public void testApplicationFromId() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_APPLICATIONS_PATH).
            setQueryParameter("applicationid", TEST_JOB_ID1).
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> applications = response.asJson().elements();
        while (applications.hasNext()) {
          JsonNode node = applications.next();
          Assert.assertEquals(node.findValue("username").asText(), "growth");
          Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
          Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
          Assert.assertEquals(node.findValue("waittime").asLong(), 20);
          Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
          Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
          Assert.assertEquals(node.findValue("severity").asText(), "None");
          Assert.assertEquals(node.findValue("queue").asText(), "misc_default");
          Assert.assertEquals(node.findValue("trackingurl").asText(),
              "http://elephant.linkedin.com:19888/jobhistory/job/job_1458194917883_1453361");
        }
      }
    });
  }

  @Test
  public void testApplicationFromIdIsEmpty() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_APPLICATIONS_PATH).
            setQueryParameter("applicationid", "random_id").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode applications = response.asJson();
        Assert.assertEquals(applications.get("username"), null);
        Assert.assertEquals(applications.get("starttime"), null);
        Assert.assertEquals(applications.get("finishtime"), null);
        Assert.assertEquals(applications.get("waittime"), null);
        Assert.assertEquals(applications.get("resourceused"), null);
        Assert.assertEquals(applications.get("resourcewasted"), null);
        Assert.assertEquals(applications.get("severity"), null);
        Assert.assertEquals(applications.get("queue"), null);
      }
    });
  }

  @Test
  public void testRestSearchDataParamUserQueue() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("queue-name", "misc_default").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchDataParamTypeUser() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("job-type", "HadoopJava").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchDataParamTimeUser() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS)
            .
                setQueryParameter("username", "growth")
            .setQueryParameter("finishTimeBegin", "1460980723925")
            .setQueryParameter("finishTimeEnd", "1460980723928")
            .
                get()
            .get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchOffsetNegative() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("offset", "-1").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchLimitNegative() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("limit", "-1").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode searchNode = response.asJson();
        Assert.assertTrue(searchNode.asText().toString().isEmpty());
      }
    });
  }

  @Test
  public void testRestSearchOffsetZero() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("offset", "0").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchLimitZero() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("limit", "0").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        JsonNode searchNode = response.asJson();
        Assert.assertTrue(searchNode.asText().toString().isEmpty());
      }
    });
  }

  @Test
  public void tstRestSearchLimitOutOfLimit() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("limit", "1000").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        testRestSearchGeneric(searchNode);
      }
    });
  }

  @Test
  public void testRestSearchOffsetOutofLimit() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        final WS.Response response = WS.url(BASE_URL + REST_SEARCH_RESULTS).
            setQueryParameter("username", "growth").setQueryParameter("offset", "100").
            get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
        Iterator<JsonNode> searchNode = response.asJson().elements();
        while (searchNode.hasNext()) {
          JsonNode node = searchNode.next();
          JsonNode summaries = node.get("summaries");
          Assert.assertTrue(summaries.asText().toString().isEmpty());
        }
      }
    });
  }

  private void testRestSearchGeneric(Iterator<JsonNode> searchNode) {
    while (searchNode.hasNext()) {
      JsonNode search = searchNode.next();
      Assert.assertEquals(search.findValue("start").asInt(), 0);
      Assert.assertEquals(search.findValue("end").asInt(), 1);
      Assert.assertEquals(search.findValue("total").asInt(), 1);
      Assert.assertTrue(!search.findValue("summaries").isNull());
      Iterator<JsonNode> iterator = search.findValue("summaries").elements();

      while (iterator.hasNext()) {
        JsonNode node = iterator.next();
        Assert.assertEquals(node.findValue("username").asText(), "growth");
        Assert.assertEquals(node.findValue("starttime").asLong(), 1460980616502L);
        Assert.assertEquals(node.findValue("finishtime").asLong(), 1460980723925L);
        Assert.assertEquals(node.findValue("waittime").asLong(), 20);
        Assert.assertEquals(node.findValue("resourceused").asLong(), 100);
        Assert.assertEquals(node.findValue("resourcewasted").asLong(), 30);
        Assert.assertEquals(node.findValue("severity").asText(), "None");
        Assert.assertEquals(node.findValue("queue").asText(), "misc_default");

        Iterator<JsonNode> heuristicsSummary = node.findValue("heuristicsummary").elements();
        HashMap<String, String> expectedHeuristics = new LinkedHashMap<String, String>();
        expectedHeuristics.put("Mapper Skew", "None");
        expectedHeuristics.put("Mapper GC", "None");
        expectedHeuristics.put("Mapper Time", "None");
        expectedHeuristics.put("Mapper Speed", "None");
        expectedHeuristics.put("Mapper Spill", "None");
        expectedHeuristics.put("Mapper Memory", "None");
        expectedHeuristics.put("Reducer Skew", "None");
        expectedHeuristics.put("Reducer Time", "None");
        expectedHeuristics.put("Reducer GC", "None");
        expectedHeuristics.put("Reducer Memory", "None");
        expectedHeuristics.put("Shuffle & Sort", "None");

        Iterator<String> keyIterator = expectedHeuristics.keySet().iterator();
        while (heuristicsSummary.hasNext() && keyIterator.hasNext()) {
          JsonNode job = heuristicsSummary.next();
          String key = keyIterator.next().toString();
          Assert.assertEquals(key, job.findValue("name").asText());
          Assert.assertEquals(expectedHeuristics.get(key), job.findValue("severity").asText());
        }
      }
    }
  }

  private void populateTestData() {
    try {
      initDB();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void populateAutoTuningTestData1() {
    try {
      initAutoTuningDB1();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
