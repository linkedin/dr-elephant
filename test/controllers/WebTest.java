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

package controllers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import controllers.api.v1.JsonKeys;
import controllers.api.v1.Web;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;
import play.test.Helpers;

import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static play.test.Helpers.*;


public class WebTest {
  private static final Logger logger = LoggerFactory.getLogger(WebTest.class);
  private static FakeApplication fakeApp;

  private static final Double delta = 0.0000001;

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

  @After
  public void stop() {
    Helpers.stop(fakeApp);
  }

  private void populateMockData() {
    try {
      initDBForExceptionFingerPrinting();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetExceptionDetailJsonForFlow1() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateMockData();
        JsonObject exceptionDetail = Web.getExceptionDetailsJSON("https://elephant.linkedin.com:8443/executor?execid=1");
        JsonArray jobExceptionDetails = exceptionDetail.getAsJsonArray("workflow-exceptions");
        for (JsonElement jobException : jobExceptionDetails) {
          JsonObject jobExceptionJsonObject = jobException.getAsJsonObject();
          Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.NAME).getAsString(), "job_name_1");
          Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.ID).getAsString(), "job_name_1");
          Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.TYPE).getAsString(), "MR");
          JsonArray application = jobExceptionJsonObject.getAsJsonArray("applications");
          Assert.assertEquals(application.size(), 1);
          for (JsonElement element : application) {
            JsonObject applicationJsonObject = element.getAsJsonObject();
            Assert.assertEquals(applicationJsonObject.get(JsonKeys.NAME).getAsString(), ".....");
            Assert.assertEquals(applicationJsonObject.get(JsonKeys.TASKS).getAsJsonArray().size(), 0);
            Assert.assertEquals(applicationJsonObject.get(JsonKeys.EXCEPTION_SUMMARY).getAsString(), "stack_trace_1");
          }
        }
      }
    });
  }

  @Test
  public void testGetExceptionDetailJsonForFlow2() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateMockData();
        JsonObject exceptionDetail = Web.getExceptionDetailsJSON("https://elephant.linkedin.com:8443/executor?execid=2");
        JsonArray jobExceptionDetails = exceptionDetail.getAsJsonArray(JsonKeys.WORKFLOW_EXCEPTIONS);
        checkMRJobExceptionDetailElementForFlow2(jobExceptionDetails.get(0));
        checkSparkJobExceptionDetailElementForFlow2(jobExceptionDetails.get(1));
        checkTonyJobExceptionDetailElementForFlow2(jobExceptionDetails.get(2));
      }
    });
  }

  private void checkMRJobExceptionDetailElementForFlow2(JsonElement jobExceptionDetail) {
    JsonObject jobExceptionJsonObject = jobExceptionDetail.getAsJsonObject();
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.NAME).getAsString(), "job_name_2_1");
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.ID).getAsString(), "job_name_2_1");
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.TYPE).getAsString(), "MR");
    JsonArray application = jobExceptionJsonObject.getAsJsonArray(JsonKeys.APPLICATIONS);
    Assert.assertEquals(application.size(), 1);
    for (JsonElement element : application) {
      JsonObject applicationJsonObject = element.getAsJsonObject();
      Assert.assertEquals(applicationJsonObject.get(JsonKeys.NAME).getAsString(), "job_id_1");
      JsonArray taskExceptionDetailArray = applicationJsonObject.getAsJsonArray(JsonKeys.TASKS);
      Assert.assertEquals(taskExceptionDetailArray.size(), 1);
      Assert.assertEquals(applicationJsonObject.get(JsonKeys.EXCEPTION_SUMMARY).getAsString(), "");
      Assert.assertEquals("UNKNOWN", applicationJsonObject.get(JsonKeys.EXCEPTION_CLASSIFICATION).getAsString());
      for (JsonElement taskExceptionDetail : taskExceptionDetailArray) {
        JsonObject taskExceptionDetailObject = taskExceptionDetail.getAsJsonObject();
        Assert.assertEquals(taskExceptionDetailObject.get(JsonKeys.NAME).getAsString(), "task_id_1");
        Assert.assertEquals(taskExceptionDetailObject.get(JsonKeys.EXCEPTION_SUMMARY).getAsString(), "stack_trace_2");
      }
    }
  }

  private void checkSparkJobExceptionDetailElementForFlow2(JsonElement jobExceptionDetail) {
    JsonObject jobExceptionJsonObject = jobExceptionDetail.getAsJsonObject();
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.NAME).getAsString(), "job_name_2_2");
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.ID).getAsString(), "job_name_2_2");
    Assert.assertEquals(jobExceptionJsonObject.get(JsonKeys.TYPE).getAsString(), "SPARK");

    JsonArray application = jobExceptionJsonObject.getAsJsonArray(JsonKeys.APPLICATIONS);
    Assert.assertEquals(application.size(), 1);
    for (JsonElement element : application) {
      JsonObject applicationJsonObject = element.getAsJsonObject();
      Assert.assertEquals(applicationJsonObject.get(JsonKeys.NAME).getAsString(), "application_id_1");
      Assert.assertEquals("UNKNOWN", applicationJsonObject.get(JsonKeys.EXCEPTION_CLASSIFICATION).getAsString());
      JsonArray taskExceptionDetail = applicationJsonObject.getAsJsonArray(JsonKeys.TASKS);
      Assert.assertEquals(taskExceptionDetail.size(), 0);
      JsonObject exceptionSummaryElementJsonObject = applicationJsonObject.get(JsonKeys.EXCEPTION_SUMMARY)
          .getAsJsonArray()
          .get(0)
          .getAsJsonObject();
      Assert.assertEquals(exceptionSummaryElementJsonObject
          .get("exceptionName")
          .getAsString(), "Caused by: java.lang.ClassNotFoundException");
      Assert.assertEquals(exceptionSummaryElementJsonObject
          .get("exceptionStackTrace")
          .getAsString(), "ABCD");
      Assert.assertEquals(exceptionSummaryElementJsonObject
          .get("weightOfException")
          .getAsString(), "5");

    }
  }

  private void checkTonyJobExceptionDetailElementForFlow2(JsonElement jobExceptionDetail) {
    JsonObject jobExceptionJsonObject = jobExceptionDetail.getAsJsonObject();
    Assert.assertEquals("job_name_2_3", jobExceptionJsonObject.get(JsonKeys.NAME).getAsString());
    Assert.assertEquals("job_name_2_3", jobExceptionJsonObject.get(JsonKeys.ID).getAsString());
    Assert.assertEquals("TONY", jobExceptionJsonObject.get(JsonKeys.TYPE).getAsString());
    JsonArray application = jobExceptionJsonObject.getAsJsonArray(JsonKeys.APPLICATIONS);
    JsonObject applicationJsonObject = application.get(0).getAsJsonObject();
    Assert.assertEquals("Not a TONY_INFRA_ERROR ", "TONY_INFRA_ERROR",
        applicationJsonObject.get(JsonKeys.EXCEPTION_CLASSIFICATION).getAsString());
    }
  }