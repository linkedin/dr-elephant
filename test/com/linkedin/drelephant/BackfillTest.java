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

package com.linkedin.drelephant;

import com.google.common.collect.Lists;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.AnalyticJobGenerator;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.ElephantBackfillFetcher;
import com.linkedin.drelephant.analysis.JobType;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.priorityexecutor.Priority;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import models.AppResult;
import models.BackfillInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Before;
import org.junit.Test;
import play.test.FakeApplication;

import static common.DBTestUtil.initDB;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_KEY;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_VALUE;
import static common.TestConstants.DB_DEFAULT_DRIVER_KEY;
import static common.TestConstants.DB_DEFAULT_DRIVER_VALUE;
import static common.TestConstants.DB_DEFAULT_URL_KEY;
import static common.TestConstants.DB_DEFAULT_URL_VALUE;
import static common.TestConstants.EVOLUTION_PLUGIN_KEY;
import static common.TestConstants.EVOLUTION_PLUGIN_VALUE;
import static common.TestConstants.TEST_SERVER_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;

/**
 * Tests backfilling of jobs when Dr.Elephant comes up.
 */
public class BackfillTest {

  private static FakeApplication fakeApp;

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<String, String>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    fakeApp = fakeApplication(dbConn);
  }

  private void populateTestData() {
    try {
      initDB();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetBackfillData() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<BackfillInfo> infos = BackfillInfo.find.select("*").findList();
        assertEquals(2, infos.size());
        for (BackfillInfo info : infos) {
          if (info.appType.equals("MAPREDUCE")) {
            assertEquals(123456789, info.backfillTs);
          } else if (info.appType.equals("SPARK")) {
            assertEquals(123456779, info.backfillTs);
          } else {
            fail("Unexpected app type retrieved from backfill_info table");
          }
        }

        // Update backfill info.
        BackfillInfo info = BackfillInfo.find.byId("MAPREDUCE");
        long ts = System.currentTimeMillis();
        info.appType = "MAPREDUCE";
        info.backfillTs = ts;
        info.save();
        BackfillInfo infoAfterUpdate = BackfillInfo.find.select("*").
            where().eq("app_type", "MAPREDUCE").findUnique();
        assertEquals(ts, infoAfterUpdate.backfillTs);

        // Insert in backfill info.
        info = BackfillInfo.find.byId("TEZ");
        assertNull(info);
        info = new BackfillInfo();
        info.appType = "TEZ";
        info.backfillTs = ts;
        info.save();
        BackfillInfo infoAfterInsert = BackfillInfo.find.select("*").
            where().eq("app_type", "TEZ").findUnique();
        assertEquals(ts, infoAfterInsert.backfillTs);
      }
    });
  }

  private static void assertAppResultEntry(String appId, long startTime, long finishTime, String appName) {
    AppResult result = AppResult.find.byId(appId);
    assertNotNull(appId + " should have been found in yarn_app_result_table", result);
    assertEquals(startTime, result.startTime);
    assertEquals(finishTime, result.finishTime);
    assertEquals("default", result.queueName);
    assertEquals("user", result.username);
    assertEquals(appName, result.name);
    assertEquals(Utils.getJobIdFromApplicationId(appId), result.jobExecId);
    assertEquals("flow_123", result.flowExecId);
    assertEquals("Azkaban", result.scheduler);
    assertEquals(Severity.LOW, result.severity);
    assertEquals(1, result.workflowDepth);
    assertEquals(100, result.score);
  }

  @Test
  public void testBackfillWithNoDataInDB() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        List<BackfillInfo> infos = BackfillInfo.find.findList();
        assertEquals(0, infos.size());

        // Start runner with 2 threads.
        ElephantContext.instance().getGeneralConf().setInt("drelephant.analysis.thread.count", 2);
        final ElephantRunner runner = spy(new ElephantRunner());
        Map<String, Priority> appsToPriorityMap = Collections.synchronizedMap(new LinkedHashMap<String, Priority>());
        // 4 jobs to be returned.
        List<AnalyticJob> jobs = new ArrayList<AnalyticJob>();
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0001").
            setAppType(new ApplicationType("MAPREDUCE")).setName("mapreduce_job").setQueueName("default").
            setStartTime(1526520000000L).setFinishTime(1526520000050L).setUser("user"));
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0002").
            setAppType(new ApplicationType("SPARK")).setName("spark_job").setQueueName("default").
            setStartTime(1526600000000L).setFinishTime(1526600000050L).setUser("user"));
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0003").
            setAppType(new ApplicationType("SPARK")).setName("spark_job").setQueueName("default").
            setStartTime(1526700000000L).setFinishTime(1526700000050L).setUser("user"));
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0004").
            setAppType(new ApplicationType("MAPREDUCE")).setName("mapreduce_job").setQueueName("default").
            setStartTime(1526800000000L).setFinishTime(1526800000050L).setUser("user"));
        doReturn(new DummyAnalyticJobGenerator(appsToPriorityMap, jobs)).when(runner).getAnalyticJobGenerator();
        Thread runnerThread = new Thread(new Runnable() {
          @Override
          public void run() {
            runner.run();
          }
        });
        runnerThread.start();

        // Wait till results have been updated in DB.
        List<AppResult> results = AppResult.find.findList();
        int resultCount = results.size();
        int loopCount = 400;
        while (resultCount < 4 && loopCount > 0) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          results = AppResult.find.findList();
          resultCount = results.size();
          loopCount--;
        }

        // Check yarn_app_result.
        assertEquals(4, resultCount);
        assertAppResultEntry("application_1526555215992_0001", 1526520000000L, 1526520000050L,
            "mapreduce_job");
        assertAppResultEntry("application_1526555215992_0002", 1526600000000L, 1526600000050L,
            "spark_job");
        assertAppResultEntry("application_1526555215992_0003", 1526700000000L, 1526700000050L,
            "spark_job");
        assertAppResultEntry("application_1526555215992_0004", 1526800000000L, 1526800000050L,
            "mapreduce_job");

        // Check backfill_info.
        assertEquals(2, BackfillInfo.find.findList().size());
        BackfillInfo mapReduceInfo = BackfillInfo.find.byId("MAPREDUCE");
        assertEquals(1526800000050L, mapReduceInfo.backfillTs);
        BackfillInfo sparkInfo = BackfillInfo.find.byId("SPARK");
        assertEquals(1526700000050L, sparkInfo.backfillTs);

        // Verify if apps were submitted with NORMAL priority.
        assertEquals(4, appsToPriorityMap.size());
        for (Priority priority : appsToPriorityMap.values()) {
          assertEquals(Priority.NORMAL, priority);
        }

        // Ensure cleanup is performed.
        assertTrue("App to analytic map should have been empty", runner.getAppToAnalyticJobMap().isEmpty());
        assertNotNull("MAPREDUCE app type should have final info", runner.getFinishTimeInfo("MAPREDUCE"));
        assertTrue("Finish times map should be cleaned up for MAPREDUCE app type",
            runner.getFinishTimeInfo("MAPREDUCE")._appFinishTimesMap.isEmpty());
        assertNotNull("SPARK app type should have final info", runner.getFinishTimeInfo("SPARK"));
        assertTrue("Finish times map should be cleaned up for SPARK app type",
            runner.getFinishTimeInfo("SPARK")._appFinishTimesMap.isEmpty());

        // Stop the runner and do cleanup before exiting.
        runner.kill();
        runnerThread.interrupt();
        try {
          runnerThread.join();
        } catch (InterruptedException e) {
          // Ignore the exception.
        }
      }
    });
  }

  private static void assertBackfillInfo(String appType, long expectedBackfillTs) {
    int loopCount = 800;
    BackfillInfo info = BackfillInfo.find.byId(appType);
    while ((info == null || info.backfillTs != expectedBackfillTs) && loopCount > 0) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      info = BackfillInfo.find.byId(appType);
      loopCount--;
    }
    assertEquals(expectedBackfillTs, info.backfillTs);
  }

  @Test
  public void testBackfillWithDataInDB() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        long backfillTs = 1526400000000L;
        List<AppResult> results = AppResult.find.findList();
        int initialResultCount = results.size();
        BackfillInfo mapReduceBackfillInfo = BackfillInfo.find.byId("MAPREDUCE");
        assertNotNull(mapReduceBackfillInfo);
        mapReduceBackfillInfo.backfillTs = backfillTs;
        mapReduceBackfillInfo.save();
        BackfillInfo sparkBackfillInfo = BackfillInfo.find.byId("SPARK");
        assertNotNull(sparkBackfillInfo);
        sparkBackfillInfo.backfillTs = backfillTs;
        sparkBackfillInfo.save();
        BackfillInfo tezBackfillInfo = new BackfillInfo();
        tezBackfillInfo.appType = "TEZ";
        tezBackfillInfo.backfillTs = backfillTs;
        tezBackfillInfo.save();
        List<BackfillInfo> infos = BackfillInfo.find.findList();
        assertEquals(3, infos.size());

        // Enable backfill and start runner with 1 thread.
        ElephantContext.instance().getGeneralConf().setInt("drelephant.analysis.thread.count", 1);
        ElephantContext.instance().getGeneralConf().setBoolean("drelephant.analysis.backfill.enabled", true);
        final ElephantRunner runner = spy(new ElephantRunner());
        Map<String, Priority> appsToPriorityMap = Collections.synchronizedMap(new LinkedHashMap<String, Priority>());
        // 2 jobs to be returned by job generator.
        // For each job, set job execution priority as MIN_PRIORITY. This acts as a sentinel value and we will wait
        // during analysis till the value changes to avoid race while adding to appsToPriorityMap.
        List<AnalyticJob> jobs = new ArrayList<AnalyticJob>();
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0007").
            setAppType(new ApplicationType("MAPREDUCE")).setName("mapreduce_job_3").setQueueName("default").
            setStartTime(1526800000000L).setFinishTime(1526800000050L).setUser("user"));
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0008").
            setAppType(new ApplicationType("SPARK")).setName("spark_job_3").setQueueName("default").
            setStartTime(1526900000000L).setFinishTime(1526900000050L).setUser("user"));
        long lowestTsFromRM = 1526800000050L;
        AnalyticJobGenerator jobGenerator = new DummyAnalyticJobGenerator(appsToPriorityMap, jobs);
        doReturn(jobGenerator).when(runner).getAnalyticJobGenerator();

        // 6 jobs to be backfilled.
        List<AnalyticJob> mapReduceBackfillJobs = new ArrayList<AnalyticJob>();
        mapReduceBackfillJobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().
            setAppId("application_1526555215992_0001").setAppType(new ApplicationType("MAPREDUCE")).
            setName("mapreduce_job_1").setQueueName("default").setStartTime(1526510000000L).
            setFinishTime(1526520000150L).setUser("user").setJobExecutionPriority(Priority.MIN_PRIORITY));
        mapReduceBackfillJobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().
            setAppId("application_1526555215992_0003").setAppType(new ApplicationType("MAPREDUCE")).
            setName("mapreduce_job_2").setQueueName("default").setStartTime(1526520000000L).
            setFinishTime(1526520000050L).setUser("user").setJobExecutionPriority(Priority.MIN_PRIORITY));
        DummyElephantBackfillFetcher mapreduceBackfillFetcher =
            new DummyElephantBackfillFetcher(mapReduceBackfillJobs, "MAPREDUCE", backfillTs, lowestTsFromRM);
        when(runner.getBackfillFetcher(eq(new ApplicationType("MAPREDUCE")))).thenReturn(mapreduceBackfillFetcher);

        List<AnalyticJob> sparkBackfillJobs = new ArrayList<AnalyticJob>();
        sparkBackfillJobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0002").
            setAppType(new ApplicationType("SPARK")).setName("spark_job_1").setQueueName("default").
            setStartTime(1526510050000L).setFinishTime(1526700000060L).setUser("user").
            setJobExecutionPriority(Priority.MIN_PRIORITY));
        sparkBackfillJobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0006").
            setAppType(new ApplicationType("SPARK")).setName("spark_job_2").setQueueName("default").
            setStartTime(1526700000000L).setFinishTime(1526700000050L).setUser("user").
            setJobExecutionPriority(Priority.MIN_PRIORITY));
        DummyElephantBackfillFetcher sparkBackfillFetcher =
            new DummyElephantBackfillFetcher(sparkBackfillJobs, "SPARK", backfillTs, lowestTsFromRM);
        when(runner.getBackfillFetcher(eq(new ApplicationType("SPARK")))).thenReturn(sparkBackfillFetcher);

        // 2 jobs below will not have finish time at the time of backfill.
        List<AnalyticJob> tezBackfillJobs = new ArrayList<AnalyticJob>();
        tezBackfillJobs.add(
            ((DummyAnalyticJobGenerator.DummyAnalyticJob) new DummyAnalyticJobGenerator.DummyAnalyticJob().
            setAppId("application_1526555215992_0004").setAppType(new ApplicationType("TEZ")).
            setName("tez_job_1").setQueueName("default").setUser("user").setStartTime(1526600000000L)).
            setFinishTimeAfterAnalysis(1526600000050L).setJobExecutionPriority(Priority.MIN_PRIORITY));
        tezBackfillJobs.add(
            ((DummyAnalyticJobGenerator.DummyAnalyticJob) new DummyAnalyticJobGenerator.DummyAnalyticJob().
            setAppId("application_1526555215992_0005").setAppType(new ApplicationType("TEZ")).
            setName("tez_job_2").setQueueName("default").setUser("user").setStartTime(1526600000020L)).
            setFinishTimeAfterAnalysis(1526600000040L).setJobExecutionPriority(Priority.MIN_PRIORITY));
        ApplicationType tezAppType = new ApplicationType("TEZ");
        doReturn(tezAppType).when(runner).getAppTypeForName("TEZ");
        DummyElephantBackfillFetcher tezBackfillFetcher = new DummyElephantBackfillFetcher(
            tezBackfillJobs, "TEZ", backfillTs, lowestTsFromRM);
        when(runner.getBackfillFetcher(eq(tezAppType))).thenReturn(tezBackfillFetcher);

        Thread runnerThread = new Thread(new Runnable() {
          @Override
          public void run() {
            runner.run();
          }
        });
        runnerThread.start();

        // Wait till results have been updated in DB.
        results = AppResult.find.findList();
        int resultCount = results.size();
        int loopCount = 400;
        while (resultCount < 8 + initialResultCount && loopCount > 0) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          results = AppResult.find.findList();
          resultCount = results.size();
          loopCount--;
        }

        // Check yarn_app_result.
        assertEquals(8 + initialResultCount, resultCount);
        assertAppResultEntry("application_1526555215992_0001", 1526510000000L, 1526520000150L,
            "mapreduce_job_1");
        assertAppResultEntry("application_1526555215992_0002", 1526510050000L, 1526700000060L,
            "spark_job_1");
        assertAppResultEntry("application_1526555215992_0003", 1526520000000L, 1526520000050L,
            "mapreduce_job_2");
        assertAppResultEntry("application_1526555215992_0004", 1526600000000L, 1526600000050L,
            "tez_job_1");
        assertAppResultEntry("application_1526555215992_0005", 1526600000020L, 1526600000040L,
            "tez_job_2");
        assertAppResultEntry("application_1526555215992_0006", 1526700000000L, 1526700000050L,
            "spark_job_2");
        assertAppResultEntry("application_1526555215992_0007", 1526800000000L, 1526800000050L,
            "mapreduce_job_3");
        assertAppResultEntry("application_1526555215992_0008", 1526900000000L, 1526900000050L,
            "spark_job_3");
        DummyElephantBackfillFetcher[] backfillFetchers =
            new DummyElephantBackfillFetcher[] {mapreduceBackfillFetcher, sparkBackfillFetcher, tezBackfillFetcher};
        for (DummyElephantBackfillFetcher backfillFetcher : backfillFetchers) {
          try {
            assertTrue("Jobs for backfill (of type " + backfillFetcher.getFetcherName() + ") should have been "
                + "fetched based on lowest ts of apps retrieved from RM and backfill ts in DB",
                backfillFetcher.getJobsFetchedFlag());
          } catch (Exception e) {
            fail("Unexpected exception.");
          }
        }

        // Verify apps submitted.
        assertEquals(8, appsToPriorityMap.size());
        assertEquals(Lists.newArrayList(Priority.NORMAL, Priority.NORMAL, Priority.LOW, Priority.LOW, Priority.LOW,
            Priority.LOW, Priority.LOW, Priority.LOW), new ArrayList<Priority>(appsToPriorityMap.values()));

        String[] appTypes = new String[] {"MAPREDUCE", "SPARK", "TEZ"};
        for (String appType : appTypes) {
          ElephantRunner.FinishTimeInfo finishTimeInfo = runner.getFinishTimeInfo(appType);
          loopCount = 400;
          while ((finishTimeInfo == null || !finishTimeInfo._appFinishTimesMap.isEmpty()) && loopCount > 0) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            loopCount--;
          }
        }

        // Ensure cleanup is performed.
        assertTrue("App to analytic map should have been empty", runner.getAppToAnalyticJobMap().isEmpty());
        assertNotNull("MAPREDUCE app type should have final info", runner.getFinishTimeInfo("MAPREDUCE"));
        assertTrue("Finish times map should be cleaned up for MAPREDUCE app type",
            runner.getFinishTimeInfo("MAPREDUCE")._appFinishTimesMap.isEmpty());
        assertNotNull("SPARK app type should have final info", runner.getFinishTimeInfo("SPARK"));
        assertTrue("Finish times map should be cleaned up for SPARK app type",
            runner.getFinishTimeInfo("SPARK")._appFinishTimesMap.isEmpty());
        assertNotNull("TEZ app type should have final info", runner.getFinishTimeInfo("TEZ"));
        assertTrue("Finish times map should be cleaned up for TEZ app type",
            runner.getFinishTimeInfo("TEZ")._appFinishTimesMap.isEmpty());

        // Check backfill_info.
        assertEquals(3, BackfillInfo.find.findList().size());
        assertBackfillInfo("MAPREDUCE", 1526800000050L);
        assertBackfillInfo("SPARK", 1526900000050L);

        // Stop the runner and do cleanup before exiting.
        runner.kill();
        runnerThread.interrupt();
        try {
          runnerThread.join();
        } catch (InterruptedException e) {
          // Ignore the exception.
        }
      }
    });
  }

  @Test
  public void testPrioritizeExecution() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        List<BackfillInfo> infos = BackfillInfo.find.findList();
        assertEquals(0, infos.size());

        // Start runner with 1 thread.
        ElephantContext.instance().getGeneralConf().setInt("drelephant.analysis.thread.count", 1);
        final ElephantRunner runner = spy(new ElephantRunner());
        Map<String, Priority> appsToPriorityMap = Collections.synchronizedMap(new LinkedHashMap<String, Priority>());
        // 3 jobs to be returned.
        List<AnalyticJob> jobs = new ArrayList<AnalyticJob>();
        // First job will sleep for 5 seconds before analysis, if not interrupted.
        jobs.add(((DummyAnalyticJobGenerator.DummyAnalyticJob) new DummyAnalyticJobGenerator.DummyAnalyticJob().
            setAppId("application_1526555215992_0001").setAppType(new ApplicationType("MAPREDUCE")).
            setName("mapreduce_job").setQueueName("default").setStartTime(1526520000000L).setFinishTime(1526520000050L).
            setUser("user")).setSleepBeforeAnalysis());
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0002").
            setAppType(new ApplicationType("SPARK")).setName("spark_job").setQueueName("default").
            setStartTime(1526600000000L).setFinishTime(1526600000050L).setUser("user"));
        jobs.add(new DummyAnalyticJobGenerator.DummyAnalyticJob().setAppId("application_1526555215992_0003").
            setAppType(new ApplicationType("SPARK")).setName("spark_job").setQueueName("default").
            setStartTime(1526700000000L).setFinishTime(1526700000050L).setUser("user"));
        doReturn(new DummyAnalyticJobGenerator(appsToPriorityMap, jobs)).when(runner).getAnalyticJobGenerator();
        Thread runnerThread = new Thread(new Runnable() {
          @Override
          public void run() {
            runner.run();
          }
        });
        runnerThread.start();

        // Wait till first app has been picked up by executor.
        int loopCount = 400;
        while (!runner.getAppToAnalyticJobMap().containsKey("application_1526555215992_0001") && loopCount > 0) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          loopCount--;
        }
        assertTrue("Executor should have picked up application_1526555215992_0001 for execution",
            runner.getAppToAnalyticJobMap().containsKey("application_1526555215992_0001"));

        // Prioritize application_1526555215992_0003.
        Future<?> future = runner.prioritizeExecutionAndReturnFuture("application_1526555215992_0003");
        assertNotNull("Priortization was not successful", future);
        // Ensure first job proceeds ahead from sleep by interrupting it.
        runner.getAppToAnalyticJobMap().get("application_1526555215992_0001").getJobFuture().cancel(true);
        runner.waitTillPrioritizedJobAnalysisFinishes(future);
        assertTrue("Prioritized job should have finished.", future.isDone());

        // Wait till results have been updated in DB.
        List<AppResult> results = AppResult.find.findList();
        int resultCount = results.size();
        loopCount = 400;
        while (resultCount < 3 && loopCount > 0) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          results = AppResult.find.findList();
          resultCount = results.size();
          loopCount--;
        }

        // Check yarn_app_result.
        assertEquals("yarn_app_result table should have 3 rows", 3, resultCount);
        assertAppResultEntry("application_1526555215992_0001", 1526520000000L, 1526520000050L,
            "mapreduce_job");
        assertAppResultEntry("application_1526555215992_0002", 1526600000000L, 1526600000050L,
            "spark_job");
        assertAppResultEntry("application_1526555215992_0003", 1526700000000L, 1526700000050L,
            "spark_job");

        // Check backfill_info.
        assertEquals("backfill_info table should have 2 rows", 2, BackfillInfo.find.findList().size());
        BackfillInfo mapReduceInfo = BackfillInfo.find.byId("MAPREDUCE");
        assertEquals(1526520000050L, mapReduceInfo.backfillTs);
        BackfillInfo sparkInfo = BackfillInfo.find.byId("SPARK");
        assertEquals(1526700000050L, sparkInfo.backfillTs);

        // Verify if apps were submitted in correct order based on prioritization.
        assertEquals(3, appsToPriorityMap.size());
        assertEquals(Lists.newArrayList("application_1526555215992_0001", "application_1526555215992_0003",
            "application_1526555215992_0002"), new ArrayList<String>(appsToPriorityMap.keySet()));
        assertEquals(Lists.newArrayList(Priority.NORMAL, Priority.HIGH, Priority.NORMAL),
            new ArrayList<Priority>(appsToPriorityMap.values()));

        // Stop the runner and do cleanup before exiting.
        runner.kill();
        runnerThread.interrupt();
        try {
          runnerThread.join();
        } catch (InterruptedException e) {
          // Ignore the exception.
        }
      }
    });
  }

  private static class DummyAnalyticJobGenerator implements AnalyticJobGenerator {
    private static Map<String, Priority> _appsToPriorityMap;
    private List<AnalyticJob> _analyticJobs;

    private DummyAnalyticJobGenerator(Map<String, Priority> appsMap, List<AnalyticJob> jobs) {
      _appsToPriorityMap = appsMap;
      _analyticJobs = jobs;
    }

    @Override
    public void configure(Configuration configuration) throws IOException {
    }

    @Override
    public void updateResourceManagerAddresses() {
    }

    @Override
    public List<AnalyticJob> fetchAnalyticJobs() throws IOException, AuthenticationException {
      return _analyticJobs;
    }

    @Override
    public void addIntoRetries(AnalyticJob job) {
    }

    @Override
    public void addIntoSecondRetryQueue(AnalyticJob job) {
    }

    @Override
    public String getEffectiveResourceManagerAddress() {
      return "localhost:8188";
    }

    @Override
    public long getFetchStartTime() {
      return 0;
    }

    private static class DummyAnalyticJob extends AnalyticJob {
      private long _finishTimeAfterAnalysis = -1;
      // For prioritization tests, sleep for 5 seconds before analysis.
      // Interruption is caught and ignored.
      private boolean _sleepBeforeAnalysis = false;
      @Override
      public AppResult getAnalysis() throws Exception {
        if (_sleepBeforeAnalysis) {
          try {
            Thread.sleep(5000L);
          } catch (InterruptedException e) {
            // Ignore and continue with analysis.
          }
        }
        AppResult result = new AppResult();
        result.id = getAppId();
        result.finishTime = getFinishTime() <= 0 ? _finishTimeAfterAnalysis : getFinishTime();
        result.jobName = getName();
        result.queueName = getQueueName();
        result.trackingUrl = getTrackingUrl() == null ? "" : getTrackingUrl();
        result.name = getName();
        result.username = getUser();
        result.startTime = getStartTime();
        List<JobType> jobTypes = ElephantContext.instance().getAppTypeToJobTypes().get(getAppType());
        result.jobType = ((jobTypes == null) ? "" :  jobTypes.get(0).getName());
        result.severity = Severity.LOW;
        result.score = 100;
        result.workflowDepth = 1;
        result.scheduler = "Azkaban";
        result.jobExecId = Utils.getJobIdFromApplicationId(getAppId());
        result.flowExecId = "flow_123";
        result.jobDefId = "job_def_123";
        result.flowDefId = "flow_def_123";
        result.jobExecUrl = "job_url";
        result.flowExecUrl = "flow_url";
        result.jobDefUrl = "job_def_url";
        result.flowDefUrl = "flow_def_url";
        // Wait for execution priority to be set, if not already set, to avoid race.
        int loopCount = 400;
        while (getJobExecutionPriority() == Priority.MIN_PRIORITY && loopCount > 0) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            // Ignore.
          }
          loopCount--;
        }
        _appsToPriorityMap.put(getAppId(), getJobExecutionPriority());
        return result;
      }

      DummyAnalyticJob setFinishTimeAfterAnalysis(long finishTime) {
        _finishTimeAfterAnalysis = finishTime;
        return this;
      }

      DummyAnalyticJob setSleepBeforeAnalysis() {
        _sleepBeforeAnalysis = true;
        return this;
      }
    }
  }

  private static class DummyElephantBackfillFetcher implements ElephantBackfillFetcher {
    private List<AnalyticJob> _backfillJobs;
    private long _backfillTs;
    private long _lowestTsFromRM;
    private volatile boolean _jobsFetchedFlag = false;
    private String _fetcherName;
    private DummyElephantBackfillFetcher(List<AnalyticJob> jobs, String fetcherName, long backfillTs, long lowestTsFromRM) {
      _backfillJobs = jobs;
      _backfillTs = backfillTs;
      _lowestTsFromRM = lowestTsFromRM;
      _fetcherName = fetcherName;
    }

    @Override
    public List<AnalyticJob> fetchJobsForBackfill(long startTime, long endTime) throws Exception {
      // Verify whether jobs were backfilled based on lowest ts of apps retrieved from RM and backfill ts in DB.
      if ((startTime == _backfillTs - ElephantRunner.BACKFILL_BUFFER_TIME) && (endTime == _lowestTsFromRM)) {
        _jobsFetchedFlag = true;
      }
      return _backfillJobs;
    }

    private boolean getJobsFetchedFlag() {
      return _jobsFetchedFlag;
    }

    private String getFetcherName() {
      return _fetcherName;
    }
  }
}
