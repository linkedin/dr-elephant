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

package com.linkedin.drelephant.tuning.hbt;

import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppResult;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;
import static com.linkedin.drelephant.tuning.Constant.*;
import static com.linkedin.drelephant.tuning.Constant.HeuristicsToTuneByPigHbt.*;
import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class PigHbtParameterRecommenderTest {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private static FakeApplication fakeApp;

  private final double delta = 0.000001;

  private void populateTestData() {
    try {
      initPigHBT();
    } catch (IOException ioEx) {
      logger.error("IOException encountered while populating test data", ioEx);
    } catch (SQLException sqlEx) {
      logger.error("SqlException encountered while populating test data", sqlEx);
    }
  }

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<>();
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

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private List<AppResult> getAppResults(String jobExecutionId, String flowExecutionId) {
    List<AppResult> appResults = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecutionId)
        .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecutionId)
        .findList();
    return appResults;
  }

  @Test
  public void testloadLatestAppliedParameters() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList;
        appResultsList = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);

        PigHbtParameterRecommender pigHbtParameterRecommender = new PigHbtParameterRecommender(appResultsList);
        pigHbtParameterRecommender.loadLatestAppliedParameters();
        Map<String, Double> latestAppliedParams = pigHbtParameterRecommender.getLatestAppliedParams();
        assertEquals("Number of last Applied Parameters", latestAppliedParams.size(), 8);
        assertEquals("Mapper Heap Memory should be 1536m", latestAppliedParams.
            get(MAPPER_HEAP_HADOOP_CONF.getValue()).intValue(), 1536);
        assertEquals("Reducer Memory should be 4gb", latestAppliedParams.
            get(REDUCER_MEMORY_HADOOP_CONF.getValue()).intValue(), 4096);
        assertEquals("Mapper Memory should be 5gb", latestAppliedParams.
            get(MAPPER_MEMORY_HADOOP_CONF.getValue()).intValue(), 6120);
        assertEquals("Split size should be 536870912", latestAppliedParams
            .get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()).intValue(), 536870912);
      }
    });
  }

  @Test (expected = IllegalArgumentException.class)
  public void testloadLatestAppliedParametersWhenNoAppResultsAvailable() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList;
        appResultsList = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID2);

        PigHbtParameterRecommender pigHbtParameterRecommender = new PigHbtParameterRecommender(appResultsList);

        assertEquals(appResultsList.size(), 0);
        pigHbtParameterRecommender.loadLatestAppliedParameters();
      }
    });
  }

  @Test
  public void testGetHeapMemory() {
    PigHbtParameterRecommender pigHbtParameterRecommender = new PigHbtParameterRecommender(null);
    assertEquals("Extracting JVM heap memory ",
        pigHbtParameterRecommender.getHeapMemory("-XX:ReservedCodeCacheSize=100M -Xmx2070m"), 2070,delta);
    assertEquals("Extracting JVM heap memory in gb",
        pigHbtParameterRecommender.getHeapMemory("-Xmx2g -XX:ReservedCodeCacheSize=1G"), 2048, delta);
    assertEquals("Extracting JVM heap memory in GB",
        pigHbtParameterRecommender.getHeapMemory("-Xmx100G"), 102400, delta);
    assertEquals("Extracting JVM heap memory when heap memory flag not available",
        pigHbtParameterRecommender.getHeapMemory("-Xms1G"), DEFAULT_CONTAINER_HEAP_MEMORY, delta);
    logger.debug("{}", "");
  }

  @Test
  public void testGetFailedHeuristics() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        List<String> failedHeuristics = pigHbtParameterRecommender_1.getFailedHeuristics();
        assertEquals("Number of failed heuristics should be four", failedHeuristics.size(), 5);
        assertTrue("Should contain Mapper Time as failed heuristic", failedHeuristics.contains(MAPPER_TIME.getValue()));
        assertTrue("Should contain Mapper Memory as failed heuristic", failedHeuristics.contains(MAPPER_MEMORY.getValue()));
        assertTrue("Should contain Mapper Spill as failed heuristic", failedHeuristics.contains(MAPPER_SPILL.getValue()));
        assertTrue("Should contain Reducer Memory as failed heuristic", failedHeuristics.contains(REDUCER_MEMORY.getValue()));
        assertTrue("Should contain Reducer Time as failed heuristic", failedHeuristics.contains(REDUCER_TIME.getValue()));
        assertFalse("Should not contain Mapper GC as failed heuristic", failedHeuristics.contains("Mapper GC"));

        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID2, TEST_PIG_HBT_FLOW_EXEC_ID2);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        assertEquals("Number of failed heuristics should be zero", pigHbtParameterRecommender_2
            .getFailedHeuristics().size(), 0);
      }
    });
  }

  @Test
  public void testSuggestionForMapperSpill() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2, appResultsList_3;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        pigHbtParameterRecommender_1.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_1.suggestParametersForMemorySpill();
        assertEquals("Split percent should be 0.80", pigHbtParameterRecommender_1.getLatestAppliedParams()
            .get(SORT_SPILL_HADOOP_CONF.getValue()), 0.80, delta);
        assertEquals("Sort buffer size should be 100", pigHbtParameterRecommender_1.getLatestAppliedParams()
            .get(SORT_BUFFER_HADOOP_CONF.getValue()).intValue(), 1400);
        assertTrue(pigHbtParameterRecommender_1.getJobSuggestedParameters().containsKey(SORT_BUFFER_HADOOP_CONF.getValue()));
        assertTrue(pigHbtParameterRecommender_1.getJobSuggestedParameters().containsKey(SORT_SPILL_HADOOP_CONF.getValue()));
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters().get(SORT_SPILL_HADOOP_CONF.getValue()), 0.85, delta);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters().get(SORT_BUFFER_HADOOP_CONF.getValue()), 1680, delta);

        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID2, TEST_PIG_HBT_FLOW_EXEC_ID2);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        pigHbtParameterRecommender_2.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_2.suggestParametersForMemorySpill();
        assertEquals("Split percent should be 0.86", pigHbtParameterRecommender_2.getLatestAppliedParams()
            .get(SORT_SPILL_HADOOP_CONF.getValue()), 0.86, delta);
        assertEquals("Sort buffer size should be ", pigHbtParameterRecommender_2.getLatestAppliedParams()
            .get(SORT_BUFFER_HADOOP_CONF.getValue()).intValue(), 100);
        assertTrue(pigHbtParameterRecommender_2.getJobSuggestedParameters().containsKey(SORT_BUFFER_HADOOP_CONF.getValue()));
        assertFalse(pigHbtParameterRecommender_2.getJobSuggestedParameters().containsKey(SORT_SPILL_HADOOP_CONF.getValue()));
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(SORT_BUFFER_HADOOP_CONF.getValue()), 130, delta);

        appResultsList_3 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID3, TEST_PIG_HBT_FLOW_EXEC_ID3);
        PigHbtParameterRecommender pigHbtParameterRecommender_3 = new PigHbtParameterRecommender(appResultsList_3);
        pigHbtParameterRecommender_3.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_3.suggestParametersForMemorySpill();
        assertEquals("Split percent should be 0.78", pigHbtParameterRecommender_3.getLatestAppliedParams()
            .get(SORT_SPILL_HADOOP_CONF.getValue()), 0.78, delta);
        assertEquals("Sort buffer size should be ", pigHbtParameterRecommender_3.getLatestAppliedParams()
            .get(SORT_BUFFER_HADOOP_CONF.getValue()).intValue(), 205);
        assertFalse(pigHbtParameterRecommender_3.getJobSuggestedParameters().containsKey(
            SORT_BUFFER_HADOOP_CONF.getValue()));
        assertFalse(pigHbtParameterRecommender_3.getJobSuggestedParameters().containsKey(
            SORT_SPILL_HADOOP_CONF.getValue()));
      }
    });
  }

  @Test
  public void testSuggestionForSplitSize() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        pigHbtParameterRecommender_1.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_1.suggestSplitSize();
        assertEquals(pigHbtParameterRecommender_1.getLatestAppliedParams().get(MAPPER_HEAP_HADOOP_CONF.getValue()), 1536, delta);
        assertTrue("Suggested parameter should contain MR split size suggestion",
            pigHbtParameterRecommender_1.getJobSuggestedParameters().containsKey(SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertTrue("Suggested parameter should contain pig max split size suggestion",
            pigHbtParameterRecommender_1.getJobSuggestedParameters().containsKey(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()).intValue(), 1536 * FileUtils.ONE_MB);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(SPLIT_SIZE_HADOOP_CONF.getValue()).longValue(), 1536 * FileUtils.ONE_MB);

        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID2, TEST_PIG_HBT_FLOW_EXEC_ID2);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        pigHbtParameterRecommender_2.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_2.suggestSplitSize();
        assertEquals(pigHbtParameterRecommender_2.getLatestAppliedParams().get(MAPPER_HEAP_HADOOP_CONF.getValue()).intValue(),
            2500);
        assertTrue("Suggested parameter should contain MR split size suggestion",
            pigHbtParameterRecommender_2.getJobSuggestedParameters().containsKey(SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertTrue("Suggested parameter should contain pig max split size suggestion",
            pigHbtParameterRecommender_2.getJobSuggestedParameters().containsKey(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()).longValue(), 1645447006L);
      }
    });
  }

  @Test
  public void testSuggestionForMapperMemory() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        pigHbtParameterRecommender_1.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_1.suggestMemoryParam(CommonConstantsHeuristic.MRJobTaskType.MAP);
        assertTrue("Latest execution parameters should contain Mapper Memory",
            pigHbtParameterRecommender_1.getLatestAppliedParams().containsKey(MAPPER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Latest execution parameters should contain Mapper Heap Memory",
            pigHbtParameterRecommender_1.getLatestAppliedParams().containsKey(MAPPER_HEAP_HADOOP_CONF.getValue()));
        assertEquals("Last execution's Mapper Memory should be 6GB",
            pigHbtParameterRecommender_1.getLatestAppliedParams().get(MAPPER_MEMORY_HADOOP_CONF.getValue()).intValue(), 6120);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(MAPPER_MEMORY_HADOOP_CONF.getValue()).longValue(), 2048);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(MAPPER_HEAP_HADOOP_CONF.getValue()).longValue(), 916);

        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID2, TEST_PIG_HBT_FLOW_EXEC_ID2);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        pigHbtParameterRecommender_2.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_2.suggestMemoryParam(CommonConstantsHeuristic.MRJobTaskType.MAP);
        assertTrue("Latest execution parameters should contain Mapper Memory",
            pigHbtParameterRecommender_2.getLatestAppliedParams().containsKey(MAPPER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Latest execution parameters should contain Mapper Heap Memory",
            pigHbtParameterRecommender_1.getLatestAppliedParams().containsKey(MAPPER_HEAP_HADOOP_CONF.getValue()));
        assertEquals("Last execution's Mapper Memory should be 4GB",
            pigHbtParameterRecommender_2.getLatestAppliedParams().get(MAPPER_MEMORY_HADOOP_CONF.getValue()).intValue(), 4096);
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(MAPPER_MEMORY_HADOOP_CONF.getValue()).longValue(), 4096);
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(MAPPER_HEAP_HADOOP_CONF.getValue()).longValue(), 3072);
      }
    });
  }

  @Test
  public void testSuggestionForReducerMemory() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        pigHbtParameterRecommender_1.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_1.suggestMemoryParam(CommonConstantsHeuristic.MRJobTaskType.REDUCE);
        assertTrue("Latest execution parameters should contain Reducer Memory",
            pigHbtParameterRecommender_1.getLatestAppliedParams().containsKey(REDUCER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Latest execution parameters should contain Reducer Heap Memory",
            pigHbtParameterRecommender_1.getLatestAppliedParams().containsKey(REDUCER_HEAP_HADOOP_CONF.getValue()));
        assertEquals("Last execution's Reducer Memory should be 4GB",
            pigHbtParameterRecommender_1.getLatestAppliedParams().get(REDUCER_MEMORY_HADOOP_CONF.getValue()).intValue(), 4096);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(REDUCER_MEMORY_HADOOP_CONF.getValue()).longValue(), 2048);
        assertEquals(pigHbtParameterRecommender_1.getJobSuggestedParameters()
            .get(REDUCER_HEAP_HADOOP_CONF.getValue()).longValue(), 600);

        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID2, TEST_PIG_HBT_FLOW_EXEC_ID2);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        pigHbtParameterRecommender_2.loadLatestAppliedParametersAndMaxParamValue();
        pigHbtParameterRecommender_2.suggestMemoryParam(CommonConstantsHeuristic.MRJobTaskType.REDUCE);
        assertTrue("Latest execution parameters should contain Reducer Memory",
            pigHbtParameterRecommender_2.getLatestAppliedParams().containsKey(REDUCER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Latest execution parameters should contain Reducer Heap Memory",
            pigHbtParameterRecommender_2.getLatestAppliedParams().containsKey(REDUCER_HEAP_HADOOP_CONF.getValue()));
        assertEquals("Last execution's Reducer Memory should be 3GB",
            pigHbtParameterRecommender_2.getLatestAppliedParams().get(REDUCER_MEMORY_HADOOP_CONF.getValue()).intValue(), 3072);
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(REDUCER_MEMORY_HADOOP_CONF.getValue()).longValue(), 2048);
        assertEquals(pigHbtParameterRecommender_2.getJobSuggestedParameters()
            .get(REDUCER_HEAP_HADOOP_CONF.getValue()).longValue(), 1522);
      }
    });
  }

  @Test
  public void testParamSuggestion() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        List<AppResult> appResultsList_1, appResultsList_2;
        appResultsList_1 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID1, TEST_PIG_HBT_FLOW_EXEC_ID1);
        PigHbtParameterRecommender pigHbtParameterRecommender_1 = new PigHbtParameterRecommender(appResultsList_1);
        Map<String, Double> suggestedParameters = pigHbtParameterRecommender_1.suggestParameters();
        assertEquals("Number of failed heuristics should be four", pigHbtParameterRecommender_1.getFailedHeuristics().size(), 5);
        assertTrue("Reducer memory must be suggested",
            suggestedParameters.containsKey(REDUCER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Mapper memory must be suggested",
            suggestedParameters.containsKey(MAPPER_MEMORY_HADOOP_CONF.getValue()));
        assertTrue("Mapper Heap Memory must be suggested",
            suggestedParameters.containsKey(MAPPER_HEAP_HADOOP_CONF.getValue()));
        assertTrue("Reducer Heap Memory must be suggested",
            suggestedParameters.containsKey(REDUCER_HEAP_HADOOP_CONF.getValue()));
        assertTrue("Pig Max split size must be suggested",
            suggestedParameters.containsKey(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertTrue("MapReduce max split size must be suggested",
            suggestedParameters.containsKey(SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertTrue("Sort spill percentage must be suggested",
            suggestedParameters.containsKey(SORT_SPILL_HADOOP_CONF.getValue()));
        assertTrue("Sort buffer size must be suggested",
            suggestedParameters.containsKey(SORT_BUFFER_HADOOP_CONF.getValue()));

        assertEquals(suggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue()).longValue(), 3072);
        assertEquals(suggestedParameters.get(MAPPER_HEAP_HADOOP_CONF.getValue()).longValue(), 916);

        assertEquals(suggestedParameters.get(REDUCER_MEMORY_HADOOP_CONF.getValue()).longValue(), 2048);
        assertEquals(suggestedParameters.get(REDUCER_HEAP_HADOOP_CONF.getValue()).longValue(), 600);

        assertEquals(suggestedParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()).intValue(), 1536 * FileUtils.ONE_MB);
        assertEquals(suggestedParameters.get(SPLIT_SIZE_HADOOP_CONF.getValue()).longValue(), 1536 * FileUtils.ONE_MB);

        assertEquals(suggestedParameters.get(SORT_SPILL_HADOOP_CONF.getValue()), 0.85, delta);
        assertEquals(suggestedParameters.get(SORT_BUFFER_HADOOP_CONF.getValue()), 1680, delta);


        appResultsList_2 = getAppResults(TEST_PIG_HBT_JOB_EXEC_ID3, TEST_PIG_HBT_FLOW_EXEC_ID3);
        PigHbtParameterRecommender pigHbtParameterRecommender_2 = new PigHbtParameterRecommender(appResultsList_2);
        suggestedParameters = pigHbtParameterRecommender_2.suggestParameters();
        Map<String, Double> latestExecutionParameters = pigHbtParameterRecommender_2.getLatestAppliedParams();
        assertEquals(latestExecutionParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue()).intValue(), 2048);
        assertEquals(latestExecutionParameters.get(REDUCER_MEMORY_HADOOP_CONF.getValue()).intValue(), 3072);
        assertEquals(latestExecutionParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()).intValue(), 536870912);
        assertEquals(latestExecutionParameters.get(SORT_BUFFER_HADOOP_CONF.getValue()).intValue(), 205);
        assertEquals(latestExecutionParameters.get(SORT_SPILL_HADOOP_CONF.getValue()), 0.78, delta);
        assertEquals(latestExecutionParameters.get(MAPPER_HEAP_HADOOP_CONF.getValue()).intValue(), 1436);
        assertEquals(latestExecutionParameters.get(REDUCER_HEAP_HADOOP_CONF.getValue()).intValue(), 1436);

        assertEquals(suggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue()).intValue(), 1024);
        assertEquals(suggestedParameters.get(MAPPER_HEAP_HADOOP_CONF.getValue()).longValue(), 768);
        assertEquals(suggestedParameters.get(REDUCER_MEMORY_HADOOP_CONF.getValue()).intValue(), 2048);
        assertEquals(suggestedParameters.get(REDUCER_HEAP_HADOOP_CONF.getValue()).longValue(), 619);

        assertEquals(suggestedParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()),
            latestExecutionParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()));
        assertEquals(suggestedParameters.get(SORT_SPILL_HADOOP_CONF.getValue()),
            latestExecutionParameters.get(SORT_SPILL_HADOOP_CONF.getValue()));
        assertEquals(suggestedParameters.get(SORT_BUFFER_HADOOP_CONF.getValue()),
            latestExecutionParameters.get(SORT_BUFFER_HADOOP_CONF.getValue()));
        assertEquals(suggestedParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()),
            latestExecutionParameters.get(PIG_SPLIT_SIZE_HADOOP_CONF.getValue()));
      }
    });
  }
}