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

package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.fetchers.MapReduceFetcherHadoop2;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic;
import common.TestUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import mockit.Expectations;
import mockit.Mocked;
import models.AppResult;
import org.junit.Ignore;
import org.junit.Test;

import static common.TestConstants.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Test aims to exercise {@code getAnalysis()} method in {@code AnalyticJob}.<br>
 * Dependencies to {@code ElephantContext, ElephantFetcher and Heuristics} are mocked
 * out with JMockit.
 */
@Ignore
public class AnalyticJobTest {
  @Mocked(stubOutClassInitialization = true)
  ElephantContext elephantContext = null;
  @Mocked
  MapReduceFetcherHadoop2 fetcher;

  @Test
  public void testGetAnalysis()
      throws Exception {
    try {
      // Setup analytic job
      final AnalyticJob analyticJob = new AnalyticJob().
          setAppId(TEST_JOB_ID1).setAppType(new ApplicationType(TEST_APP_TYPE)).
          setFinishTime(1462178403).setStartTime(1462178412).setName(TEST_JOB_NAME).
          setQueueName(TEST_DEFAULT_QUEUE_NAME).setUser(TEST_USERNAME).setTrackingUrl(TEST_TRACKING_URL);

      // Setup job counter data
      String filePath = FILENAME_JOBCOUNTER;
      MapReduceCounterData jobCounter = new MapReduceCounterData();
      setCounterData(jobCounter, filePath);

      // Setup mapper data
      long[][] mapperTasksTime = {{2563, 0, 0, 0, 0}, {2562, 0, 0, 0, 0}, {2567, 0, 0, 0, 0}};
      MapReduceTaskData[] mappers = new MapReduceTaskData[3];
      for (int i = 1; i <= mappers.length; i++) {
        MapReduceCounterData taskCounter = new MapReduceCounterData();
        setCounterData(taskCounter, FILENAME_MAPPERTASK.replaceFirst("\\$", Integer.toString(i)));
        mappers[i - 1 ] = new MapReduceTaskData("task-id-"+(i-1), "task-attempt-id-"+(i-1));
        mappers[i - 1].setTimeAndCounter(mapperTasksTime[i - 1], taskCounter);
      }

      // Setup reducer data
      long[][] reducerTasksTime = {{1870, 1665, 14, 0, 0}};
      MapReduceTaskData[] reducers = new MapReduceTaskData[1];
      for (int i = 1; i <= reducers.length; i++) {
        MapReduceCounterData taskCounter = new MapReduceCounterData();
        setCounterData(taskCounter, FILENAME_REDUCERTASK.replaceFirst("\\$", Integer.toString(i)));
        reducers[i - 1] = new MapReduceTaskData("task-id-"+(i-1), "task-attempt-id-"+(i-1));
        reducers[i - 1].setTimeAndCounter(reducerTasksTime[i - 1], taskCounter);
      }

      // Setup job configuration data
      filePath = FILENAME_JOBCONF;
      Properties jobConf = TestUtil.loadProperties(filePath);

      // Setup application data
      final MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).
          setMapperData(mappers).setReducerData(reducers).setJobConf(jobConf).setSucceeded(true).
          setDiagnosticInfo("").setUsername(TEST_USERNAME).setUrl("").setJobName(TEST_JOB_NAME).
          setStartTime(1462178412).setFinishTime(1462178403).setRetry(false).setAppId(TEST_JOB_ID1);

      // Setup heuristics
      final List<Heuristic> heuristics = loadHeuristics();

      // Setup job type
      final JobType jobType = new JobType(TEST_JOB_TYPE, TEST_JOBCONF_NAME, TEST_JOBCONF_PATTERN);

      // Set expectations in JMockit
      new Expectations() {{
        fetcher.fetchData(analyticJob);
        result = data;

        elephantContext.getHeuristicsForApplicationType(analyticJob.getAppType());
        result = heuristics;

        elephantContext.matchJobType(data);
        result = jobType;
      }};

      // Call the method under test
      AppResult result = analyticJob.getAnalysis();

      // Make assertions on result
      assertTrue("Result is null", result != null);
      assertTrue("Score did not match", result.score == TEST_SCORE);
      assertTrue("Severity did not match", result.severity.toString().equals(TEST_SEVERITY));
      assertTrue("APP ID did not match", result.id.equals(TEST_JOB_ID1));
      assertTrue("Scheduler did not match", result.scheduler.equals(TEST_SCHEDULER));
    } catch (Exception e) {
      e.printStackTrace();
      assertFalse("Test failed with exception", true);
    }
  }

  private void setCounterData(MapReduceCounterData counter, String filePath)
      throws IOException {
    Properties counterData = TestUtil.loadProperties(filePath);

    for (Object groupName : counterData.keySet()) {
      String counterValueString = (String) counterData.get(groupName);
      counterValueString = counterValueString.replaceAll("\\{|\\}", "");

      StringBuilder stringBuilder = new StringBuilder();

      for (String counterKeyValue : counterValueString.split(",")) {
        stringBuilder.append(counterKeyValue.trim()).append('\n');
      }
      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringBuilder.toString().getBytes(DEFAULT_ENCODING));
      Properties counterProperties = new Properties();
      counterProperties.load(inputStream);

      for (Object counterKey : counterProperties.keySet()) {
        long counterValue = Long.parseLong(counterProperties.get(counterKey).toString());
        counter.set(groupName.toString(), counterKey.toString(), counterValue);
      }
    }
  }

  private List<Heuristic> loadHeuristics() {
    List<Heuristic> heuristics = new ArrayList<Heuristic>();
    // dummy hash map
    Map<String, String> paramsMap = new HashMap<String, String>();
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Mapper Skew",
        "com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic",
        "views.html.help.mapreduce.helpMapperSkew", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(
        new HeuristicConfigurationData("Mapper GC", "com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic",
            "views.html.help.mapreduce.helpGC", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Mapper Time",
        "com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic", "views.html.help.mapreduce.helpMapperTime",
        new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Mapper Speed",
        "com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic",
        "views.html.help.mapreduce.helpMapperSpeed", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Mapper Spill",
        "com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic",
        "views.html.help.mapreduce.helpMapperSpill", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Mapper Memory",
        "com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic",
        "views.html.help.mapreduce.helpMapperMemory", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Reducer Skew",
        "com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic",
        "views.html.help.mapreduce.helpReducerSkew", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(
        new HeuristicConfigurationData("Reducer GC", "com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic",
            "views.html.help.mapreduce.helpGC", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Reducer Time",
        "com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic",
        "views.html.help.mapreduce.helpReducerTime", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Reducer Memory",
        "com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic",
        "views.html.help.mapreduce.helpReducerMemory", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(new HeuristicConfigurationData("Shuffle &#38; Sort",
        "com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic",
        "views.html.help.mapreduce.helpShuffleSort", new ApplicationType("mapreduce"), paramsMap)));
    heuristics.add(new MapperSkewHeuristic(
        new HeuristicConfigurationData("Exception", "com.linkedin.drelephant.mapreduce.heuristics.ExceptionHeuristic",
            "views.html.help.mapreduce.helpException", new ApplicationType("mapreduce"), paramsMap)));

    return heuristics;
  }
}
