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

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;


public class MapperTimeHeuristicTest extends TestCase {

  private static final long DUMMY_INPUT_SIZE = 0;

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new MapperTimeHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  // Test batch 1: Large runtime. Heuristic is not affected by various number of tasks */

  public void testLongRuntimeTasksCritical() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(10, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksCriticalMore() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksSevere() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(10, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksSevereMore() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(1000, 60 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 2: Short runtime and various number of tasks

  public void testShortRuntimeTasksCritical() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksSevere() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(500, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(101, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(50, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksNone() throws IOException {
    // Small file with small number of tasks and short runtime. This should be the common case.
    assertEquals(Severity.NONE, analyzeJob(5, 1 * Statistics.MINUTE_IN_MS));
  }

  private Severity analyzeJob(int numTasks, long runtime) throws IOException {
    MapReduceCounterData jobCounter = new MapReduceCounterData();
    MapReduceTaskData[] mappers = new MapReduceTaskData[numTasks + 1];

    MapReduceCounterData taskCounter = new MapReduceCounterData();
    taskCounter.set(MapReduceCounterData.CounterName.HDFS_BYTES_READ, DUMMY_INPUT_SIZE / 4);
    taskCounter.set(MapReduceCounterData.CounterName.S3_BYTES_READ, DUMMY_INPUT_SIZE / 4);
    taskCounter.set(MapReduceCounterData.CounterName.S3A_BYTES_READ, DUMMY_INPUT_SIZE / 4);
    taskCounter.set(MapReduceCounterData.CounterName.S3N_BYTES_READ, DUMMY_INPUT_SIZE / 4);

    int i = 0;
    for (; i < numTasks; i++) {
      mappers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);
      mappers[i].setTimeAndCounter(new long[] { runtime, 0, 0, 0, 0 }, taskCounter);
    }
    // Non-sampled task, which does not contain time and counter data
    mappers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
