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
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import java.io.IOException;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;


public class ReducerTimeHeuristicTest extends TestCase {

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new ReducerTimeHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testShortRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(1 * MINUTE_IN_MS, 1000));
  }

  public void testShortRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1 * MINUTE_IN_MS, 500));
  }

  public void testShortRunetimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1 * MINUTE_IN_MS, 101));
  }

  public void testShortRunetimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1 * MINUTE_IN_MS, 50));
  }

  public void testShortRunetimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1 * MINUTE_IN_MS, 2));
  }

  public void testLongRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 10));
  }

  // Long runtime severity is not affected by number of tasks
  public void testLongRunetimeCriticalMore() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 1000));
  }

  public void testLongRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(60 * MINUTE_IN_MS, 10));
  }

  public void testLongRunetimeSevereMore() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(60 * MINUTE_IN_MS, 1000));
  }

  private Severity analyzeJob(long runtimeMs, int numTasks) throws IOException {
    MapReduceCounterData dummyCounter = new MapReduceCounterData();
    MapReduceTaskData[] reducers = new MapReduceTaskData[numTasks + 1];

    int i = 0;
    for (; i < numTasks; i++) {
      reducers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);
      reducers[i].setTimeAndCounter(new long[] { runtimeMs, 0, 0, 0, 0 }, dummyCounter);
    }
    // Non-sampled task, which does not contain time and counter data
    reducers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
