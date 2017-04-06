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


public class ShuffleSortHeuristicTest extends TestCase {

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new ShuffleSortHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private static final int NUMTASKS = 100;
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testLongShuffleCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(30 * MINUTE_IN_MS, 0, 5 * MINUTE_IN_MS));
  }

  public void testLongShuffleSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(30 * MINUTE_IN_MS, 0, 10 * MINUTE_IN_MS));
  }

  public void testLongShuffleModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(30 * MINUTE_IN_MS, 0, 20 * MINUTE_IN_MS));
  }

  public void testLongShuffleLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(30 * MINUTE_IN_MS, 0, 40 * MINUTE_IN_MS));
  }

  public void testLongShuffleNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(30 * MINUTE_IN_MS, 0, 80 * MINUTE_IN_MS));
  }

  public void testLongSortCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(0, 30 * MINUTE_IN_MS, 5 * MINUTE_IN_MS));
  }

  public void testLongSortSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(0, 30 * MINUTE_IN_MS, 10 * MINUTE_IN_MS));
  }

  public void testLongSortModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(0, 30 * MINUTE_IN_MS, 20 * MINUTE_IN_MS));
  }

  public void testLongSortLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(0, 30 * MINUTE_IN_MS, 40 * MINUTE_IN_MS));
  }

  public void testLongSortNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, 30 * MINUTE_IN_MS, 80 * MINUTE_IN_MS));
  }

  public void testShortShuffle() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(MINUTE_IN_MS / 2, 0, MINUTE_IN_MS / 2));
  }

  public void testShortSort() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, MINUTE_IN_MS / 2, MINUTE_IN_MS / 2));
  }

  private Severity analyzeJob(long shuffleTimeMs, long sortTimeMs, long reduceTimeMs) throws IOException {
    MapReduceCounterData dummyCounter = new MapReduceCounterData();
    MapReduceTaskData[] reducers = new MapReduceTaskData[NUMTASKS + 1];

    int i = 0;
    for (; i < NUMTASKS; i++) {
      reducers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);
      reducers[i].setTimeAndCounter(
          new long[] { shuffleTimeMs + sortTimeMs + reduceTimeMs, shuffleTimeMs, sortTimeMs, 0, 0}, dummyCounter);
    }
    // Non-sampled task, which does not contain time and counter data
    reducers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);
    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }

}
