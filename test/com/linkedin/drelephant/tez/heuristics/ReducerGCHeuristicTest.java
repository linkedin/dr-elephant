/*
 * Copyright 2017 Electronic Arts Inc.
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
 *
 */
package com.linkedin.drelephant.tez.heuristics;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerGCHeuristicTest extends TestCase{

  private static Map<String, String> paramsMap = new HashMap<String, String>();

  private static Heuristic _heuristic = new ReducerGCHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private static int NUMTASKS = 100;

  public void testGCCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(1000000, 50000, 2000));
  }

  public void testGCSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1000000, 50000, 1500));
  }

  public void testGCModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1000000, 50000, 1000));
  }

  public void testGCNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1000000, 50000, 300));
  }

  public void testShortTasksNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(100000, 50000, 2000));
  }


  private Severity analyzeJob(long runtimeMs, long cpuMs, long gcMs) throws IOException {
    TezCounterData jobCounter = new TezCounterData();
    TezTaskData[] reducers = new TezTaskData[NUMTASKS + 1];

    TezCounterData counter = new TezCounterData();
    counter.set(TezCounterData.CounterName.CPU_MILLISECONDS, cpuMs);
    counter.set(TezCounterData.CounterName.GC_TIME_MILLIS, gcMs);

    int i = 0;
    for (; i < NUMTASKS; i++) {
      reducers[i] = new TezTaskData("task-id-"+i, "task-attempt-id-"+i);
      reducers[i].setTimeAndCounter(new long[] { runtimeMs, 0, 0, 0, 0 }, counter);
    }
    // Non-sampled task, which does not contain time and counter data
    reducers[i] = new TezTaskData("task-id-"+i, "task-attempt-id-"+i);

    TezApplicationData data = new TezApplicationData().setCounters(jobCounter).setReduceTaskData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}