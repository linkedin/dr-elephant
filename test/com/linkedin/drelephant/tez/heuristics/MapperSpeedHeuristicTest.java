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
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapperSpeedHeuristicTest extends TestCase {

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new MapperSpeedHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private static final long MB_IN_BYTES = FileUtils.ONE_MB;
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;
  private static final int NUMTASKS = 100;

  public void testCritical() throws IOException {
    long runtime = 120 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.CRITICAL, analyzeJob(runtime, 1 * speed_factor));
  }

  public void testSevere() throws IOException {
    long runtime = 120 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.SEVERE, analyzeJob(runtime, 4 * speed_factor));
  }

  public void testModerate() throws IOException {
    long runtime = 120 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.MODERATE, analyzeJob(runtime, 13 * speed_factor));
  }

  public void testLow() throws IOException {
    long runtime = 120 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.LOW, analyzeJob(runtime, 50 * speed_factor));
  }

  public void testNone() throws IOException {
    long runtime = 120 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.NONE, analyzeJob(runtime, 51 * speed_factor));
  }

  public void testShortTask() throws IOException {
    long runtime = 2 * MINUTE_IN_MS;
    long speed_factor = (runtime * MB_IN_BYTES) / 1000;
    assertEquals(Severity.NONE, analyzeJob(runtime, 1 * speed_factor));
  }

  private Severity analyzeJob(long runtimeMs, long readBytes) throws IOException {
    TezCounterData jobCounter = new TezCounterData();
    TezTaskData[] mappers = new TezTaskData[NUMTASKS + 1];

    TezCounterData counter = new TezCounterData();
    counter.set(TezCounterData.CounterName.HDFS_BYTES_READ, readBytes / 2);
    counter.set(TezCounterData.CounterName.S3A_BYTES_READ, readBytes / 2);

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new TezTaskData(counter, new long[] { runtimeMs, 0, 0 ,0, 0});
    }
    // Non-sampled task, which does not contain time and counter data
    mappers[i] = new TezTaskData("task-id-"+i, "task-attempt-id-"+i);

    TezApplicationData data = new TezApplicationData().setCounters(jobCounter).setMapTaskData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}