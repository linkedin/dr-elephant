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
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MapperMemoryHeuristicTest extends TestCase{

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new MapperMemoryHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private int NUMTASKS = 100;

  public void testLargeContainerSizeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(2048, 8192));
  }

  public void testLargeContainerSizeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(3072, 8192));
  }

  public void testLargeContainerSizeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(4096, 8192));
  }

  public void testLargeContainerSizeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(6144, 8192));
  }

  // If the task use default container size, it should not be flagged
  // Not using Default Container param, will calculate severity irrespective of default container size Chaning NONE -> CRITICAL
  public void testDefaultContainerNone() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(256, 2048));
  }

  // Not using Default Container param, will calculate severity irrespective of default container size Chaning NONE -> MODERATE
  public void testDefaultContainerNoneMore() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1024, 2048));
  }

  private Severity analyzeJob(long taskAvgMemMB, long containerMemMB) throws IOException {
    TezCounterData jobCounter = new TezCounterData();
    TezTaskData[] mappers = new TezTaskData[NUMTASKS + 1];

    TezCounterData counter = new TezCounterData();
    counter.set(TezCounterData.CounterName.PHYSICAL_MEMORY_BYTES, taskAvgMemMB* FileUtils.ONE_MB);

    Properties p = new Properties();
    p.setProperty(MapperMemoryHeuristic.MAPPER_MEMORY_CONF, Long.toString(containerMemMB));

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new TezTaskData("task-id-"+i, "task-attempt-id-"+i);
      mappers[i].setTime(new long[5]);
      mappers[i].setCounter(counter);
    }
    // Non-sampled task, which does not contain time and counter data
    mappers[i] = new TezTaskData("task-id-"+i, "task-attempt-id-"+i);

    TezApplicationData data = new TezApplicationData().setCounters(jobCounter).setMapTaskData(mappers);
    data.setConf(p);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}