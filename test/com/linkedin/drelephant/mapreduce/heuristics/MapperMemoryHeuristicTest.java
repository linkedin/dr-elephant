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

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;


public class MapperMemoryHeuristicTest extends TestCase {

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
  public void testDefaultContainerNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(256, 2048));
  }

  public void testDefaultContainerNoneMore() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1024, 2048));
  }

  private Severity analyzeJob(long taskAvgMemMB, long containerMemMB) throws IOException {
    MapReduceCounterData jobCounter = new MapReduceCounterData();
    MapReduceTaskData[] mappers = new MapReduceTaskData[NUMTASKS + 1];

    MapReduceCounterData counter = new MapReduceCounterData();
    counter.set(MapReduceCounterData.CounterName.PHYSICAL_MEMORY_BYTES, taskAvgMemMB* FileUtils.ONE_MB);

    Properties p = new Properties();
    p.setProperty(MapperMemoryHeuristic.MAPPER_MEMORY_CONF, Long.toString(containerMemMB));

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);
      mappers[i].setTimeAndCounter(new long[5], counter);
    }
    // Non-sampled task, which does not contain time and counter data
    mappers[i] = new MapReduceTaskData("task-id-"+i, "task-attempt-id-"+i);

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    data.setJobConf(p);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
