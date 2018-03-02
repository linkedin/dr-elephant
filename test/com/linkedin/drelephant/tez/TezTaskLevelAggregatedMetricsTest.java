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
package com.linkedin.drelephant.tez;


import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import org.junit.Assert;
import org.junit.Test;

public class TezTaskLevelAggregatedMetricsTest {

  @Test
  public void testZeroTasks() {
    TezTaskData taskData[] = {};
    TezTaskLevelAggregatedMetrics taskMetrics = new TezTaskLevelAggregatedMetrics(taskData, 0, 0);
    Assert.assertEquals(taskMetrics.getDelay(), 0);
    Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
    Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
  }

  @Test
  public void testNullTaskArray() {
    TezTaskLevelAggregatedMetrics taskMetrics = new TezTaskLevelAggregatedMetrics(null, 0, 0);
    Assert.assertEquals(taskMetrics.getDelay(), 0);
    Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
    Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
  }

  @Test
  public void testTaskLevelData() {
    TezTaskData taskData[] = new TezTaskData[3];
    TezCounterData counterData = new TezCounterData();
    counterData.set(TezCounterData.CounterName.PHYSICAL_MEMORY_BYTES, 655577088L);
    counterData.set(TezCounterData.CounterName.VIRTUAL_MEMORY_BYTES, 3051589632L);
    long time[] = {0,0,0,1464218501117L, 1464218534148L};
    taskData[0] = new TezTaskData("task", "id");
    taskData[0].setTimeAndCounter(time,counterData);
    taskData[1] = new TezTaskData("task", "id");
    taskData[1].setTimeAndCounter(new long[5],counterData);
    // Non-sampled task, which does not contain time and counter data
    taskData[2] = new TezTaskData("task", "id");
    TezTaskLevelAggregatedMetrics taskMetrics = new TezTaskLevelAggregatedMetrics(taskData, 4096L, 1463218501117L);
    Assert.assertEquals(taskMetrics.getDelay(), 1000000000L);
    Assert.assertEquals(taskMetrics.getResourceUsed(), 135168L);
    Assert.assertEquals(taskMetrics.getResourceWasted(), 66627L);
  }
}