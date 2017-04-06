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

package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskLevelAggregatedMetrics {

    @Test
    public void testZeroTasks() {
        MapReduceTaskData taskData[] = {};
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(taskData, 0, 0);
        Assert.assertEquals(taskMetrics.getDelay(), 0);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
    }

    @Test
    public void testNullTaskArray() {
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(null, 0, 0);
        Assert.assertEquals(taskMetrics.getDelay(), 0);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
    }

    @Test
    public void testTaskLevelData() {
        MapReduceTaskData taskData[] = new MapReduceTaskData[3];
        MapReduceCounterData counterData = new MapReduceCounterData();
        counterData.set(MapReduceCounterData.CounterName.PHYSICAL_MEMORY_BYTES, 655577088L);
        counterData.set(MapReduceCounterData.CounterName.VIRTUAL_MEMORY_BYTES, 3051589632L);
        long time[] = {0,0,0,1464218501117L, 1464218534148L};
        taskData[0] = new MapReduceTaskData("task", "id");
        taskData[0].setTimeAndCounter(time, counterData);
        taskData[1] = new MapReduceTaskData("task", "id");
        taskData[1].setTimeAndCounter(new long[5], counterData);
        // Non-sampled task, which does not contain time and counter data
        taskData[2] = new MapReduceTaskData("task", "id");
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(taskData, 4096L, 1463218501117L);
        Assert.assertEquals(taskMetrics.getDelay(), 1000000000L);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 135168L);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 66627L);
    }
}
