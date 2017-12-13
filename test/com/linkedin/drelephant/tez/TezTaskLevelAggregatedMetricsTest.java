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