package com.linkedin.drelephant.tony.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.drelephant.tony.data.TonyTaskData;
import com.linkedin.tony.Constants;
import com.linkedin.tony.events.Metric;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Test;


public class TonyUtilsTest {
  /**
   * Worker 0 is missing metrics, but worker 1 has metrics; we should use worker 1's
   * max memory metrics.
   */
  @Test
  public void testGetMaxMemorySomeTasksMissingMetrics() {
    Map<Integer, TonyTaskData> taskDataMap = new TreeMap<>();
    TonyTaskData worker0Data = new TonyTaskData("worker", 0);
    TonyTaskData worker1Data = new TonyTaskData("worker", 1);
    double worker1MaxMemoryBytes = 123d;
    worker1Data.setMetrics(ImmutableList.of(new Metric(Constants.MAX_MEMORY_BYTES, worker1MaxMemoryBytes)));

    taskDataMap.put(0, worker0Data);
    taskDataMap.put(1, worker1Data);

    Assert.assertEquals(worker1MaxMemoryBytes,
        TonyUtils.getMaxMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.MAX_MEMORY_BYTES), 0);
  }

  @Test
  public void testGetMaxMetricForTaskTypeAndMetricNameMissingTask() {
    Map<Integer, TonyTaskData> taskDataMap = new TreeMap<>();
    TonyTaskData worker0Data = new TonyTaskData("worker", 0);

    taskDataMap.put(0, worker0Data);

    Assert.assertEquals(-1.0d,
        TonyUtils.getMaxMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "ps",
        Constants.MAX_MEMORY_BYTES), 0);
  }

  @Test
  public void testGetMaxMetricForTaskTypeAndMetricName() {
    Map<Integer, TonyTaskData> taskDataMap = new TreeMap<>();
    TonyTaskData worker0Data = new TonyTaskData("worker", 0);
    TonyTaskData worker1Data = new TonyTaskData("worker", 1);

    double worker0MaxGPUUtilization = 20.0d;
    double worker1MaxGPUUtilization = 21.0d;
    double worker0MaxGPUFBMemoryUsage = 22.0d;
    double worker1MaxGPUFBMemoryUsage = 23.0d;
    double worker0MaxGPUMainMemoryUsage = 2.0d;
    double worker1MaxGPUMainMemoryUsage = -1.0d;

    worker0Data.setMetrics(ImmutableList.of(
        new Metric(Constants.MAX_GPU_UTILIZATION, worker0MaxGPUUtilization),
        new Metric(Constants.MAX_GPU_FB_MEMORY_USAGE, worker0MaxGPUFBMemoryUsage),
        new Metric(Constants.MAX_GPU_MAIN_MEMORY_USAGE, worker0MaxGPUMainMemoryUsage)
    ));
    worker1Data.setMetrics(ImmutableList.of(
        new Metric(Constants.MAX_GPU_UTILIZATION, worker1MaxGPUUtilization),
        new Metric(Constants.MAX_GPU_FB_MEMORY_USAGE, worker1MaxGPUFBMemoryUsage),
        new Metric(Constants.MAX_GPU_MAIN_MEMORY_USAGE, worker1MaxGPUMainMemoryUsage)
    ));

    taskDataMap.put(0, worker0Data);
    taskDataMap.put(1, worker1Data);

    Assert.assertEquals(worker1MaxGPUUtilization,
        TonyUtils.getMaxMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.MAX_GPU_UTILIZATION), 0);
    Assert.assertEquals(worker1MaxGPUFBMemoryUsage,
        TonyUtils.getMaxMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.MAX_GPU_FB_MEMORY_USAGE), 0);
    Assert.assertEquals(worker0MaxGPUMainMemoryUsage,
        TonyUtils.getMaxMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.MAX_GPU_MAIN_MEMORY_USAGE), 0);
  }

  @Test
  public void testGetAvgMetricForTaskTypeAndMetricName() {
    Map<Integer, TonyTaskData> taskDataMap = new TreeMap<>();
    TonyTaskData worker0Data = new TonyTaskData("worker", 0);
    TonyTaskData worker1Data = new TonyTaskData("worker", 1);

    double worker0AvgGPUUtilization = 10.0d;
    double worker1AvgGPUUtilization = 20.0d;
    double worker0AvgGPUFBMemoryUsage = 30.0d;
    double worker1AvgGPUFBMemoryUsage = 0.0d;
    double worker0AvgGPUMainMemoryUsage = 40.0d;
    double worker1AvgGPUMainMemoryUsage = -1.0d;

    worker0Data.setMetrics(ImmutableList.of(
        new Metric(Constants.AVG_GPU_UTILIZATION, worker0AvgGPUUtilization),
        new Metric(Constants.AVG_GPU_FB_MEMORY_USAGE, worker0AvgGPUFBMemoryUsage),
        new Metric(Constants.AVG_GPU_MAIN_MEMORY_USAGE, worker0AvgGPUMainMemoryUsage))
    );
    worker1Data.setMetrics(ImmutableList.of(
        new Metric(Constants.AVG_GPU_UTILIZATION, worker1AvgGPUUtilization),
        new Metric(Constants.AVG_GPU_FB_MEMORY_USAGE, worker1AvgGPUFBMemoryUsage),
        new Metric(Constants.AVG_GPU_MAIN_MEMORY_USAGE, worker1AvgGPUMainMemoryUsage)
    ));

    taskDataMap.put(0, worker0Data);
    taskDataMap.put(1, worker1Data);

    Assert.assertEquals(15.0d,
        TonyUtils.getAvgMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.AVG_GPU_UTILIZATION), 0);
    Assert.assertEquals(30.0d,
        TonyUtils.getAvgMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.AVG_GPU_FB_MEMORY_USAGE), 0);
    Assert.assertEquals(40.0d,
        TonyUtils.getAvgMetricForTaskTypeAndMetricName(ImmutableMap.of("worker", taskDataMap), "worker",
            Constants.AVG_GPU_MAIN_MEMORY_USAGE), 0);
  }
}
