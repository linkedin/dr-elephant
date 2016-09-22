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

package com.linkedin.drelephant.spark.aggregator;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.spark.MockSparkApplicationData;
import com.linkedin.drelephant.spark.data.SparkEnvironmentData;
import com.linkedin.drelephant.spark.data.SparkExecutorData;
import com.linkedin.drelephant.util.SparkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkMetricsAggregator;
import org.junit.Assert;
import org.junit.Test;


public class TestYarnSparkAggregatedMetrics {
  private static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";

  private static final long HOUR = 60 * 60 * 1000;

  private SparkExecutorData.ExecutorInfo mockExecutorInfo(long startTime, long finishTime, long duration) {
    SparkExecutorData.ExecutorInfo executorInfo = new SparkExecutorData.ExecutorInfo();
    executorInfo.startTime = startTime;
    executorInfo.finishTime = finishTime;
    executorInfo.duration = duration;

    return executorInfo;
  }

  @Test
  public void TestNullExecutors() {
    ApplicationType appType = new ApplicationType("SPARK");
    AggregatorConfigurationData conf =
        new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", appType, null);
    YarnSparkMetricsAggregator metrics = new YarnSparkMetricsAggregator(conf);

    MockSparkApplicationData appData = new MockSparkApplicationData();

    metrics.aggregate(appData);

    Assert.assertEquals(metrics.getResult().getResourceUsed() , 0L);
    Assert.assertEquals(metrics.getResult().getResourceWasted() , 0L);
    Assert.assertEquals(metrics.getResult().getTotalDelay() , 0L);
  }

  @Test
  public void TestValidExecutorsWithNoEnvironmentData() {
    ApplicationType appType = new ApplicationType("SPARK");
    AggregatorConfigurationData conf =
        new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", appType, null);
    YarnSparkMetricsAggregator metrics = new YarnSparkMetricsAggregator(conf);

    MockSparkApplicationData appData = new MockSparkApplicationData();
    appData.getExecutorData().setExecutorInfo("1", mockExecutorInfo(0, 1 * HOUR, 1000));
    appData.getExecutorData().setExecutorInfo("2", mockExecutorInfo(0, 2 * HOUR, 1000));

    metrics.aggregate(appData);

    Assert.assertEquals(1024 * 3 * 3600, metrics.getResult().getResourceUsed());
    Assert.assertEquals(0, metrics.getResult().getResourceWasted());
    Assert.assertEquals(0L, metrics.getResult().getTotalDelay());
  }

  @Test
  public void TestValidExecutorsAndValidEnvironmentData() {
    ApplicationType appType = new ApplicationType("SPARK");
    AggregatorConfigurationData conf =
        new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", appType, null);
    YarnSparkMetricsAggregator metrics = new YarnSparkMetricsAggregator(conf);

    long appFinishTime = 3 * HOUR;

    MockSparkApplicationData appData = new MockSparkApplicationData();
    appData.getExecutorData().setExecutorInfo("1", mockExecutorInfo(0, 2 * HOUR, 1000)); // runningTime: 2h
    appData.getExecutorData().setExecutorInfo("2", mockExecutorInfo(0, appFinishTime, 1000)); // runningTime: 3h
    appData.getExecutorData().setExecutorInfo("3", mockExecutorInfo(1 * HOUR, 2 * HOUR, 1000)); // runningTime: 1h
    appData.getExecutorData().setExecutorInfo("4", mockExecutorInfo(2 * HOUR, appFinishTime, 1000)); // runningTime: 1h

    // finally executor memory request on YARN is 1g + 2g
    setExecutorReqEnv(appData.getEnvironmentData(), "10g", "2048", "0.2");

    metrics.aggregate(appData);

    // Total executor running time is 7h, so aggregated memory resource usage is 7h * 12g = 7 * 3600 * 12 * 1024 MB-seconds
    Assert.assertEquals(7 * 3600 * 12 * 1024, metrics.getResult().getResourceUsed());
    //Assert.assertEquals(20L, metrics.getResult().getResourceWasted());
    //Assert.assertEquals(0L, metrics.getResult().getTotalDelay());
  }

  @Test
  public void testGetYarnExecutorMemoryRequest() {
    // memory request: 11g
    MockSparkApplicationData appData = new MockSparkApplicationData();
    setExecutorReqEnv(appData.getEnvironmentData(), "10g", "1024", "0.3");
    Assert.assertEquals(11 * 1024, SparkUtils.getExecutorReqMemoryMB(appData, "1g"));

    // memory request: 13g
    appData = new MockSparkApplicationData();
    setExecutorReqEnv(appData.getEnvironmentData(), "10g", null, "0.3");
    Assert.assertEquals(13 * 1024, SparkUtils.getExecutorReqMemoryMB(appData, "1g"));

    // memory request: 12g
    appData = new MockSparkApplicationData();
    setExecutorReqEnv(appData.getEnvironmentData(), "10g", null, null);
    Assert.assertEquals(12 * 1024, SparkUtils.getExecutorReqMemoryMB(appData, "1g"));

    // memory request: 10g + 384m
    appData = new MockSparkApplicationData();
    setExecutorReqEnv(appData.getEnvironmentData(), "10g", null, "0.01");
    Assert.assertEquals(10 * 1024 + 384, SparkUtils.getExecutorReqMemoryMB(appData, "1g"));

    // memory request: 5g + 1024m
    appData = new MockSparkApplicationData();
    setExecutorReqEnv(appData.getEnvironmentData(), null, null, null);
    Assert.assertEquals(5 * 1024 + 1024, SparkUtils.getExecutorReqMemoryMB(appData, "5g"));
  }

  private void setExecutorReqEnv(SparkEnvironmentData envData, String execMemory, String execOverhead, String execOverheadFactor) {
    envData.addSparkProperty(SparkUtils.SPARK_MASTER(), "yarn-client");
    if (execMemory != null) {
      envData.addSparkProperty(SparkUtils.SPARK_EXECUTOR_MEMORY(), execMemory);
    }
    if (execOverhead != null) {
      envData.addSparkProperty(SparkUtils.MEMORY_OVERHEAD(), execOverhead);
    }

    if (execOverheadFactor != null) {
      envData.addSparkProperty(SparkUtils.MEMORY_OVERHEAD_FACTOR(), execOverheadFactor);
    }
  }

}
