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

package com.linkedin.drelephant.spark;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.spark.data.SparkExecutorData;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.apache.spark.SparkMetricsAggregator;
import org.apache.spark.deploy.history.SparkDataCollection;
import org.junit.Test;


public class TestSparkAggregatedMetrics {
  private static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";

  private SparkExecutorData.ExecutorInfo mockExecutorInfo(long maxMem, long memUsed, long duration) {
    SparkExecutorData.ExecutorInfo executorInfo = new SparkExecutorData.ExecutorInfo();
    executorInfo.maxMem = maxMem;
    executorInfo.memUsed = memUsed;
    executorInfo.duration = duration;

    return executorInfo;
  }
  @Test
  public void TestNullExecutors() {
    ApplicationType appType = new ApplicationType("SPARK");
    AggregatorConfigurationData conf =
        new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", appType, null);
    SparkMetricsAggregator metrics = new SparkMetricsAggregator(conf);

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
    SparkMetricsAggregator metrics = new SparkMetricsAggregator(conf);

    MockSparkApplicationData appData = new MockSparkApplicationData();
    appData.getExecutorData().setExecutorInfo("1", mockExecutorInfo(100 * FileUtils.ONE_MB, 60 * FileUtils.ONE_MB, 1000));
    appData.getExecutorData().setExecutorInfo("2", mockExecutorInfo(100 * FileUtils.ONE_MB, 60 * FileUtils.ONE_MB, 1000));

    metrics.aggregate(appData);

    Assert.assertEquals(0L, metrics.getResult().getResourceUsed());
    Assert.assertEquals(20L, metrics.getResult().getResourceWasted());
    Assert.assertEquals(0L, metrics.getResult().getTotalDelay());
  }

  @Test
  public void TestValidExecutorsAndValidEnvironmentData() {
    ApplicationType appType = new ApplicationType("SPARK");
    AggregatorConfigurationData conf =
        new AggregatorConfigurationData("org.apache.spark.SparkMetricsAggregator", appType, null);
    SparkMetricsAggregator metrics = new SparkMetricsAggregator(conf);

    MockSparkApplicationData appData = new MockSparkApplicationData();
    appData.getExecutorData().setExecutorInfo("1", mockExecutorInfo(100 * FileUtils.ONE_MB, 60 * FileUtils.ONE_MB, 1000));
    appData.getExecutorData().setExecutorInfo("2", mockExecutorInfo(100 * FileUtils.ONE_MB, 60 * FileUtils.ONE_MB, 1000));

    appData.getEnvironmentData().addSparkProperty(SPARK_EXECUTOR_MEMORY, "1048576000");

    metrics.aggregate(appData);

    Assert.assertEquals(2000L, metrics.getResult().getResourceUsed());
    Assert.assertEquals(20L, metrics.getResult().getResourceWasted());
    Assert.assertEquals(0L, metrics.getResult().getTotalDelay());
  }

}
