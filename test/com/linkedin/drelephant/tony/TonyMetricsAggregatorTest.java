/*
 * Copyright 2019 LinkedIn Corp.
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
package com.linkedin.drelephant.tony;

import com.google.common.collect.ImmutableList;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tony.data.TonyApplicationData;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.events.Metric;
import com.linkedin.tony.events.TaskFinished;
import com.linkedin.tony.events.TaskStarted;
import com.linkedin.tony.rpc.impl.TaskStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.linkedin.drelephant.tony.TonyMetricsAggregator.MEMORY_BUFFER;


public class TonyMetricsAggregatorTest {
  @Test
  public void testMetricsAggregator() {
    Configuration conf = new Configuration(false);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 2);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 1);

    List<Event> events = new ArrayList<>();
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 0, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 1, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.PS_JOB_NAME, 0, null),0L));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.WORKER_JOB_NAME, 0, TaskStatus.SUCCEEDED.toString(),
            ImmutableList.of(new Metric(Constants.MAX_MEMORY_BYTES, (double) FileUtils.ONE_GB))),
        10L * Statistics.SECOND_IN_MS));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.WORKER_JOB_NAME, 1, TaskStatus.SUCCEEDED.toString(),
            ImmutableList.of(new Metric(Constants.MAX_MEMORY_BYTES, (double) 2 * FileUtils.ONE_GB))),
        20L * Statistics.SECOND_IN_MS));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.PS_JOB_NAME, 0, TaskStatus.SUCCEEDED.toString(),
            ImmutableList.of(new Metric(Constants.MAX_MEMORY_BYTES, (double) FileUtils.ONE_GB))),
        20L * Statistics.SECOND_IN_MS));

    long expectedResourcesUsed = 10 * 4 * 1024 + 20 * 4 * 1024 + 20 * 4 * 1024;
    long expectedResourcesWasted = 10 * (long) (4 * 1024 - 2 * 1024 * MEMORY_BUFFER)
        + 20 * (long) (4 * 1024 - 2 * 1024 * MEMORY_BUFFER)
        + 20 * (long) (4 * 1024 - 1 * 1024 * MEMORY_BUFFER);

    ApplicationType appType = new ApplicationType(Constants.APP_TYPE);
    TonyApplicationData data = new TonyApplicationData("application_123_456", appType, conf, events);
    TonyMetricsAggregator metricsAggregator = new TonyMetricsAggregator(null);
    metricsAggregator.aggregate(data);
    HadoopAggregatedData result = metricsAggregator.getResult();
    Assert.assertEquals(expectedResourcesUsed, result.getResourceUsed());
    Assert.assertEquals(expectedResourcesWasted, result.getResourceWasted());
  }

  /**
   * Verifies that wasted resources is 0 when there are no metrics.
   */
  @Test
  public void testNullMetrics() {
    Configuration conf = new Configuration(false);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 2);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 1);

    List<Event> events = new ArrayList<>();
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 0, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 1, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.PS_JOB_NAME, 0, null),0L));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.WORKER_JOB_NAME, 0, TaskStatus.SUCCEEDED.toString(), Collections.emptyList()),
        10L * Statistics.SECOND_IN_MS));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.WORKER_JOB_NAME, 1, TaskStatus.SUCCEEDED.toString(), Collections.emptyList()),
        20L * Statistics.SECOND_IN_MS));
    events.add(new Event(EventType.TASK_FINISHED,
        new TaskFinished(Constants.PS_JOB_NAME, 0, TaskStatus.SUCCEEDED.toString(), Collections.emptyList()),
        20L * Statistics.SECOND_IN_MS));

    long expectedResourcesUsed = 10 * 4 * 1024 + 20 * 4 * 1024 + 20 * 4 * 1024;
    long expectedResourcesWasted = 0;

    ApplicationType appType = new ApplicationType(Constants.APP_TYPE);
    TonyApplicationData data = new TonyApplicationData("application_123_456", appType, conf, events);
    TonyMetricsAggregator metricsAggregator = new TonyMetricsAggregator(null);
    metricsAggregator.aggregate(data);
    HadoopAggregatedData result = metricsAggregator.getResult();
    Assert.assertEquals(expectedResourcesUsed, result.getResourceUsed());
    Assert.assertEquals(expectedResourcesWasted, result.getResourceWasted());
  }

  /**
   * Verifies that used and wasted resources are 0 when there are no task finished or application finished events.
   */
  @Test
  public void testNoEndEvents() {
    Configuration conf = new Configuration(false);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 2);
    conf.set(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.MEMORY), "4g");
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 1);

    List<Event> events = new ArrayList<>();
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 0, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.WORKER_JOB_NAME, 1, null),0L));
    events.add(new Event(EventType.TASK_STARTED, new TaskStarted(Constants.PS_JOB_NAME, 0, null),0L));

    long expectedResourcesUsed = 0;
    long expectedResourcesWasted = 0;

    ApplicationType appType = new ApplicationType(Constants.APP_TYPE);
    TonyApplicationData data = new TonyApplicationData("application_123_456", appType, conf, events);
    TonyMetricsAggregator metricsAggregator = new TonyMetricsAggregator(null);
    metricsAggregator.aggregate(data);
    HadoopAggregatedData result = metricsAggregator.getResult();
    Assert.assertEquals(expectedResourcesUsed, result.getResourceUsed());
    Assert.assertEquals(expectedResourcesWasted, result.getResourceWasted());
  }
}
