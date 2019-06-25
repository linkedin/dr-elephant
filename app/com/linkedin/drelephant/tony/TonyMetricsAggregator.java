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

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tony.data.TonyApplicationData;
import com.linkedin.drelephant.tony.data.TonyTaskData;
import com.linkedin.drelephant.tony.util.TonyUtils;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;


public class TonyMetricsAggregator implements HadoopMetricsAggregator {
  @VisibleForTesting
  static final double MEMORY_BUFFER = 1.5;

  private HadoopAggregatedData _hadoopAggregatedData;

  /**
   * Creates a new {@code TonyMetricsAggregator}.
   * @param unused  Dr. Elephant expects a constructor of this form but {@code TonyMetricsAggregator} does not need this
   */
  public TonyMetricsAggregator(AggregatorConfigurationData unused) { }

  @Override
  public void aggregate(HadoopApplicationData data) {
    _hadoopAggregatedData = new HadoopAggregatedData();

    TonyApplicationData tonyData = (TonyApplicationData) data;
    Configuration tonyConf = tonyData.getConfiguration();

    long mbSecUsed = 0;
    long mbSecWasted = 0;

    Map<String, Map<Integer, TonyTaskData>> taskMap = tonyData.getTaskMap();
    for (Map.Entry<String, Map<Integer, TonyTaskData>> entry : taskMap.entrySet()) {
      String taskType = entry.getKey();

      String memoryString = tonyConf.get(TonyConfigurationKeys.getResourceKey(taskType, Constants.MEMORY));
      String memoryStringMB = com.linkedin.tony.util.Utils.parseMemoryString(memoryString);
      long mbRequested = Long.parseLong(memoryStringMB);
      double maxMemoryMBUsed = TonyUtils.getMaxMetricForTaskTypeAndMetricName(taskMap, taskType,
          Constants.MAX_MEMORY_BYTES) / FileUtils.ONE_MB;

      for (TonyTaskData taskData : entry.getValue().values()) {
        long taskDurationSec = (taskData.getTaskEndTime() - taskData.getTaskStartTime()) / Statistics.SECOND_IN_MS;
        if (taskDurationSec < 0) {
          // Most likely TASK_FINISHED and APPLICATION_FINISHED events are missing for the task.
          continue;
        }
        mbSecUsed += mbRequested * taskDurationSec;

        if (maxMemoryMBUsed == 0) {
          // If we don't have max memory metrics, don't calculate wasted memory.
          continue;
        }
        long wastedMemory = (long) (mbRequested - maxMemoryMBUsed * MEMORY_BUFFER);
        if (wastedMemory > 0) {
          mbSecWasted += wastedMemory * taskDurationSec;
        }
      }
    }

    _hadoopAggregatedData.setResourceUsed(mbSecUsed);
    _hadoopAggregatedData.setResourceWasted(mbSecWasted);
    // TODO: Calculate and set delay
  }

  @Override
  public HadoopAggregatedData getResult() {
    return _hadoopAggregatedData;
  }
}
