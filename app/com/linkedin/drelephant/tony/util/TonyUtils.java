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
package com.linkedin.drelephant.tony.util;

import com.linkedin.drelephant.tony.data.TonyTaskData;
import com.linkedin.tony.events.Metric;
import java.util.List;
import java.util.Map;


public class TonyUtils {
  /**
   * Returns the max metric value of any task of the specified type given metricName
   * Skips metrics collection for unavailable metric data
   * @param taskMap a map containing data for all tasks
   * @param taskType the task type
   * @param metricName the name of the metric to query
   * @return the max metric value of any task of the specified type
   */
  public static double getMaxMetricForTaskTypeAndMetricName(Map<String, Map<Integer, TonyTaskData>> taskMap,
      String taskType, String metricName) {
    double maxMetric = 0.0;
    if (taskMap.get(taskType) == null) {
      return -1.0d;
    }

    for (TonyTaskData taskData : taskMap.get(taskType).values()) {
      List<Metric> metrics = taskData.getMetrics();
      if (metrics == null) {
        continue;
      }
      for (Metric metric : metrics) {
        if (metric.getName().equals(metricName)) {
          if (metric.getValue() <= 0) {
            continue;
          }

          if (metric.getValue() > maxMetric) {
            maxMetric = metric.getValue();
          }
        }
      }
    }

    return maxMetric;
  }

  /**
   * Returns the average metric value of all task of the specified type given metricName
   * Skips metrics collection for unavailable metric data
   * @param taskMap a map containing data for all tasks
   * @param taskType the task type
   * @param metricName the name of the metric to query
   * @return the average metric value of any task of the specified type
   */
  public static double getAvgMetricForTaskTypeAndMetricName(Map<String, Map<Integer, TonyTaskData>> taskMap,
      String taskType, String metricName) {
    double avgMetric = 0.0;
    double numMetrics = 0;
    if (taskMap.get(taskType) == null) {
      return -1.0d;
    }
    for (TonyTaskData taskData : taskMap.get(taskType).values()) {
      double avgMetricPerTask = 0.0;
      List<Metric> metrics = taskData.getMetrics();
      if (metrics == null) {
        continue;
      }
      for (Metric metric : metrics) {
        if (metric.getName().equals(metricName)) {
          if (metric.getValue() <= 0) {
            continue;
          }

          avgMetric += metric.getValue();
          numMetrics++;
        }
      }
    }

    return avgMetric / numMetrics;
  }
}
