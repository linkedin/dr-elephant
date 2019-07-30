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
package com.linkedin.drelephant.tony.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.HeuristicResultDetails;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.tony.data.TonyApplicationData;
import com.linkedin.drelephant.tony.data.TonyTaskData;
import com.linkedin.drelephant.tony.util.TonyUtils;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * For each type of task (e.g.: worker, ps), this heuristic checks the GPU utilization across all tasks of that type
 * and compares against some thresholds. The final severity is the highest severity across all task types.
 */
public class TaskGPUHeuristic implements Heuristic<TonyApplicationData> {
  private static final Logger _LOGGER = Logger.getLogger(TaskGPUHeuristic.class);
  private static final String[] MAX_METRICS_TO_APPLY = {
      Constants.MAX_GPU_UTILIZATION,
      Constants.MAX_GPU_FB_MEMORY_USAGE,
      Constants.MAX_GPU_MAIN_MEMORY_USAGE};
  private static final String[] AVG_METRICS_TO_APPLY = {
      Constants.AVG_GPU_UTILIZATION,
      Constants.AVG_GPU_FB_MEMORY_USAGE,
      Constants.AVG_GPU_MAIN_MEMORY_USAGE
  };

  private HeuristicConfigurationData _heuristicConfData;

  /**
   * Constructor for {@link TaskGPUHeuristic}.
   * @param heuristicConfData  the configuration for this heuristic
   */
  public TaskGPUHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }

  @Override
  public HeuristicResult apply(TonyApplicationData data) {
    _LOGGER.debug("Applying TaskGPUHeuristic");
    Map<String, Map<Integer, TonyTaskData>> taskMap = data.getTaskMap();
    Configuration conf = data.getConfiguration();

    Set<String> taskTypes = com.linkedin.tony.util.Utils.getAllJobTypes(conf);
    Severity finalSeverity = Severity.NONE;
    List<HeuristicResultDetails> details = new ArrayList<>();

    for (String taskType : taskTypes) {
      details.add(new HeuristicResultDetails("Number of " + taskType + " tasks",
          Integer.toString(taskMap.get(taskType).size())));

      // get number of GPU resource requested, if any
      int numGPUsRequested = conf.getInt(TonyConfigurationKeys.getResourceKey(taskType, Constants.GPUS), 0);
      if (numGPUsRequested > 0) {
        details.add(new HeuristicResultDetails("Number of GPUs requested per " + taskType + " tasks",
            Integer.toString(numGPUsRequested)));
      }

      // get global max gpu utilization metrics
      for (String maxMetricToApply : MAX_METRICS_TO_APPLY) {
        double maxMetric = TonyUtils.getMaxMetricForTaskTypeAndMetricName(taskMap, taskType, maxMetricToApply);

        if (maxMetric <= 0 || Double.isNaN(maxMetric)) {
          details.add(new HeuristicResultDetails(maxMetricToApply + " in any " + taskType + " task", "Unknown"));
          continue;
        }
        details.add(new HeuristicResultDetails(maxMetricToApply + " in any " + taskType + " task",
            String.format("%.2f", maxMetric) + "%"));
      }

      // get global average gpu utilization metrics
      for (String avgMetricToApply : AVG_METRICS_TO_APPLY) {
        double avgMetric = TonyUtils.getAvgMetricForTaskTypeAndMetricName(taskMap, taskType, avgMetricToApply);

        if (avgMetric <= 0 || Double.isNaN(avgMetric)) {
          details.add(new HeuristicResultDetails(avgMetricToApply + " in any " + taskType + " task", "Unknown"));
          continue;
        }
        details.add(new HeuristicResultDetails(avgMetricToApply + " in any " + taskType + " task",
            String.format("%.2f", avgMetric) + "%"));
      }

      // compare to threshold and update severity
      // TODO: pass all metrics collected as not severe for now, update heuristic when collected enough data to
      // determine the appropriate threshold.
    }

    return new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), finalSeverity,
        0, details);
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }
}
