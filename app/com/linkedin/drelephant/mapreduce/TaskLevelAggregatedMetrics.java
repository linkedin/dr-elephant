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
import com.linkedin.drelephant.math.Statistics;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class TaskLevelAggregatedMetrics {

  private static final Logger logger = Logger.getLogger(TaskLevelAggregatedMetrics.class);

  private long _delay = 0;
  private long _resourceWasted = 0;
  private long _resourceUsed = 0;

  private List<Long> finishTimes = new ArrayList<Long>();
  private List<Long> durations = new ArrayList<Long>();

  private static final double MEMORY_BUFFER = 1.5;
  private static final double CLUSTER_MEMORY_FACTOR = 2.1;

  /**
   * Returns the nth percentile finish job
   * @param percentile The percentile of finish job to return
   * @return The nth percentile finish job
   */
  public long getNthPercentileFinishTime(int percentile)
  {
    if(finishTimes == null || finishTimes.size() == 0 ) {
      return -1;
    }
    return Statistics.percentile(finishTimes, percentile);
  }

  /**
   * Constructor for TaskLevelAggregatedMetrics
   * @param taskData Array containing the task data for mappers and/or reducers
   * @param containerSize The container size of the tasks
   * @param idealStartTime The ideal start time for the task. For mappers it is the submit time, for
   *                       reducers, it is the time when the number of completed maps become more than
   *                       the slow start time.
   */
  public TaskLevelAggregatedMetrics(MapReduceTaskData[] taskData, long containerSize, long idealStartTime) {
    compute(taskData, containerSize, idealStartTime);
  }

  /**
   * Returns the overall delay for the tasks.
   * @return The delay of the tasks.
   */
  public long getDelay() {
    return _delay;
  }

  /**
   * Retruns the resources wasted by all the tasks in MB Seconds
   * @return The wasted resources of all the tasks in MB Seconds
   */
  public long getResourceWasted() {
    return _resourceWasted;
  }

  /**
   * Returns the resource used by all the tasks in MB Seconds
   * @return The total resources used by all tasks in MB Seconds
   */
  public long getResourceUsed() {
    return _resourceUsed;
  }

  /**
   * Computes the aggregated metrics -> peakMemory, delay, total task duration, wasted resources and memory usage.
   * Aggregated metrics are expected to be approximation when sampling is enabled.
   * @param taskDatas
   * @param containerSize
   * @param idealStartTime
   */
  private void compute(MapReduceTaskData[] taskDatas, long containerSize, long idealStartTime) {

    long peakMemoryNeed = 0;
    long taskFinishTimeMax = 0;
    long taskDurationMax = 0;

    // if there are zero tasks, then nothing to compute.
    if(taskDatas == null || taskDatas.length == 0) {
      return;
    }

    for (MapReduceTaskData taskData: taskDatas) {
      if (!taskData.isTimeAndCounterDataPresent()) {
        continue;
      }
      long taskMemory = taskData.getCounters().get(MapReduceCounterData.CounterName.PHYSICAL_MEMORY_BYTES)/ FileUtils.ONE_MB; // MB
      long taskVM = taskData.getCounters().get(MapReduceCounterData.CounterName.VIRTUAL_MEMORY_BYTES)/ FileUtils.ONE_MB; // MB
      long taskDuration = taskData.getFinishTimeMs() - taskData.getStartTimeMs(); // Milliseconds
      long taskCost =  (containerSize) * (taskDuration / Statistics.SECOND_IN_MS); // MB Seconds

      durations.add(taskDuration);
      finishTimes.add(taskData.getFinishTimeMs());

      //peak Memory usage
      long memoryRequiredForVM = (long) (taskVM/CLUSTER_MEMORY_FACTOR);
      long biggerMemoryRequirement = memoryRequiredForVM > taskMemory ? memoryRequiredForVM : taskMemory;
      peakMemoryNeed = biggerMemoryRequirement > peakMemoryNeed ? biggerMemoryRequirement : peakMemoryNeed;

      if(taskFinishTimeMax < taskData.getFinishTimeMs()) {
        taskFinishTimeMax = taskData.getFinishTimeMs();
      }

      if(taskDurationMax < taskDuration) {
        taskDurationMax = taskDuration;
      }
      _resourceUsed += taskCost;
    }

    // Compute the delay in starting the task.
    _delay = taskFinishTimeMax - (idealStartTime + taskDurationMax);

    // invalid delay
    if(_delay < 0) {
      _delay = 0;
    }

    // wastedResources
    long wastedMemory = containerSize -  (long) (peakMemoryNeed * MEMORY_BUFFER); // give a 50% buffer
    if(wastedMemory > 0) {
      for (long duration : durations) {
        _resourceWasted += (wastedMemory) * (duration / Statistics.SECOND_IN_MS); // MB Seconds
      }
    }
  }

}
