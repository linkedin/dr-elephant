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

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class MapReduceMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = Logger.getLogger(MapReduceMetricsAggregator.class);
  private static final String MAP_CONTAINER_CONFIG = "mapreduce.map.memory.mb";
  private static final String REDUCER_CONTAINER_CONFIG = "mapreduce.reduce.memory.mb";
  private static final String REDUCER_SLOW_START_CONFIG = "mapreduce.job.reduce.slowstart.completedmaps";
  private static final long CONTAINER_MEMORY_DEFAULT_MBYTES = 2048L;

  private HadoopAggregatedData _hadoopAggregatedData = null;
  private TaskLevelAggregatedMetrics mapTasks;
  private TaskLevelAggregatedMetrics reduceTasks;

  private AggregatorConfigurationData _aggregatorConfigurationData;

  public MapReduceMetricsAggregator(AggregatorConfigurationData _aggregatorConfigurationData) {
    this._aggregatorConfigurationData = _aggregatorConfigurationData;
    _hadoopAggregatedData = new HadoopAggregatedData();
  }

  @Override
  public void aggregate(HadoopApplicationData hadoopData) {

    MapReduceApplicationData data = (MapReduceApplicationData) hadoopData;

    long mapTaskContainerSize = getMapContainerSize(data);
    long reduceTaskContainerSize = getReducerContainerSize(data);

    int reduceTaskSlowStartPercentage =
        (int) (Double.parseDouble(data.getConf().getProperty(REDUCER_SLOW_START_CONFIG)) * 100);


    //overwrite reduceTaskSlowStartPercentage to 100%. TODO: make use of the slow start percent
    reduceTaskSlowStartPercentage = 100;

    mapTasks = new TaskLevelAggregatedMetrics(data.getMapperData(), mapTaskContainerSize, data.getSubmitTime());

    long reduceIdealStartTime = mapTasks.getNthPercentileFinishTime(reduceTaskSlowStartPercentage);

    // Mappers list is empty
    if(reduceIdealStartTime == -1) {
      // ideal start time for reducer is infinite since it cannot start
      reduceIdealStartTime = Long.MAX_VALUE;
    }

    reduceTasks = new TaskLevelAggregatedMetrics(data.getReducerData(), reduceTaskContainerSize, reduceIdealStartTime);

    _hadoopAggregatedData.setResourceUsed(mapTasks.getResourceUsed() + reduceTasks.getResourceUsed());
    _hadoopAggregatedData.setTotalDelay(mapTasks.getDelay() + reduceTasks.getDelay());
    _hadoopAggregatedData.setResourceWasted(mapTasks.getResourceWasted() + reduceTasks.getResourceWasted());
  }

  @Override
  public HadoopAggregatedData getResult() {
    return _hadoopAggregatedData;
  }

  private long getMapContainerSize(HadoopApplicationData data) {
    try {
      long value = Long.parseLong(data.getConf().getProperty(MAP_CONTAINER_CONFIG));
      return (value < 0) ? CONTAINER_MEMORY_DEFAULT_MBYTES : value;
    } catch ( NumberFormatException ex) {
      return CONTAINER_MEMORY_DEFAULT_MBYTES;
    }
  }

  private long getReducerContainerSize(HadoopApplicationData data) {
    try {
      long value = Long.parseLong(data.getConf().getProperty(REDUCER_CONTAINER_CONFIG));
      return (value < 0) ? CONTAINER_MEMORY_DEFAULT_MBYTES : value;
    } catch ( NumberFormatException ex) {
      return CONTAINER_MEMORY_DEFAULT_MBYTES;
    }
  }
}
