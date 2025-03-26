/*
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
 *
 */
package com.linkedin.drelephant.tez;

import com.linkedin.drelephant.analysis.*;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Aggregates task level metrics to application
 */

public class TezMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = Logger.getLogger(TezMetricsAggregator.class);

  private static final String TEZ_CONTAINER_CONFIG = "hive.tez.container.size";
  private static final String MAP_CONTAINER_CONFIG = "mapreduce.map.memory.mb";
  private static final String REDUCER_CONTAINER_CONFIG = "mapreduce.reduce.memory.mb";
  private static final String REDUCER_SLOW_START_CONFIG = "mapreduce.job.reduce.slowstart.completedmaps";
  private static final long CONTAINER_MEMORY_DEFAULT_BYTES = 2048L * FileUtils.ONE_MB;

  private HadoopAggregatedData _hadoopAggregatedData = null;
  private TezTaskLevelAggregatedMetrics _mapTasks;
  private TezTaskLevelAggregatedMetrics _reduceTasks;

  private AggregatorConfigurationData _aggregatorConfigurationData;

  public TezMetricsAggregator(AggregatorConfigurationData _aggregatorConfigurationData) {
    this._aggregatorConfigurationData = _aggregatorConfigurationData;
    _hadoopAggregatedData = new HadoopAggregatedData();
  }

  @Override
  public void aggregate(HadoopApplicationData hadoopData) {

    TezApplicationData data = (TezApplicationData) hadoopData;

    long mapTaskContainerSize = getMapContainerSize(data);
    long reduceTaskContainerSize = getReducerContainerSize(data);

    int reduceTaskSlowStartPercentage =
        (int) (Double.parseDouble(data.getConf().getProperty(REDUCER_SLOW_START_CONFIG)) * 100);


    //overwrite reduceTaskSlowStartPercentage to 100%. TODO: make use of the slow start percent
    reduceTaskSlowStartPercentage = 100;

    _mapTasks = new TezTaskLevelAggregatedMetrics(data.getMapTaskData(), mapTaskContainerSize, data.getStartTime());

    long reduceIdealStartTime = _mapTasks.getNthPercentileFinishTime(reduceTaskSlowStartPercentage);

    // Mappers list is empty
    if(reduceIdealStartTime == -1) {
      // ideal start time for reducer is infinite since it cannot start
      reduceIdealStartTime = Long.MAX_VALUE;
    }

    _reduceTasks = new TezTaskLevelAggregatedMetrics(data.getReduceTaskData(), reduceTaskContainerSize, reduceIdealStartTime);

    _hadoopAggregatedData.setResourceUsed(_mapTasks.getResourceUsed() + _reduceTasks.getResourceUsed());
    _hadoopAggregatedData.setTotalDelay(_mapTasks.getDelay() + _reduceTasks.getDelay());
    _hadoopAggregatedData.setResourceWasted(_mapTasks.getResourceWasted() + _reduceTasks.getResourceWasted());
  }

  @Override
  public HadoopAggregatedData getResult() {
    return _hadoopAggregatedData;
  }

  private long getMapContainerSize(HadoopApplicationData data) {
    try {
      long mapContainerSize = Long.parseLong(data.getConf().getProperty(TEZ_CONTAINER_CONFIG));
      if (mapContainerSize > 0)
        return mapContainerSize;
      else
        return Long.parseLong(data.getConf().getProperty(MAP_CONTAINER_CONFIG));
    } catch ( NumberFormatException ex) {
      return CONTAINER_MEMORY_DEFAULT_BYTES;
    }
  }

  private long getReducerContainerSize(HadoopApplicationData data) {
    try {
      long reducerContainerSize = Long.parseLong(data.getConf().getProperty(TEZ_CONTAINER_CONFIG));
      if (reducerContainerSize > 0)
        return reducerContainerSize;
      else
        return Long.parseLong(data.getConf().getProperty(REDUCER_CONTAINER_CONFIG));
    } catch ( NumberFormatException ex) {
      return CONTAINER_MEMORY_DEFAULT_BYTES;
    }
  }
}
