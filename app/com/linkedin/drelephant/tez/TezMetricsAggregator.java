package com.linkedin.drelephant.tez;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class TezMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = Logger.getLogger(TezMetricsAggregator.class);

  private static final String TEZ_CONTAINER_CONFIG = "hive.tez.container.size";
  private static final String MAP_CONTAINER_CONFIG = "mapreduce.map.memory.mb";
  private static final String REDUCER_CONTAINER_CONFIG = "mapreduce.reduce.memory.mb";
  private static final String REDUCER_SLOW_START_CONFIG = "mapreduce.job.reduce.slowstart.completedmaps";
  private static final long CONTAINER_MEMORY_DEFAULT_BYTES = 2048L * FileUtils.ONE_MB;

  private HadoopAggregatedData _hadoopAggregatedData = null;
  private TezTaskLevelAggregatedMetrics mapTasks;
  private TezTaskLevelAggregatedMetrics reduceTasks;

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

    mapTasks = new TezTaskLevelAggregatedMetrics(data.getMapTaskData(), mapTaskContainerSize, data.getStartTime());

    long reduceIdealStartTime = mapTasks.getNthPercentileFinishTime(reduceTaskSlowStartPercentage);

    // Mappers list is empty
    if(reduceIdealStartTime == -1) {
      // ideal start time for reducer is infinite since it cannot start
      reduceIdealStartTime = Long.MAX_VALUE;
    }

    reduceTasks = new TezTaskLevelAggregatedMetrics(data.getReduceTaskData(), reduceTaskContainerSize, reduceIdealStartTime);

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