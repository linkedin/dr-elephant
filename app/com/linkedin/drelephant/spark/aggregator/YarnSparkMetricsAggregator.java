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

import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.spark.data.SparkApplicationData;
import com.linkedin.drelephant.spark.data.SparkExecutorData;
import com.linkedin.drelephant.util.SparkUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A more general and accurate implementation of SparkMetricsAggregator which is compatible with Spark on YARN
 * as well as executor dynamic allocation mode.
 *
 * What's new:
 * 1) Memory: Use actual executor memory request considering `overhead` instead of only `spark.executor.memory`.
 * 2) Time: Use running time of all existed executor, not duration(sum of task executing time) of
 *      executors only including of those which are alive while ending of application.
 *
 */
public class YarnSparkMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = LoggerFactory.getLogger(YarnSparkMetricsAggregator.class);

  private AggregatorConfigurationData _aggregatorConfigurationData;
  private double _storageMemWastageBuffer = 0.5;
  private String executorMemoryDefault = "1g";

  private static final String EXECUTOR_MEMORY_DEFAULT = "executor_memory_default";
  //private static final String STORAGE_MEM_WASTAGE_BUFFER = "storage_mem_wastage_buffer";

  private HadoopAggregatedData _hadoopAggregatedData = new HadoopAggregatedData();


  public YarnSparkMetricsAggregator(AggregatorConfigurationData _aggregatorConfigurationData) {
    this._aggregatorConfigurationData = _aggregatorConfigurationData;
    String configValue = _aggregatorConfigurationData.getParamMap().get(EXECUTOR_MEMORY_DEFAULT);
    if(configValue != null) {
      executorMemoryDefault = configValue;
    }
  }

  @Override
  public void aggregate(HadoopApplicationData data) {
    long resourceUsed = 0;
    long resourceWasted = 0;
    SparkApplicationData applicationData = (SparkApplicationData) data;

    long perExecutorMem = SparkUtils.getExecutorReqMemoryMB(applicationData, executorMemoryDefault) * FileUtils.ONE_MB;

    Iterator<String> executorIds = applicationData.getExecutorTrackingData().getExecutors().iterator();

    while(executorIds.hasNext()) {
      String executorId = executorIds.next();
      SparkExecutorData.ExecutorInfo executorInfo = applicationData.getExecutorTrackingData().getExecutorInfo(executorId);
      long executorDuration = executorInfo.getExecutorDuration();
      // store the resourceUsed in MBSecs
      resourceUsed += (executorDuration / Statistics.SECOND_IN_MS) * (perExecutorMem / FileUtils.ONE_MB);
      // maxMem is the maximum available storage memory
      // memUsed is how much storage memory is used.
      // any difference is wasted after a buffer of 50% is wasted
      long excessMemory = (long) (executorInfo.maxMem - (executorInfo.memUsed * (1.0 + _storageMemWastageBuffer)));
      if( excessMemory > 0) {
        resourceWasted += (executorDuration / Statistics.SECOND_IN_MS) * (excessMemory / FileUtils.ONE_MB);
      }
    }

    _hadoopAggregatedData.setResourceUsed(resourceUsed);
    _hadoopAggregatedData.setResourceWasted(resourceWasted);
    // TODO: to find a way to calculate the delay
    _hadoopAggregatedData.setTotalDelay(0L);
  }

  @Override
  public HadoopAggregatedData getResult() {
    return _hadoopAggregatedData;
  }
}
