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

package org.apache.spark;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = LoggerFactory.getLogger(SparkMetricsAggregator.class);

  private AggregatorConfigurationData _aggregatorConfigurationData;
  private double _storageMemWastageBuffer = 0.5;

  private static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  private static final String STORAGE_MEM_WASTAGE_BUFFER = "storage_mem_wastage_buffer";

  private HadoopAggregatedData _hadoopAggregatedData = new HadoopAggregatedData();


  public SparkMetricsAggregator(AggregatorConfigurationData _aggregatorConfigurationData) {
    this._aggregatorConfigurationData = _aggregatorConfigurationData;
    String configValue = _aggregatorConfigurationData.getParamMap().get(STORAGE_MEM_WASTAGE_BUFFER);
    if(configValue != null) {
      _storageMemWastageBuffer = Double.parseDouble(configValue);
    }
  }

  @Override
  public void aggregate(HadoopApplicationData data) {
    long resourceUsed = 0;
    long resourceWasted = 0;
    SparkApplicationData applicationData = (SparkApplicationData) data;

    long perExecutorMem =
        MemoryFormatUtils.stringToBytes(applicationData.getEnvironmentData().getSparkProperty(SPARK_EXECUTOR_MEMORY, "0"));

    Iterator<String> executorIds = applicationData.getExecutorData().getExecutors().iterator();

    while(executorIds.hasNext()) {
      String executorId = executorIds.next();
      SparkExecutorData.ExecutorInfo executorInfo = applicationData.getExecutorData().getExecutorInfo(executorId);
      // store the resourceUsed in MBSecs
      resourceUsed += (executorInfo.duration / Statistics.SECOND_IN_MS) * (perExecutorMem / FileUtils.ONE_MB);
      // maxMem is the maximum available storage memory
      // memUsed is how much storage memory is used.
      // any difference is wasted after a buffer of 50% is wasted
      long excessMemory = (long) (executorInfo.maxMem - (executorInfo.memUsed * (1.0 + _storageMemWastageBuffer)));
      if( excessMemory > 0) {
        resourceWasted += (executorInfo.duration / Statistics.SECOND_IN_MS) * (excessMemory / FileUtils.ONE_MB);
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
