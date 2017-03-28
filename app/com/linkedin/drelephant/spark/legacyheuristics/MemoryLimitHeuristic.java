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

package com.linkedin.drelephant.spark.legacyheuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.spark.legacydata.SparkEnvironmentData;
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import com.linkedin.drelephant.util.Utils;
import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import static com.linkedin.drelephant.spark.legacydata.SparkExecutorData.EXECUTOR_DRIVER_NAME;


/**
 * This heuristic checks for memory consumption.
 */
public class MemoryLimitHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(MemoryLimitHeuristic.class);

  public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
  public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
  public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";

  public static final String SPARK_STORAGE_MEMORY_FRACTION = "spark.storage.memoryFraction";
  public static final double DEFAULT_SPARK_STORAGE_MEMORY_FRACTION = 0.6d;

  // Severity parameters.
  private static final String MEM_UTILIZATION_SEVERITY = "mem_util_severity";
  private static final String TOTAL_MEM_SEVERITY = "total_mem_severity_in_tb";

  // Default value of parameters
  private double[] memUtilLimits = {0.8d, 0.6d, 0.4d, 0.2d};
  private double[] totalMemLimits = {0.5d, 1d, 1.5d, 2d};      // Peak Memory / Total Storage Memory

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    if(paramMap.get(MEM_UTILIZATION_SEVERITY) != null) {
      double[] confMemUtilLimits = Utils.getParam(paramMap.get(MEM_UTILIZATION_SEVERITY), memUtilLimits.length);
      if (confMemUtilLimits != null) {
        memUtilLimits = confMemUtilLimits;
      }
    }
    logger.info(heuristicName + " will use " + MEM_UTILIZATION_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(memUtilLimits));

    if(paramMap.get(TOTAL_MEM_SEVERITY) != null) {
      double[] confTotalMemLimits = Utils.getParam(paramMap.get(TOTAL_MEM_SEVERITY), totalMemLimits.length);
      if (confTotalMemLimits != null) {
        totalMemLimits = confTotalMemLimits;
      }
    }
    logger.info(heuristicName + " will use " + TOTAL_MEM_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(totalMemLimits));
    for (int i = 0; i < totalMemLimits.length; i++) {
      totalMemLimits[i] = MemoryFormatUtils.stringToBytes(totalMemLimits[i] + "T");
    }
  }

  public MemoryLimitHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {

    int executorNum = Integer.parseInt(data.getEnvironmentData().getSparkProperty(SPARK_EXECUTOR_INSTANCES, "0"));
    long perExecutorMem =
        MemoryFormatUtils.stringToBytes(data.getEnvironmentData().getSparkProperty(SPARK_EXECUTOR_MEMORY, "0"));

    long totalExecutorMem = executorNum * perExecutorMem;

    long totalStorageMem = getTotalStorageMem(data);
    long totalDriverMem = getTotalDriverMem(data);
    long peakMem = getStoragePeakMemory(data);

    Severity totalMemorySeverity = getTotalMemorySeverity(totalExecutorMem);
    Severity memoryUtilizationServerity = getMemoryUtilizationSeverity(peakMem, totalStorageMem);

    HeuristicResult result =
        new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(),
            Severity.max(totalMemorySeverity, memoryUtilizationServerity), 0);

    result.addResultDetail("Total executor memory allocated", String
        .format("%s (%s x %s)", MemoryFormatUtils.bytesToString(totalExecutorMem),
            MemoryFormatUtils.bytesToString(perExecutorMem), executorNum));
    result.addResultDetail("Total driver memory allocated", MemoryFormatUtils.bytesToString(totalDriverMem));
    result.addResultDetail("Total memory allocated for storage", MemoryFormatUtils.bytesToString(totalStorageMem));
    result.addResultDetail("Total memory used at peak", MemoryFormatUtils.bytesToString(peakMem));
    result.addResultDetail("Memory utilization rate", String.format("%1.3f", peakMem * 1.0 / totalStorageMem));
    return result;
  }

  /**
   * Get the total driver memory
   *
   * @param data The spark application data that contains the information
   * @return The memory in bytes
   */
  private static long getTotalDriverMem(SparkApplicationData data) {
    long bytes = MemoryFormatUtils.stringToBytes(data.getEnvironmentData().getSparkProperty(SPARK_DRIVER_MEMORY));
    // spark.driver.memory might not be present, in which case we would infer it from the executor data
    if (bytes == 0L) {
      SparkExecutorData.ExecutorInfo info = data.getExecutorData().getExecutorInfo(EXECUTOR_DRIVER_NAME);
      if (info == null) {
        logger.error("Application id [" + data.getGeneralData().getApplicationId()
            + "] does not contain driver memory configuration info and also does not contain executor driver info."
            + " Unable to detect is driver memory usage.");
        return 0L;
      }
      // This maxmium memory only counts in memory for storage
      bytes = (long) (info.maxMem / getStorageMemoryFraction(data.getEnvironmentData()));
    }

    return bytes;
  }

  /**
   * Get the storage memory fraction ratio used for storage
   *
   * @param data The spark environment data
   * @return the memory fraction
   */
  private static double getStorageMemoryFraction(SparkEnvironmentData data) {
    String ratio = data.getSparkProperty(SPARK_STORAGE_MEMORY_FRACTION);
    if (ratio == null) {
      ratio = new SparkConf().get(SPARK_STORAGE_MEMORY_FRACTION, String.valueOf(DEFAULT_SPARK_STORAGE_MEMORY_FRACTION));
    }
    return Double.parseDouble(ratio);
  }

  /**
   * Get the peak storage memory used during all running time of the spark application
   *
   * @param data The spark application data that contains the information
   * @return The memory in bytes
   */
  private static long getStoragePeakMemory(SparkApplicationData data) {
    SparkExecutorData executorData = data.getExecutorData();
    long mem = 0L;
    for (String id : executorData.getExecutors()) {
      mem += executorData.getExecutorInfo(id).memUsed;
    }
    return mem;
  }

  /**
   * Get the total memory allocated for storage
   *
   * @param data The spark application data that contains the information
   * @return The memory in bytes
   */
  private static long getTotalStorageMem(SparkApplicationData data) {
    SparkExecutorData executorData = data.getExecutorData();
    long totalStorageMem = 0L;
    for (String id : executorData.getExecutors()) {
      totalStorageMem += executorData.getExecutorInfo(id).maxMem;
    }
    return totalStorageMem;
  }

  public Severity getTotalMemorySeverity(long memory) {
    return Severity.getSeverityAscending(memory, totalMemLimits[0], totalMemLimits[1], totalMemLimits[2],
        totalMemLimits[3]);
  }

  private Severity getMemoryUtilizationSeverity(long peakMemory, long totalStorageMemory) {
    double fraction = peakMemory * 1.0 / totalStorageMemory;
    if (totalStorageMemory < MemoryFormatUtils.stringToBytes("500 GB")) {
      return Severity.NONE;
    } else {
      return Severity.getSeverityDescending(
          fraction, memUtilLimits[0], memUtilLimits[1], memUtilLimits[2], memUtilLimits[3]);
    }
  }
}
