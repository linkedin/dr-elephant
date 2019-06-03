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
package com.linkedin.drelephant.tuning.engine;

import java.util.HashMap;
import java.util.Map;

import models.AppResult;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.spark.heuristics.ConfigurationHeuristic;
import com.linkedin.drelephant.spark.heuristics.DriverHeuristic;
import com.linkedin.drelephant.spark.heuristics.ExecutorGcHeuristic;
import com.linkedin.drelephant.spark.heuristics.JvmUsedMemoryHeuristic;
import com.linkedin.drelephant.spark.heuristics.UnifiedMemoryHeuristic;
import com.linkedin.drelephant.util.MemoryFormatUtils;


/**
 * This class recommends spark parameters based on previous run heuristics. For spark it expects one
 */
public class SparkHBTParamRecommender {
  private final Logger logger = Logger.getLogger(SparkHBTParamRecommender.class);

  AppResult appResult;

  // TODos Move these to configuration
  public static final int MAX_EXECUTOR_CORE = 3;
  public static final long MAX_EXECUTOR_MEMORY = 8 * FileUtils.ONE_GB;
  public static final long MIN_EXECUTOR_MEMORY = 900 * FileUtils.ONE_MB;

  public static final long MIN_DRIVER_MEMORY = 900 * FileUtils.ONE_MB;

  public static final int CLUSTER_DEFAULT_EXECUTOR_CORE = 1;
  public static final int CLUSTER_DEFAULT_DYNAMIC_ALLOCATION_MAX_EXECUTOR = 900;
  public static final long RESERVED_MEMORY = 300 * FileUtils.ONE_MB;
  public static final long DEFAULT_MEMORY_OVERHEAD = 384 * FileUtils.ONE_MB;

  private static final long EXECUTOR_MEMORY_BUFFER_PER_CORE = 0;
  private static final long EXECUTOR_MEMORY_BUFFER_OVERALL = 0;
  private static final long FIX_MEMORY_BUFFER_OVERALL = 200 * FileUtils.ONE_MB;
  private static final long FIX_MEMORY_BUFFER_PER_CORE = 100 * FileUtils.ONE_MB;

  private static final long DRIVER_MEMORY_BUFFER = 25;

  private static final int GC_MEMORY_INCREASE = 0;
  private static final int GC_MEMORY_DECREASE = 0;

  private long maxPeakUnifiedMemory;
  private long maxPeakJVMUsedMemory;
  private long driverMaxPeakJVMUsedMemory;
  private long suggestedSparkDriverMemory;

  private long lastRunExecutorMemory;
  private int lastRunExecutorCore;
  private long lastRunExecutorMemoryOverhead;
  private long lastRunDriverMemoryOverhead;
  private Integer lastRunExecutorInstances;
  private Integer lastRunDynamicAllocationMaxExecutors;
  private Integer lastRunDynamicAllocationMinExecutors;
  private boolean lastRunDynamicAllocationEnabled;

  private Float gcRunTimeRatio;
  private Long suggestedExecutorMemory;
  private Integer suggestedCore;
  private Integer suggestedExecutorInstances;
  private Long suggestedExecutorMemoryOverhead;

  private Double suggestedMemoryFactor;
  private Long suggestedDriverMemory;

  public SparkHBTParamRecommender(AppResult appResult) {
    this.appResult = appResult;
    Map<String, String> appHeuristicsResultDetailsMap = appResult.getHeuristicsResultDetailsMap();

    maxPeakUnifiedMemory =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(UnifiedMemoryHeuristic.class
            .getCanonicalName() + "_" + UnifiedMemoryHeuristic.MAX_PEAK_UNIFIED_MEMORY_HEURISTIC_NAME()));

    maxPeakJVMUsedMemory =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(JvmUsedMemoryHeuristic.class
            .getCanonicalName() + "_" + JvmUsedMemoryHeuristic.MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME()));

    lastRunExecutorMemoryOverhead =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(ConfigurationHeuristic.class
            .getCanonicalName() + "_" + ConfigurationHeuristic.SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD()));


    String coreConfigStr =
        appHeuristicsResultDetailsMap.get(ConfigurationHeuristic.class.getCanonicalName() + "_"
            + ConfigurationHeuristic.SPARK_EXECUTOR_CORES_KEY());

    lastRunExecutorCore = CLUSTER_DEFAULT_EXECUTOR_CORE;
    try {
      lastRunExecutorCore = Integer.parseInt(coreConfigStr);
    } catch (NumberFormatException e) {
      // Do Nothing
    }

    lastRunDynamicAllocationMaxExecutors =
        getStringToIntegerParameter(appHeuristicsResultDetailsMap, ConfigurationHeuristic.class.getCanonicalName()
            + "_" + ConfigurationHeuristic.SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS(),
            CLUSTER_DEFAULT_DYNAMIC_ALLOCATION_MAX_EXECUTOR);

    lastRunDynamicAllocationMinExecutors =
        getStringToIntegerParameter(appHeuristicsResultDetailsMap, ConfigurationHeuristic.class.getCanonicalName()
            + "_" + ConfigurationHeuristic.SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS(), null);

    lastRunExecutorInstances =
        getStringToIntegerParameter(appHeuristicsResultDetailsMap, ConfigurationHeuristic.class.getCanonicalName()
            + "_" + ConfigurationHeuristic.SPARK_EXECUTOR_INSTANCES_KEY(), null);

    lastRunDynamicAllocationEnabled =
        Boolean.parseBoolean(appHeuristicsResultDetailsMap.get(ConfigurationHeuristic.class.getCanonicalName() + "_"
            + ConfigurationHeuristic.SPARK_DYNAMIC_ALLOCATION_ENABLED()));

    gcRunTimeRatio =
        Float.parseFloat(appHeuristicsResultDetailsMap.get(ExecutorGcHeuristic.class.getCanonicalName() + "_"
            + ExecutorGcHeuristic.GC_RUN_TIME_RATIO()));

    driverMaxPeakJVMUsedMemory =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(DriverHeuristic.class.getCanonicalName()
            + "_" + DriverHeuristic.DRIVER_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME()));

    lastRunDriverMemoryOverhead =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(DriverHeuristic.class.getCanonicalName()
            + "_" + DriverHeuristic.SPARK_YARN_DRIVER_MEMORY_OVERHEAD()));

    suggestedSparkDriverMemory =
        MemoryFormatUtils.stringToBytes(appHeuristicsResultDetailsMap.get(DriverHeuristic.class.getCanonicalName()
            + "_" + DriverHeuristic.SUGGESTED_SPARK_DRIVER_MEMORY_HEURISTIC_NAME()));

    if(lastRunExecutorMemoryOverhead == 0){
      lastRunExecutorMemoryOverhead = Math.max(Math.round(lastRunExecutorMemory * 0.1), DEFAULT_MEMORY_OVERHEAD);
    }

    logger.info("Following are the heuristics values for last run : ");
    logger.info("maxPeakUnifiedMemory: " + maxPeakUnifiedMemory);
    logger.info("maxPeakJVMUsedMemory: " + maxPeakJVMUsedMemory);
    logger.info("lastRunExecutorCore: " + lastRunExecutorCore);
    logger.info("driverMaxPeakJVMUsedMemory: " + driverMaxPeakJVMUsedMemory);
    logger.info("suggestedSparkDriverMemory: " + suggestedSparkDriverMemory);
    logger.info("lastRunDriverMemoryOverhead: " + lastRunDriverMemoryOverhead);
    logger.info("lastRunDynamicAllocationMaxExecutors: " + lastRunDynamicAllocationMaxExecutors);
    logger.info("lastRunDynamicAllocationMinExecutors: " + lastRunDynamicAllocationMinExecutors);
    logger.info("lastRunExecutorInstances: " + lastRunExecutorInstances);
    logger.info("lastRunDynamicAllocationEnabled: " + lastRunDynamicAllocationEnabled);
    logger.info("lastRunExecutorMemoryOverhead: " + lastRunExecutorMemoryOverhead);
  }

  private Long getStringToLongParameter(Map<String, String> appHeuristicsResultDetailsMap, String key, Long defaultValue) {
    Long parameterValue = defaultValue;
    if (appHeuristicsResultDetailsMap.containsKey(key)) {
      try {
        parameterValue = Long.parseLong(appHeuristicsResultDetailsMap.get(key));
      } catch (NumberFormatException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Number format exception for value " + appHeuristicsResultDetailsMap.get(key), e);
        }
      }
    }
    return parameterValue;
  }

  private Integer getStringToIntegerParameter(Map<String, String> appHeuristicsResultDetailsMap, String key,
      Integer defaultValue) {
    Integer parameterValue = defaultValue;
    if (appHeuristicsResultDetailsMap.containsKey(key)) {
      try {
        parameterValue = Integer.parseInt(appHeuristicsResultDetailsMap.get(key));
      } catch (NumberFormatException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Number format exception for value " + appHeuristicsResultDetailsMap.get(key), e);
        }
      }
    }
    return parameterValue;
  }

  /**
   * This method returns suggested parameter for the job. Currently it runs following parameters:
   * executor memory, core, driver memory and memory factor
   *
   * @return HashMap with property name as key and double value
   */
  public HashMap<String, Double> getHBTSuggestion() {
    HashMap<String, Double> suggestedParameters = new HashMap<String, Double>();
    try {
      suggestExecutorMemoryCore();
      suggestMemoryFactor();
      suggestDriverMemory();
      suggestExecutorInstances();
      suggestedParameters.put(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_KEY, (double) suggestedExecutorMemory
          / FileUtils.ONE_MB);
      suggestedParameters.put(SparkConfigurationConstants.SPARK_EXECUTOR_CORES_KEY, suggestedCore.doubleValue());
      if (suggestedMemoryFactor < UnifiedMemoryHeuristic.SPARK_MEMORY_FRACTION_THRESHOLD()) {
        suggestedMemoryFactor = UnifiedMemoryHeuristic.SPARK_MEMORY_FRACTION_THRESHOLD();
      }
      suggestedParameters.put(SparkConfigurationConstants.SPARK_MEMORY_FRACTION_KEY, suggestedMemoryFactor);
      suggestedParameters.put(SparkConfigurationConstants.SPARK_DRIVER_MEMORY_KEY, (double) suggestedDriverMemory
          / FileUtils.ONE_MB);
      if (suggestedExecutorInstances != null) {
        suggestedParameters.put(SparkConfigurationConstants.SPARK_EXECUTOR_INSTANCES_KEY,
            (double) suggestedExecutorInstances);
      }
      if(suggestedExecutorMemoryOverhead != 0) {
        suggestedParameters.put(SparkConfigurationConstants.SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD,
            (double) suggestedExecutorMemoryOverhead/FileUtils.ONE_MB);
      }
      logger.info("Following are the suggestions for spark parameters for app id : " + appResult.flowExecId);
      logger.info("suggestedExecutorMemory " + suggestedExecutorMemory);
      logger.info("suggestedCore " + suggestedCore);
      logger.info("suggestedMemoryFactor " + suggestedMemoryFactor);
      logger.info("suggestedDriverMemory " + suggestedDriverMemory);
      logger.info("suggestedExecutorInstances: " + suggestedExecutorInstances);
      logger.info("suggestedExecutorMemoryOverhead: " + suggestedExecutorMemoryOverhead);

    } catch (Exception e) {
      logger.error("Error in generating parameters ", e);
    }
    return suggestedParameters;
  }

  private Long getMaxPeakJVMUsedMemoryPerCore() {
    return maxPeakJVMUsedMemory / lastRunExecutorCore;
  }

  private Long getMaxPeakUnifiedMemoryPerCore() {
    return maxPeakUnifiedMemory / lastRunExecutorCore;
  }

  /**
   * Suggest executor memory and core We calculate per core peak JVM used memory. Then we start from
   * max executor core and compute memory requirement for max executor core If this is below
   * threshold of max executor core then good, otherwise reduce the core and check memory
   * requirement.
   */
  private void suggestExecutorMemoryCore() {
    boolean validSuggestion = false;
    long maxPeakJVMUsedMemoryPerCore = getMaxPeakJVMUsedMemoryPerCore();
    for (int core = MAX_EXECUTOR_CORE; core > 0; core--) {
      long currSuggestedMemory = suggestExecutorMemory(maxPeakJVMUsedMemoryPerCore, core);
      if (getMemoryIncreaseForGC() != 0) {
        currSuggestedMemory = currSuggestedMemory * (100 + getMemoryIncreaseForGC()) / 100;
      }
      //Commenting as peak jvm memory is inclusive of reserved memory
      //currSuggestedMemory = currSuggestedMemory + RESERVED_MEMORY;
      if (currSuggestedMemory < MAX_EXECUTOR_MEMORY || core == 1 || currSuggestedMemory < lastRunExecutorMemory) {
        suggestedExecutorMemory = currSuggestedMemory;
        suggestedCore = core;
        validSuggestion = true;
        break;
      }
    }
    if (!validSuggestion) {
      suggestedExecutorMemory = lastRunExecutorMemory;
      suggestedCore = lastRunExecutorCore;
    }
    if (suggestedExecutorMemory < MIN_EXECUTOR_MEMORY) {
      suggestedExecutorMemory = MIN_EXECUTOR_MEMORY;
    }
    suggestOverheadMemoryBasedOnCore();
    suggestedExecutorMemory = getRoundedExecutorMemory();
  }

  /**
   * This method is to get the rounded executor memory. We don't want to round executor memory to
   * multiple of 1GB as container size is decided by executor memory + memoryOverhead. So round the
   * executor memory + memoryOverhead and then distribute extra memory to executor memory
   *
   * @return rounded executor memory
   */
  private long getRoundedExecutorMemory() {
    long memoryOverhead = suggestedExecutorMemoryOverhead;
    if (suggestedExecutorMemoryOverhead == 0) {
      memoryOverhead = Math.max(suggestedExecutorMemory / 10, DEFAULT_MEMORY_OVERHEAD);
    }

    long roundedContainerSize = getRoundedContainerSize(suggestedExecutorMemory + memoryOverhead + FileUtils.ONE_MB);
    long executorMemory = roundedContainerSize - memoryOverhead;
    return executorMemory;
  }

  /**
   * This method is to get the rounded executor memory. We don't want to round executor memory to
   * multiple of 1GB as container size is decided by executor memory + memoryOverhead. So round the
   * executor memory + memoryOverhead and then distribute extra memory to executor memory
   *
   * @return rounded driver memory
   */
  private long getRoundedDriverMemory() {
    long memoryOverhead = lastRunDriverMemoryOverhead;
    if (lastRunDriverMemoryOverhead == 0) {
      memoryOverhead = Math.max(suggestedDriverMemory / 10, DEFAULT_MEMORY_OVERHEAD);
    }

    long roundedContainerSize = getRoundedContainerSize(suggestedDriverMemory + memoryOverhead + FileUtils.ONE_MB);
    long driverMemory =
        (long) (suggestedDriverMemory + (roundedContainerSize - suggestedDriverMemory - memoryOverhead - FileUtils.ONE_MB) * 0.9);

    return driverMemory;
  }

  /**
   * Suggest executor memory based on peak JVM used memory per core and number of core after adding
   * buffer
   *
   * @param maxPeakJVMUsedMemoryPerCore
   * @param core
   * @return suggested executor memory
   */
  private long suggestExecutorMemory(long maxPeakJVMUsedMemoryPerCore, int core) {
    long execMemorySuggestion = (maxPeakJVMUsedMemoryPerCore * (100 + EXECUTOR_MEMORY_BUFFER_PER_CORE) / 100) * core
        * (100 + EXECUTOR_MEMORY_BUFFER_OVERALL) / 100;
    execMemorySuggestion += (core * FIX_MEMORY_BUFFER_PER_CORE) + FIX_MEMORY_BUFFER_OVERALL;
    return execMemorySuggestion;
  }

  /**
   * Suggest memory factor using peak unified memory per core and number of core after adding buffer
   */
  private void suggestMemoryFactor() {
    long unifiedMemoryRequirement =
        (getMaxPeakUnifiedMemoryPerCore() * (100 + EXECUTOR_MEMORY_BUFFER_PER_CORE) / 100) * suggestedCore
            * (100 + EXECUTOR_MEMORY_BUFFER_OVERALL) / 100;
    suggestedMemoryFactor = unifiedMemoryRequirement * 1.0 / suggestedExecutorMemory;
  }

  /**
   * Suggest driver memory based on driver peak JVM used memory
   */
  private void suggestDriverMemory() {
    suggestedDriverMemory = driverMaxPeakJVMUsedMemory * (100 + DRIVER_MEMORY_BUFFER) / 100;
    if (suggestedDriverMemory < MIN_DRIVER_MEMORY) {
      suggestedDriverMemory = MIN_DRIVER_MEMORY;
    }
    suggestedDriverMemory += RESERVED_MEMORY;
    suggestedDriverMemory = getRoundedDriverMemory();
  }

  /**
   * Rounded container size in multiple of 1GB
   *
   * @param memory
   * @return rounded container size
   */
  private long getRoundedContainerSize(long memory) {
    return ((long) Math.ceil(memory * 1.0 / FileUtils.ONE_GB)) * FileUtils.ONE_GB;
  }

  private int getMemoryIncreaseForGC() {
    if (gcRunTimeRatio > ExecutorGcHeuristic.DEFAULT_GC_SEVERITY_A_THRESHOLDS().moderate().floatValue()) {
      return GC_MEMORY_INCREASE;
    } else if (gcRunTimeRatio < ExecutorGcHeuristic.DEFAULT_GC_SEVERITY_D_THRESHOLDS().moderate().floatValue()) {
      return GC_MEMORY_DECREASE;
    } else {
      return 0;
    }
  }

  private void suggestExecutorInstances() {
    if (suggestedCore != lastRunExecutorCore) {
      suggestExecutorInstancesBasedOnCore();
    } else {
      suggestedExecutorInstances = lastRunExecutorInstances;
    }
    if (suggestedExecutorInstances != null) {
      if (suggestedExecutorInstances > CLUSTER_DEFAULT_DYNAMIC_ALLOCATION_MAX_EXECUTOR) {
        suggestedExecutorInstances = CLUSTER_DEFAULT_DYNAMIC_ALLOCATION_MAX_EXECUTOR;
      }
      //job fails if max executors is less than executor instances
      if (lastRunDynamicAllocationMaxExecutors != null
          && suggestedExecutorInstances > lastRunDynamicAllocationMaxExecutors) {
        suggestedExecutorInstances = lastRunDynamicAllocationMaxExecutors;
      }
    }
  }

  private void suggestExecutorInstancesBasedOnCore() {
    if (lastRunExecutorInstances != null) {
      suggestedExecutorInstances =
          (int) Math.ceil(lastRunExecutorInstances.doubleValue() * lastRunExecutorCore / suggestedCore);
    }
  }
  private void suggestOverheadMemoryBasedOnCore() {
    if (lastRunExecutorMemoryOverhead != 0) {
      suggestedExecutorMemoryOverhead =
          (long) Math.ceil(lastRunExecutorMemoryOverhead * 1.0 * suggestedCore / lastRunExecutorCore);
    }
  }
}
