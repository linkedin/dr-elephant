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
  public static final long MAX_EXECUTOR_MEMORY = 10 * FileUtils.ONE_GB;
  public static final long MIN_EXECUTOR_MEMORY = 900 * FileUtils.ONE_MB;
  public static final long MIN_DRIVER_MEMORY = 900 * FileUtils.ONE_MB;

  public static final int CLUSTER_DEFAULT_EXECUTOR_CORE = 1;
  public static final long RESERVED_MEMORY = 300 * FileUtils.ONE_MB;

  private static final long EXECUTOR_MEMORY_BUFFER_PER_CORE = 0;
  private static final long EXECUTOR_MEMORY_BUFFER_OVERALL = 0;
  private static final long DRIVER_MEMORY_BUFFER = 0;

  private static final int GC_MEMORY_INCREASE = 5;
  private static final int GC_MEMORY_DECREASE = -5;

  private long maxPeakUnifiedMemory;
  private long maxPeakJVMUsedMemory;
  private long driverMaxPeakJVMUsedMemory;
  private long suggestedSparkDriverMemory;

  private long lastRunExecutorMemory;
  private int lastRunExecutorCore;
  private long lastRunExecutorMemoryOverhead;
  private long lastRunDriverMemoryOverhead;
  private Float gcRunTimeRatio;
  private Long suggestedExecutorMemory;
  private Integer suggestedCore;
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

    logger.info("Following are the heuristics values for last run : ");
    logger.info("maxPeakUnifiedMemory: " + maxPeakUnifiedMemory);
    logger.info("maxPeakJVMUsedMemory: " + maxPeakJVMUsedMemory);
    logger.info("lastRunExecutorCore: " + lastRunExecutorCore);
    logger.info("driverMaxPeakJVMUsedMemory: " + driverMaxPeakJVMUsedMemory);
    logger.info("suggestedSparkDriverMemory: " + suggestedSparkDriverMemory);

  }

  public long getLastRunExecutorMemoryOverhead() {
    return lastRunExecutorMemoryOverhead;
  }

  public void setLastRunExecutorMemoryOverhead(long lastRunExecutorMemoryOverhead) {
    this.lastRunExecutorMemoryOverhead = lastRunExecutorMemoryOverhead;
  }

  public Long getSuggestedExecutorMemory() {
    return suggestedExecutorMemory;
  }

  public void setSuggestedExecutorMemory(Long suggestedExecutorMemory) {
    this.suggestedExecutorMemory = suggestedExecutorMemory;
  }

  public Integer getSuggestedCore() {
    return suggestedCore;
  }

  public void setSuggestedCore(Integer suggestedCore) {
    this.suggestedCore = suggestedCore;
  }

  public Double getSuggestedMemoryFactor() {
    return suggestedMemoryFactor;
  }

  public void setSuggestedMemoryFactor(Double suggestedMemoryFactor) {
    this.suggestedMemoryFactor = suggestedMemoryFactor;
  }

  public Long getSuggestedDriverMemory() {
    return suggestedDriverMemory;
  }

  public void setSuggestedDriverMemory(Long suggestedDriverMemory) {
    this.suggestedDriverMemory = suggestedDriverMemory;
  }

  public long getMaxPeakUnifiedMemory() {
    return maxPeakUnifiedMemory;
  }

  public void setMaxPeakUnifiedMemory(long maxPeakUnifiedMemory) {
    this.maxPeakUnifiedMemory = maxPeakUnifiedMemory;
  }

  public long getMaxPeakJVMUsedMemory() {
    return maxPeakJVMUsedMemory;
  }

  public void setMaxPeakJVMUsedMemory(long maxPeakJVMUsedMemory) {
    this.maxPeakJVMUsedMemory = maxPeakJVMUsedMemory;
  }

  public long getLastRunExecutorMemory() {
    return lastRunExecutorMemory;
  }

  public void setLastRunExecutorMemory(long lastRunExecutorMemory) {
    this.lastRunExecutorMemory = lastRunExecutorMemory;
  }

  public int getLastRunExecutorCore() {
    return lastRunExecutorCore;
  }

  public void setLastRunExecutorCore(int lastRunExecutorCore) {
    this.lastRunExecutorCore = lastRunExecutorCore;
  }

  public long getDriverMaxPeakJVMUsedMemory() {
    return driverMaxPeakJVMUsedMemory;
  }

  public void setDriverMaxPeakJVMUsedMemory(long driverMaxPeakJVMUsedMemory) {
    this.driverMaxPeakJVMUsedMemory = driverMaxPeakJVMUsedMemory;
  }

  public long getSuggestedSparkDriverMemory() {
    return suggestedSparkDriverMemory;
  }

  public void setSuggestedSparkDriverMemory(long suggestedSparkDriverMemory) {
    this.suggestedSparkDriverMemory = suggestedSparkDriverMemory;
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
      suggestedParameters.put(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_KEY, (double) suggestedExecutorMemory
          / FileUtils.ONE_MB);
      suggestedParameters.put(SparkConfigurationConstants.SPARK_EXECUTOR_CORES_KEY, suggestedCore.doubleValue());
      if (suggestedMemoryFactor < UnifiedMemoryHeuristic.SPARK_MEMORY_FRACTION_THRESHOLD()) {
        suggestedMemoryFactor = UnifiedMemoryHeuristic.SPARK_MEMORY_FRACTION_THRESHOLD();
      }
      suggestedParameters.put(SparkConfigurationConstants.SPARK_MEMORY_FRACTION_KEY, suggestedMemoryFactor);
      suggestedParameters.put(SparkConfigurationConstants.SPARK_DRIVER_MEMORY_KEY, (double) suggestedDriverMemory
          / FileUtils.ONE_MB);
      logger.info("Following are the suggestions for spark parameters for app id : " + appResult.flowExecId);
      logger.info("suggestedExecutorMemory " + suggestedExecutorMemory);
      logger.info("suggestedCore " + suggestedCore);
      logger.info("suggestedMemoryFactor " + suggestedMemoryFactor);
      logger.info("suggestedDriverMemory " + suggestedDriverMemory);
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
      if (currSuggestedMemory < MAX_EXECUTOR_MEMORY || core == 1) {
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
    if (getMemoryIncreaseForGC() != 0) {
      suggestedExecutorMemory = suggestedExecutorMemory * (100 + getMemoryIncreaseForGC()) / 100;
    }
    if (suggestedExecutorMemory < MIN_EXECUTOR_MEMORY) {
      suggestedExecutorMemory = MIN_EXECUTOR_MEMORY;
    }
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
    long memoryOverhead = lastRunExecutorMemoryOverhead;
    if (lastRunExecutorMemoryOverhead == 0) {
      memoryOverhead = Math.max(suggestedExecutorMemory / 10, 384);
    }

    long roundedContainerSize = getRoundedContainerSize(suggestedExecutorMemory + memoryOverhead + FileUtils.ONE_MB);
    long executorMemory =
        (long) (suggestedExecutorMemory + (roundedContainerSize - suggestedExecutorMemory - memoryOverhead - FileUtils.ONE_MB) * 0.9);

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
      memoryOverhead = Math.max(suggestedDriverMemory / 10, 384);
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
    return (maxPeakJVMUsedMemoryPerCore * (100 + EXECUTOR_MEMORY_BUFFER_PER_CORE) / 100) * core
        * (100 + EXECUTOR_MEMORY_BUFFER_OVERALL) / 100;
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
}
