package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.SeverityThresholds

object ConfigurationUtils {
  val JVM_USED_MEMORY = "jvmUsedMemory"

  // Spark configuration parameters
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"
  val SPARK_DRIVER_MEMORY_OVERHEAD = "spark.yarn.driver.memoryOverhead"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_DRIVER_CORES = "spark.driver.cores"
  val SPARK_EXECUTOR_INSTANCES = "spark.executor.instances"
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val SPARK_MEMORY_FRACTION = "spark.memory.fraction"

  // Spark default configuration values
  val SPARK_EXECUTOR_MEMORY_DEFAULT = "1g"
  val SPARK_DRIVER_MEMORY_DEFAULT = "1g"
  val SPARK_MEMORY_OVERHEAD_PCT_DEFAULT = 0.1
  val SPARK_MEMORY_OVERHEAD_MIN_DEFAULT = 384L << 20 // 384MB
  val SPARK_EXECUTOR_CORES_DEFAULT = 1
  val SPARK_DRIVER_CORES_DEFAULT = 1
  val SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT = 200
  val SPARK_MEMORY_FRACTION_DEFAULT = 0.6

  val SPARK_RESERVED_MEMORY = 300L << 20 // 300MB

  val MILLIS_PER_MIN = 1000D * 60.0D
  val DISTRIBUTION_MEDIAN_IDX = 2
  val DISTRIBUTION_MAX_IDX = 4

  val SPARK_STAGE_EXECUTION_MEMORY_SPILL_THRESHOLD_KEY = "spark_stage_execution_memory_spill_threshold"
  val SPARK_STAGE_TASK_SKEW_THRESHOLD_KEY = "spark_stage_task_skew_threshold"
  val SPARK_STAGE_TASK_DURATION_THRESHOLD_KEY = "spark_stage_task_duration_threshold"
  val SPARK_STAGE_MAX_DATA_PROCESSED_THRESHOLD_KEY = "spark_stage_task_duration_threshold"

  val DEFAULT_TASK_DURATION_THRESHOLDS =
    SeverityThresholds(low = 2.5D * MILLIS_PER_MIN, moderate = 5.0D * MILLIS_PER_MIN,
      severe = 10.0D * MILLIS_PER_MIN, critical = 15.0D * MILLIS_PER_MIN, ascending = true)

  val DEFAULT_TASK_SKEW_THRESHOLDS =
    SeverityThresholds(low = 2, moderate = 4, severe = 8, critical = 16, ascending = true)

  val DEFAULT_EXECUTION_MEMORY_SPILL_THRESHOLDS =
    SeverityThresholds(low = 0.01D, moderate = 0.1D, severe = 0.25D, critical = 0.5D, ascending = true)

  /** The ascending severity thresholds for the ratio of JVM GC Time and executor Run Time
    *  (checking whether ratio is above normal) These thresholds are experimental and are
    *  likely to change
    */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.09D, severe = 0.1D, critical = 0.15D, ascending = true)

  val DEFAULT_MAX_DATA_PROCESSED_THRESHOLD = 5L << 40 // 5 TB

  val DEFAULT_LONG_TASK_TO_STAGE_DURATION_RATIO = 0.75

  val DEFAULT_TASK_SKEW_TASK_DURATION_MIN_THRESHOLD = 150L * 1000

  val DEFAULT_TARGET_TASK_DURATION = 150 * 1000 // 2.5 minutes

  val DEFAULT_MAX_RECOMMENDED_PARTITIONS = 4000

  val DEFAULT_FAILED_TASK_PERCENTAGE_THRESHOLD = 0.1

}