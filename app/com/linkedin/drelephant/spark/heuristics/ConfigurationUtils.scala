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
  val SPARK_EXECUTOR_CORES_DEFAULT = 1
  val SPARK_DRIVER_CORES_DEFAULT = 1
  val SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT = 200
  val SPARK_MEMORY_FRACTION_DEFAULT = 0.6

  // if the overhead memory is not explicitly specified by the user, the default amount is
  // max(0.1 * spark.executor.memory, 384MB)
  val SPARK_MEMORY_OVERHEAD_PCT_DEFAULT = 0.1

  // the minimum amount of overhead memory
  val SPARK_MEMORY_OVERHEAD_MIN_DEFAULT = 384L << 20 // 384MB

  // the amount of Spark reserved memory (300MB)
  val SPARK_RESERVED_MEMORY = 300L << 20

  // number of milliseconds in a minute
  val MILLIS_PER_MIN = 1000D * 60.0D

  // the index for the median value for executor and task metrics distributions
  val DISTRIBUTION_MEDIAN_IDX = 2

  // the index for the max value for executor and task metrics distributions
  val DISTRIBUTION_MAX_IDX = 4

  // keys for finding Dr. Elephant configuration parameter values
  val SPARK_STAGE_EXECUTION_MEMORY_SPILL_THRESHOLD_KEY = "spark_stage_execution_memory_spill_threshold"
  val SPARK_STAGE_TASK_SKEW_THRESHOLD_KEY = "spark_stage_task_skew_threshold"
  val SPARK_STAGE_TASK_DURATION_THRESHOLD_KEY = "spark_stage_task_duration_threshold"
  val SPARK_STAGE_MAX_DATA_PROCESSED_THRESHOLD_KEY = "spark_stage_task_duration_threshold"
  val TASK_FAILURE_RATE_SEVERITY_THRESHOLDS_KEY = "stage_task_failure_rate_severity_threshold"
  val MAX_DATA_PROCESSED_THRESHOLD_KEY = "execution_memory_spill_max_data_threshold"
  val LONG_TASK_TO_STAGE_DURATION_RATIO_KEY = "task_skew_task_to_stage_duration_ratio"
  val TASK_SKEW_TASK_DURATION_MIN_THRESHOLD_KEY = "task_skew_task_duration_threshold"
  val MAX_RECOMMENDED_PARTITIONS_KEY = "max_recommended_partitions"


  // Severity hresholds for task duration in minutes, when checking to see if the median task
  // run time is too long for a stage.
  val DEFAULT_TASK_DURATION_THRESHOLDS =
    SeverityThresholds(low = 2.5D * MILLIS_PER_MIN, moderate = 5.0D * MILLIS_PER_MIN,
      severe = 10.0D * MILLIS_PER_MIN, critical = 15.0D * MILLIS_PER_MIN, ascending = true)

  // Severity thresholds for checking task skew, ratio of maximum to median task run times.
  val DEFAULT_TASK_SKEW_THRESHOLDS =
    SeverityThresholds(low = 2, moderate = 4, severe = 8, critical = 16, ascending = true)

  // Severity thresholds for checking execution memory spill, ratio of exection spill compared
  // to the maximum amount of data (input, output, shuffle read, or shuffle write) processed.
  val DEFAULT_EXECUTION_MEMORY_SPILL_THRESHOLDS =
    SeverityThresholds(low = 0.01D, moderate = 0.1D, severe = 0.25D, critical = 0.5D, ascending = true)

  // The ascending severity thresholds for the ratio of JVM GC time and task run time,
  // checking if too much time is being spent in GC.
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.09D, severe = 0.1D, critical = 0.15D, ascending = true)

  /** The default severity thresholds for the rate of a stage's tasks failing. */
  val DEFAULT_TASK_FAILURE_RATE_SEVERITY_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)

  // The default threshold (5TB) for checking for maximum amount of data processed, for which to
  // alert for execution memory spill. Tasks processing more data would be expected to have some
  // amount of spill, due to the large amount of data processed.
  val DEFAULT_MAX_DATA_PROCESSED_THRESHOLD = "5TB"

  // The default threshold for the ratio of the time for longest running task for a stage to the
  // stage duration. With Spark, some amount of task skew may be OK, since exectuors can process
  // multiple tasks, so one executor could process multiple shorter tasks, while another executor
  // processes a longer task. However, if the length of the long task is a large fraction of the
  // stage duration, then it is likely contributing to the overall stage duration.
  val DEFAULT_LONG_TASK_TO_STAGE_DURATION_RATIO = "0.75"

  // Some task skew is also tolerable if the tasks are short (2.5 minutes or less).
  val DEFAULT_TASK_SKEW_TASK_DURATION_MIN_THRESHOLD = "150000"

  // The target task duration (2.5 minutes). This is the same as the idle executor timeout.
  val DEFAULT_TARGET_TASK_DURATION = "150000"

  // The default maximum number of partitions that would be recommended. More partitions means
  // less data per partition, so shorter tasks and less memory needed per task. However more
  // partitions also inceases the amount of overhead for shuffle.
  val DEFAULT_MAX_RECOMMENDED_PARTITIONS = "4000"
}