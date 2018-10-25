package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{HeuristicResult, Severity}
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities._
import com.linkedin.drelephant.spark.heuristics.ConfigurationUtils._
import org.scalatest.{FunSpec, Matchers}

class ConfigurationParametersHeuristicTest extends FunSpec with Matchers {
  describe(".apply") {
    it("unused executor memory, increase executor cores") {
      // executor and driver memory are both over provisioned, increase executor
      // cores and decrease driver memory
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).create(),
        StageBuilder(2, 200).create(),
        StageBuilder(3, 200).create(),
        StageBuilder(4, 200).create(),
        StageBuilder(5, 10).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 200, 3, 300),
        createExecutorSummary("1", 800, 3, 300),
        createExecutorSummary("2", 500, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
      SPARK_EXECUTOR_MEMORY -> "4G",
      SPARK_DRIVER_MEMORY -> "6G",
      SPARK_EXECUTOR_MEMORY_OVERHEAD -> "1G",
      SPARK_DRIVER_MEMORY_OVERHEAD -> "2G",
      SPARK_EXECUTOR_CORES -> "1",
      SPARK_DRIVER_CORES -> "1",
      SPARK_EXECUTOR_INSTANCES -> "200",
      SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
      SPARK_MEMORY_FRACTION -> "0.1"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "4GB",
        "Current spark.driver.memory" -> "6GB",
        "Current spark.executor.cores" -> "1",
        "Current spark.driver.cores" -> "1",
        "Current spark.memory.fraction" -> "0.1",
        "Current spark.executor.instances" -> "200",
        "Current spark.yarn.executor.memoryOverhead" -> "1024MB",
        "Current spark.yarn.driver.memoryOverhead" -> "2GB",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "3",
        "Recommended spark.executor.memory" -> "4GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.driver.cores" -> "1",
        "Recommended spark.driver.memory" -> "640MB",
        "Recommended spark.yarn.executor.memoryOverhead" -> "1024MB",
        "Recommended spark.yarn.driver.memoryOverhead" -> "2GB",
        "Recommended spark.executor.instances" -> "66")
      checkHeuristicResults(result, Severity.CRITICAL, 16, expectedDetails)
    }

    it("unused executor memory, decrease executor memory") {
      // executor memory and driver memory are both over provisioned, decrease both
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).create(),
        StageBuilder(2, 200).create(),
        StageBuilder(3, 200).create(),
        StageBuilder(4, 200).create(),
        StageBuilder(5, 10).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1200, 3, 300),
        createExecutorSummary("1", 800, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "4G",
        SPARK_DRIVER_MEMORY -> "6G",
        SPARK_EXECUTOR_MEMORY_OVERHEAD -> "1G",
        SPARK_DRIVER_MEMORY_OVERHEAD -> "2G",
        SPARK_EXECUTOR_CORES -> "5",
        SPARK_DRIVER_CORES -> "5",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.1"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "4GB",
        "Current spark.driver.memory" -> "6GB",
        "Current spark.executor.cores" -> "5",
        "Current spark.driver.cores" -> "5",
        "Current spark.memory.fraction" -> "0.1",
        "Current spark.executor.instances" -> "200",
        "Current spark.yarn.executor.memoryOverhead" -> "1024MB",
        "Current spark.yarn.driver.memoryOverhead" -> "2GB",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "5",
        "Recommended spark.executor.memory" -> "1550MB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "1800MB",
        "Recommended spark.yarn.executor.memoryOverhead" -> "1024MB",
        "Recommended spark.yarn.driver.memoryOverhead" -> "2GB",
        "Recommended spark.executor.instances" -> "40")
      checkHeuristicResults(result, Severity.CRITICAL, 17, expectedDetails)
    }

    it("long and skewed tasks, increase partitions") {
      // There are both skewed tasks and long tasks, increase the number of partitions
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(500, 5300, 15000).create(),
        StageBuilder(2, 200).taskRuntime(600, 600, 120000).create(),
        StageBuilder(3, 200).taskRuntime(300, 800, 120000).create(),
        StageBuilder(4, 200).taskRuntime(20, 30, 5000).create(),
        StageBuilder(5, 10).taskRuntime(1000, 1000, 10000).create()

      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails =
        "Stage 1 has skew in task run time (median is 8.33 min, max is 1.47 hr).\n" +
        "Stage 1: please try to modify the application to make the partitions more even.\n" +
        "Stage 1 has a long median task run time of 8.33 min.\n" +
        "Stage 1 has 3 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 1: please increase the number of partitions.\n" +
        "Stage 2 has a long median task run time of 10.00 min.\n" +
        "Stage 2 has 200 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 3 has a long median task run time of 5.00 min.\n" +
        "Stage 3 has 200 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 5 has a long median task run time of 16.67 min.\n" +
        "Stage 5 has 10 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 5: please increase the number of partitions."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "2GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "800",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "200",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 1055, expectedDetails)
    }

    it("OOM and long tasks, increase partitions") {
      // There are both failed tasks and long tasks, increase the number of partitions
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(500, 5300, 15000).create(),
        StageBuilder(2, 200).taskRuntime(600, 600, 120000).create(),
        StageBuilder(3, 200).taskRuntime(300, 800, 120000).create(),
        StageBuilder(4, 200).taskRuntime(20, 30, 5000).failures(20, 20, 0).create(),
        StageBuilder(5, 10).taskRuntime(1000, 1000, 10000).create()

      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails =
        "Stage 1 has skew in task run time (median is 8.33 min, max is 1.47 hr).\n" +
          "Stage 1: please try to modify the application to make the partitions more even.\n" +
          "Stage 1 has a long median task run time of 8.33 min.\n" +
          "Stage 1 has 3 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
          "Stage 1: please increase the number of partitions.\n" +
          "Stage 2 has a long median task run time of 10.00 min.\n" +
          "Stage 2 has 200 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
          "Stage 3 has a long median task run time of 5.00 min.\n" +
          "Stage 3 has 200 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
          "Stage 4 has 20 failed tasks.\n" +
          "Stage 4 has 20 tasks that failed because of OutOfMemory exception.\n" +
          "Stage 5 has a long median task run time of 16.67 min.\n" +
          "Stage 5 has 10 tasks, 0 B input, 0 B shuffle read, 0 B shuffle write, and 0 B output.\n" +
          "Stage 5: please increase the number of partitions."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "2GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "800",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "200",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 1095, expectedDetails)
    }

    it("OOM, double memory") {
      // tasks failed with OOM, since executor memory is low (2G), double the memory
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).failures(5, 5, 0).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "20",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has 5 failed tasks.\n" +
        "Stage 2 has 5 tasks that failed because of OutOfMemory exception."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "20",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "4GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "5",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 20, expectedDetails)
    }

    it("OOM, increase memory by half") {
      // tasks failed with OOM, since executor memory is moderate (6G), increase memory by .5
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).failures(5, 5, 0).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 5000, 3, 300),
        createExecutorSummary("2", 5500, 3, 300),
        createExecutorSummary("3", 3000, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "6G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "20",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has 5 failed tasks.\n" +
        "Stage 2 has 5 tasks that failed because of OutOfMemory exception."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "6GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "20",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "9GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "5",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 20, expectedDetails)
    }

    it("OOM, increase partitions") {
      // tasks failed with OOM, since executor memory is higher (9G), increase partitions
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).failures(5, 5, 0).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 8000, 3, 300),
        createExecutorSummary("2", 6000, 3, 300),
        createExecutorSummary("3", 6000, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "9G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "20",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has 5 failed tasks.\n" +
        "Stage 2 has 5 tasks that failed because of OutOfMemory exception."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "9GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "20",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "9GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "40",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "10",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 20, expectedDetails)
    }

    it("OOM, non-default partitions, decrease cores") {
      // tasks failed with OOM, since executor memory is higher (9G), decrease cores
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).failures(5, 5, 0).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 8000, 3, 300),
        createExecutorSummary("2", 6000, 3, 300),
        createExecutorSummary("3", 6000, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "9G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "20",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 3 has 5 failed tasks.\n" +
        "Stage 3 has 5 tasks that failed because of OutOfMemory exception."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "9GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "20",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "2",
        "Recommended spark.executor.memory" -> "9GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "10",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 20, expectedDetails)
    }

    it("OOM, non-default partitions, increase memory by 0.25") {
      // tasks failed with OOM, since executor memory is higher (9G), increase memory 25%
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).failures(5, 5, 0).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 8000, 3, 300),
        createExecutorSummary("2", 6000, 3, 300),
        createExecutorSummary("3", 6000, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "9G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "2",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "20",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 3 has 5 failed tasks.\n" +
        "Stage 3 has 5 tasks that failed because of OutOfMemory exception."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "9GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "2",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "20",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "2",
        "Recommended spark.executor.memory" -> "12GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "10",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 20, expectedDetails)
    }

    it("Container killed by yarn") {
      // tasks failed with container killed by YARN, increase memory overhead
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 60, 15000).create(),
        StageBuilder(2, 20).taskRuntime(60, 60, 1200).create(),
        StageBuilder(3, 10).taskRuntime(100, 100, 1000).failures(7, 0, 5).create()
      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 8000, 3, 300),
        createExecutorSummary("2", 6000, 3, 300),
        createExecutorSummary("3", 6000, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "9G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "2",
        SPARK_DRIVER_CORES -> "2",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "20",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 3 has 7 failed tasks.\n" +
        "Stage 3 has 5 tasks that failed because the container was killed by YARN for exeeding memory limits."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "9GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "2",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.executor.cores" -> "2",
        "Recommended spark.executor.memory" -> "9GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "20",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.yarn.executor.memoryOverhead" -> "1946MB",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 28, expectedDetails)
    }

    it("long tasks and execution memory spill 1") {
      // There are both long tasks and execution memory spill, increasing the number of
      // partitions for long tasks to 800 is sufficient to fix execution memory spill as well
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 50, 1500).create(),
        StageBuilder(2, 200).taskRuntime(600, 600, 120000).shuffleRead(500, 600, 12000).spill(50, 60, 1200).create(),
        StageBuilder(3, 200).taskRuntime(30, 80, 1200).create(),
        StageBuilder(4, 10).taskRuntime(100, 100, 1000).create())
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has a long median task run time of 10.00 min.\n" +
        "Stage 2 has 200 tasks, 0 B input, 11.72 GB shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 2 has 1.17 GB execution memory spill."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "2GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "800",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "200",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.SEVERE, 1000, expectedDetails)
    }

    it("long tasks and execution memory spill 2") {
      // There are both long tasks and execution memory spill, increase the number of
      // partitions and executor memory.
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 50, 1500).create(),
        StageBuilder(2, 200).taskRuntime(90, 90, 1800).shuffleRead(500, 600, 12000).spill(500, 600, 600000).create(),
        StageBuilder(3, 200).taskRuntime(900, 900, 180000).shuffleRead(500, 600, 12000).spill(50, 60, 1200).create(),
        StageBuilder(4, 10).taskRuntime(100, 100, 1000).create())
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has 585.94 GB execution memory spill.\n" +
        "Stage 3 has a long median task run time of 15.00 min.\n" +
        "Stage 3 has 200 tasks, 0 B input, 11.72 GB shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 3 has 1.17 GB execution memory spill."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "4GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "1200",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "200",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 2000, expectedDetails)
    }

    it("long tasks and execution memory spill 3") {
      // There are both long tasks and execution memory spill, increase partitions
      // and memory, and decrease cores.
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 50, 1500).create(),
        StageBuilder(2, 200).taskRuntime(60, 60, 1200).shuffleRead(500, 600, 12000).spill(500, 600, 1200000).create(),
        StageBuilder(3, 200).taskRuntime(600, 600, 120000).shuffleRead(500, 600, 12000).spill(50, 60, 1200).create(),
        StageBuilder(4, 10).taskRuntime(100, 100, 1000).create())
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 2 has 1.14 TB execution memory spill.\n" +
        "Stage 3 has a long median task run time of 10.00 min.\n" +
        "Stage 3 has 200 tasks, 0 B input, 11.72 GB shuffle read, 0 B shuffle write, and 0 B output.\n" +
        "Stage 3 has 1.17 GB execution memory spill."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "3",
        "Recommended spark.executor.memory" -> "6GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "2000",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "266",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 1800, expectedDetails)
    }

    it("execution memory spill 1") {
      // Execution memory spill for stages 2 and 3, stage 2 is processing a lot of data.
      // Values should be calculated for stage 2, but do not flag stage 2.
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 50, 1500).create(),
        StageBuilder(2, 200).taskRuntime(60, 60, 1200).shuffleRead(50000, 60000, 4000000).spill(500, 600, 4000000).create(),
        StageBuilder(3, 200).taskRuntime(60, 60, 1200).shuffleRead(500, 600, 12000).spill(50, 60, 1200).create(),
        StageBuilder(4, 10).taskRuntime(100, 100, 1000).create()

      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails =
        "Stage 2: a large amount of data is being processesd. Examine the application to see if this can be reduced.\n" +
          "Stage 2 has 3.81 TB execution memory spill.\n" +
          "Stage 2 has 200 tasks, 0 B input read, 3.81 TB shuffle read, 0 B shuffle write, 0 B output.\n" +
          "Stage 2 has median task values: 500 MB memory spill, 0 B input, 48.83 GB shuffle read, 0 B shuffle write, 0 B output.\n" +
          "Stage 3 has 1.17 GB execution memory spill."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "2",
        "Recommended spark.executor.memory" -> "8GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "2000",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "400",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.MODERATE, 400, expectedDetails)
    }

    it("execution memory spill 2") {
      // Execution memory spill for stages 1 and 2, where stage 1 has different number of partitions.
      val heuristicConfigurationData = createHeuristicConfigurationData()
      val stages = Seq(
        StageBuilder(1, 3).taskRuntime(50, 50, 1500).input(50000, 60000, 2000000).spill(5000, 6000, 2000000).create(),
        StageBuilder(2, 200).taskRuntime(60, 60, 1200).shuffleRead(5000, 6000, 400000).spill(500, 600, 400000).create(),
        StageBuilder(3, 200).taskRuntime(60, 60, 1200).create(),
        StageBuilder(4, 10).taskRuntime(100, 100, 1000).create()

      )
      val executors = Seq(
        createExecutorSummary("driver", 1500, 3, 300),
        createExecutorSummary("1", 1500, 3, 300),
        createExecutorSummary("2", 1000, 3, 300),
        createExecutorSummary("3", 800, 3, 300)
      )
      val properties = Map(
        SPARK_EXECUTOR_MEMORY -> "2G",
        SPARK_DRIVER_MEMORY -> "2G",
        SPARK_EXECUTOR_CORES -> "4",
        SPARK_DRIVER_CORES -> "2",
        SPARK_EXECUTOR_INSTANCES -> "200",
        SPARK_SQL_SHUFFLE_PARTITIONS -> "200",
        SPARK_MEMORY_FRACTION -> "0.6"
      )

      val data = createSparkApplicationData(stages, executors, Some(properties))

      val configurationParametersHeuristic = new ConfigurationParametersHeuristic(
        heuristicConfigurationData)
      val evaluator = new ConfigurationParametersHeuristic.Evaluator(
        configurationParametersHeuristic, data)

      val result = configurationParametersHeuristic.apply(data)
      val expectedStageDetails = "Stage 1 has 1.91 TB execution memory spill.\n" +
        "Stage 2 has 390.62 GB execution memory spill."
      val expectedDetails = Map(
        "Current spark.executor.memory" -> "2GB",
        "Current spark.driver.memory" -> "2GB",
        "Current spark.executor.cores" -> "4",
        "Current spark.driver.cores" -> "2",
        "Current spark.memory.fraction" -> "0.6",
        "Current spark.executor.instances" -> "200",
        "Current spark.sql.shuffle.partitions" -> "200",
        "Recommended spark.executor.cores" -> "4",
        "Recommended spark.executor.memory" -> "4GB",
        "Recommended spark.memory.fraction" -> "0.6",
        "Recommended spark.sql.shuffle.partitions" -> "1000",
        "Recommended spark.driver.cores" -> "2",
        "Recommended spark.driver.memory" -> "2GB",
        "Recommended spark.executor.instances" -> "200",
        "stage details" -> expectedStageDetails)
      checkHeuristicResults(result, Severity.CRITICAL, 812, expectedDetails)
    }
  }

  /**
    * Check if the calculated heuristic results match the expected values.
    *
    * @param actual the actual heuristic result.
    * @param expectedSeverity expected severity.
    * @param expectedScore expeced score.
    * @param expectedDetails expected details.
    */
  private def checkHeuristicResults(
      actual: HeuristicResult,
      expectedSeverity: Severity,
      expectedScore: Int,
      expectedDetails: Map[String, String]) = {
    actual.getSeverity should be(expectedSeverity)
    actual.getScore should be(expectedScore)
    actual.getHeuristicResultDetails.size() should be(expectedDetails.size)
    (0 until expectedDetails.size).foreach { i =>
      val actualDetail = actual.getHeuristicResultDetails.get(i)
      Some(actualDetail.getValue) should be (expectedDetails.get(actualDetail.getName))
    }
  }
}
