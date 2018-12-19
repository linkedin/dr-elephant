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

package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ExecutorSummary, TaskDataImpl}
import com.linkedin.drelephant.util.MemoryFormatUtils

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils

/**
  * A heuristic for recommending configuration parameter values, based on metrics from the application run.
  * @param heuristicConfigurationData
  */
class ConfigurationParametersHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import JavaConverters._
  import ConfigurationParametersHeuristic._
  import ConfigurationHeuristicsConstants._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {

    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    val evaluator = new Evaluator(this, data)

    // add current configuration parameter values, and recommended parameter values to the result.
    var resultDetails = ArrayBuffer(
      new HeuristicResultDetails(CURRENT_SPARK_EXECUTOR_MEMORY,
        bytesToString(evaluator.sparkExecutorMemory)),
      new HeuristicResultDetails(CURRENT_SPARK_DRIVER_MEMORY,
        bytesToString(evaluator.sparkDriverMemory)),
      new HeuristicResultDetails(CURRENT_SPARK_EXECUTOR_CORES, evaluator.sparkExecutorCores.toString),
      new HeuristicResultDetails(CURRENT_SPARK_DRIVER_CORES, evaluator.sparkDriverCores.toString),
      new HeuristicResultDetails(CURRENT_SPARK_MEMORY_FRACTION, evaluator.sparkMemoryFraction.toString))
    evaluator.sparkExecutorInstances.foreach { numExecutors =>
      resultDetails += new HeuristicResultDetails(CURRENT_SPARK_EXECUTOR_INSTANCES, numExecutors.toString)
    }
    evaluator.sparkExecutorMemoryOverhead.foreach { memOverhead =>
      resultDetails += new HeuristicResultDetails(CURRENT_SPARK_EXECUTOR_MEMORY_OVERHEAD,
        bytesToString(memOverhead))
    }
    evaluator.sparkDriverMemoryOverhead.foreach { memOverhead =>
      resultDetails += new HeuristicResultDetails(CURRENT_SPARK_DRIVER_MEMORY_OVERHEAD,
        bytesToString(memOverhead))
    }

    resultDetails ++= Seq(
      new HeuristicResultDetails(RECOMMENDED_SPARK_EXECUTOR_CORES,
        evaluator.recommendedExecutorCores.toString),
      new HeuristicResultDetails(RECOMMENDED_SPARK_EXECUTOR_MEMORY,
        bytesToString(evaluator.recommendedExecutorMemory)),
      new HeuristicResultDetails(RECOMMENDED_SPARK_MEMORY_FRACTION,
        evaluator.recommendedMemoryFraction.toString),
      new HeuristicResultDetails(RECOMMENDED_SPARK_DRIVER_CORES,
        evaluator.recommendedDriverCores.toString),
      new HeuristicResultDetails(RECOMMENDED_SPARK_DRIVER_MEMORY,
        bytesToString(evaluator.recommendedDriverMemory))
     )
    evaluator.recommendedExecutorInstances.foreach { numExecutors =>
      resultDetails += new HeuristicResultDetails(RECOMMENDED_SPARK_EXECUTOR_INSTANCES,
        numExecutors.toString)
    }
    evaluator.recommendedExecutorMemoryOverhead.foreach { memoryOverhead =>
      resultDetails += new HeuristicResultDetails(RECOMMENDED_SPARK_EXECUTOR_MEMORY_OVERHEAD,
        bytesToString(memoryOverhead))
    }
    evaluator.recommendedDriverMemoryOverhead.foreach { memoryOverhead =>
      resultDetails += new HeuristicResultDetails(RECOMMENDED_SPARK_DRIVER_MEMORY_OVERHEAD,
        bytesToString(memoryOverhead))
    }
    if (evaluator.stageDetails.getValue.length > 0) {
      resultDetails += evaluator.stageDetails
    }

    new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      evaluator.score,
      resultDetails.asJava
    )
  }
}

 object ConfigurationParametersHeuristic {
   import ConfigurationHeuristicsConstants._

   /**
     * Evaluate the metrics for a given Spark application, and determine recommended configuration
     * parameter values
     *
     * @param configurationParametersHeuristic configuration parameters heurisitc
     * @param data Spark application data
     */
   class Evaluator(
       configurationParametersHeuristic: ConfigurationParametersHeuristic,
       data: SparkApplicationData) {
     lazy val appConfigurationProperties: Map[String, String] =
       data.appConfigurationProperties

     // current configuration parameters
     lazy val sparkExecutorMemory = MemoryFormatUtils.stringToBytes(
       appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY)
         .getOrElse(SPARK_EXECUTOR_MEMORY_DEFAULT))
     lazy val sparkDriverMemory = MemoryFormatUtils.stringToBytes(appConfigurationProperties
       .get(SPARK_DRIVER_MEMORY).getOrElse(SPARK_DRIVER_MEMORY_DEFAULT))
     lazy val sparkExecutorCores = appConfigurationProperties
       .get(SPARK_EXECUTOR_CORES).map(_.toInt).getOrElse(SPARK_EXECUTOR_CORES_DEFAULT)
     lazy val sparkDriverCores = appConfigurationProperties
       .get(SPARK_DRIVER_CORES).map(_.toInt).getOrElse(SPARK_DRIVER_CORES_DEFAULT)
     lazy val sparkMemoryFraction = appConfigurationProperties
       .get(SPARK_MEMORY_FRACTION).map(_.toDouble).getOrElse(SPARK_MEMORY_FRACTION_DEFAULT)
     lazy val sparkSqlShufflePartitions = appConfigurationProperties
       .get(SPARK_SQL_SHUFFLE_PARTITIONS).map(_.toInt).getOrElse(SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT)
     lazy val sparkExecutorInstances = appConfigurationProperties
       .get(SPARK_EXECUTOR_INSTANCES).map(_.toInt)
     lazy val sparkExecutorMemoryOverhead = appConfigurationProperties
       .get(SPARK_EXECUTOR_MEMORY_OVERHEAD).map(MemoryFormatUtils.stringToBytes(_))
     lazy val sparkDriverMemoryOverhead = appConfigurationProperties
       .get(SPARK_DRIVER_MEMORY_OVERHEAD).map(MemoryFormatUtils.stringToBytes(_))

     // from observation of user applications, adjusting spark.memory.fraction has not had
     // much benefit, so always set to the default value.
     val recommendedMemoryFraction: Double = SPARK_MEMORY_FRACTION_DEFAULT

     // recommended executor configuration values, whose recommended values will be
     // adjusted as various metrics are analyzed. Initialize to current values.
     var recommendedExecutorMemory: Long = sparkExecutorMemory
     var recommendedExecutorCores: Int = sparkExecutorCores

     // TODO: adjust when there is more information about total container memory usage
     val recommendedDriverMemoryOverhead: Option[Long] = sparkDriverMemoryOverhead

     private lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
     private lazy val driver: ExecutorSummary = executorSummaries.find(_.id == "driver").getOrElse(null)

     if (driver == null) {
       throw new Exception("No driver found!")
     }

     val currentParallelism = sparkExecutorInstances.map(_ * sparkExecutorCores)

     val jvmUsedMemoryHeuristic =
       new JvmUsedMemoryHeuristic(configurationParametersHeuristic.heuristicConfigurationData)
     val jvmUsedMemoryEvaluator = new JvmUsedMemoryHeuristic.Evaluator(jvmUsedMemoryHeuristic, data)

     val executorGcHeuristic =
       new ExecutorGcHeuristic(configurationParametersHeuristic.heuristicConfigurationData)
     val executorGcEvaluator = new ExecutorGcHeuristic.Evaluator(executorGcHeuristic, data)
     val executorGcSeverity = executorGcEvaluator.severityTimeA

     val stageAnalyzer =
       new StagesAnalyzer(configurationParametersHeuristic.heuristicConfigurationData, data)
     val stageAnalysis = stageAnalyzer.getStageAnalysis()

     // check for execution memory spill for any stages, and adjust memory and cores
     adjustParametersForExecutionMemorySpill()

     // check for too much time in GC or OOM, and adjust memory and cores
     if (hasOOMorGC()) {
         adjustParametersForGCandOOM()
     } else if (!stageAnalysis.exists { stage =>
       hasSignificantSeverity(stage.executionMemorySpillResult.severity)
     }) {
       // check if executor memory can be lowered
         adjustExecutorMemory()
     }

     // check to see if the number of executor instances should be adjusted
     val recommendedExecutorInstances = calculateExecutorInstances()

     // check to see if the executor memory overhead should be adjusted
     val recommendedExecutorMemoryOverhead = calculateExecutorMemoryOverhead()

     // adjust driver configuration parameters
     val (recommendedDriverCores, recommendedDriverMemory, driverSeverity, driverScore) =
       adjustDriverParameters()

     // stage level informaton and recommendations
     private val stageDetailsStr = stageAnalysis.flatMap { analysis =>
       analysis.taskFailureResult.details ++ analysis.stageFailureResult.details ++
         analysis.taskSkewResult.details ++ analysis.longTaskResult.details ++
         analysis.executionMemorySpillResult.details
     }.toArray.mkString("\n")
     val stageDetails = new HeuristicResultDetails("stage details", stageDetailsStr)

     val score = stageAnalysis.map(_.getStageAnalysisResults.map(_.score).sum).sum +
       executorGcEvaluator.score + jvmUsedMemoryEvaluator.score + driverScore

     val severity = Severity.max(calculateStageSeverity(stageAnalysis), executorGcSeverity,
       jvmUsedMemoryEvaluator.severity, driverSeverity)

     /**
       * Examine stages for execution memory spill, and adjust cores and memory
       * to try to keep execution memory from spilling.
       *  - Decreasing cores reduces the number of tasks running concurrently on the executor,
       *    so there is more executor memory available per task.
       *  - Increasing executor memory also proportionally increases the size of the unified
       *    memory region.
       */
     private def adjustParametersForExecutionMemorySpill() = {

       // calculate recommended values for executor memory and cores to
       // try to avoid/reduce spill
       if (stageAnalysis.exists { analysis =>
         hasSignificantSeverity(analysis.executionMemorySpillResult.severity)
       }) {
         // find the stage with the max amount of execution memory spill, that has no skew.
         val stagesWithSpill = stageAnalysis.filter { stageInfo =>
           !hasSignificantSeverity(stageInfo.taskSkewResult.severity)
         }
         if (stagesWithSpill.size > 0) {
           val maxSpillStage = stagesWithSpill.maxBy(_.executionMemorySpillResult.memoryBytesSpilled)

           if (maxSpillStage.executionMemorySpillResult.memoryBytesSpilled > 0) {
             // calculate the total unified memory allocated for all tasks, plus the execution memory
             // spill -- this is roughly the amount of memory needed to keep execution data in memory.
             // Note that memoryBytesSpilled is the total amount of execution memory spilled, which could
             // be sequential, so this calculation could be higher than the actual amount needed.
             val totalUnifiedMemoryNeeded = maxSpillStage.executionMemorySpillResult.memoryBytesSpilled +
               calculateTotalUnifiedMemory(sparkExecutorMemory, sparkMemoryFraction,
                 sparkExecutorCores, maxSpillStage.numTasks)

             // If the amount of unified memory allocated for all tasks with the recommended
             // memory value is less than the calculated needed value.
             def checkMem(modified: Boolean): Boolean = {
               calculateTotalUnifiedMemory(recommendedExecutorMemory, recommendedMemoryFraction,
                 recommendedExecutorCores, maxSpillStage.numTasks) < totalUnifiedMemoryNeeded
             }
             // Try incrementally adjusting the number of cores and memory to try
             // to keep everything (allocated unified memory plus spill) in memory
             adjustExecutorParameters(STAGE_SPILL_ADJUSTMENTS, checkMem)
           }
         }
       }
     }

     /** @return true if the application has tasks that failed with OutOfMemory, or spent too much time in GC */
     private def hasOOMorGC() = {
       hasSignificantSeverity(executorGcSeverity) ||
         stageAnalysis.exists(stage => hasSignificantSeverity(stage.taskFailureResult.oomSeverity))
     }

     /**
       * Adjust cores and/or memory to reduce likelihood of OutOfMemory errors or
       * excessive time in GC.
       *  - Decreasing cores reduces the number of tasks running concurrently on the executor,
       *    so there is more executor memory available per task.
       *  - Increasing executor memory also proportionally increases memory available per task.
        *    there is less data processed (and memory needed to store it) per task.
       */
     private def adjustParametersForGCandOOM() = {
        if (recommendedExecutorCores >= sparkExecutorCores &&
         recommendedExecutorMemory <= sparkExecutorMemory) {
         // Configuration parameters haven't been adjusted for execution memory spill or long tasks.
         // Adjust them now to try to prevent OOM.
         adjustExecutorParameters(OOM_GC_ADJUSTMENTS, (modified: Boolean) => !modified)
       }
     }

     /**
       * Adjust the executor configuration parameters, according to the passed in
       * adjustment recommendations.
       *
       * @param adjustments the list of adjustments to try.
       * @param continueFn Given if the last adjustment was applied, return whether or not
       *                   adjustments should continue (true) or terminate (false)
       */
     private def adjustExecutorParameters(
         adjustments: Seq[ConfigurationParameterAdjustment[_>: Int with Long <: AnyVal]],
         continueFn: (Boolean) => Boolean) = {
       var modified = false
       val iter = adjustments.iterator

       while (iter.hasNext && continueFn(modified)) {
         iter.next() match {
           case adjustment: CoreAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorCores)) {
               recommendedExecutorCores = adjustment.adjust(recommendedExecutorCores)
               modified = true
             }
           case adjustment: MemoryAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorMemory)) {
               recommendedExecutorMemory = adjustment.adjust(recommendedExecutorMemory)
               modified = true
             }
         }
       }
     }

     /**
       * Check if executor memory has been over allocated, compared to max peak JVM used memory.
       * If so, either increase cores to make better use of the memory, or decrease executor
       * memory.
       */
     private def adjustExecutorMemory() = {
       if (((sparkExecutorMemory > SPARK_EXECUTOR_MEMORY_THRESHOLD_LOW &&
         jvmUsedMemoryEvaluator.severity != Severity.NONE) ||
         (sparkExecutorMemory > SPARK_EXECUTOR_MEMORY_THRESHOLD_MODERATE &&
           (jvmUsedMemoryEvaluator.severity != Severity.NONE &&
             jvmUsedMemoryEvaluator.severity != Severity.LOW)))) {
         if (sparkExecutorMemory <= SPARK_EXECUTOR_MEMORY_THRESHOLD_INCREASE_CORES &&
           sparkExecutorCores < MAX_RECOMMENDED_CORES) {
           // try to increase the number of cores, so that more tasks can run in parallel, and make
           // use of the allocated memory
           val possibleCores = ((sparkExecutorMemory - JvmUsedMemoryHeuristic.reservedMemory) /
             (jvmUsedMemoryEvaluator.maxExecutorPeakJvmUsedMemory * (1 + DEFAULT_MEMORY_BUFFER_PERCENTAGE)
             / sparkExecutorCores)).toInt
           recommendedExecutorCores = Math.min(MAX_RECOMMENDED_CORES, possibleCores)
         }

         // adjust the allocated memory
         recommendedExecutorMemory = calculateRecommendedMemory(sparkExecutorMemory,
           jvmUsedMemoryEvaluator.maxExecutorPeakJvmUsedMemory, sparkExecutorCores,
           recommendedExecutorCores)
       }
     }

     /**
       * If the number of executor instances is explicitly specified, then calculate the
       * recommended number of executor instances, trying to keep the level of parallelism
       * the same.
       * @return the recommended number of executor instances.
       */
     private def calculateExecutorInstances(): Option[Int] = {
       sparkExecutorInstances.map { numExecutors =>
         Seq(numExecutors * sparkExecutorCores / recommendedExecutorCores,
           sparkSqlShufflePartitions / recommendedExecutorCores, MAX_RECOMMENDED_NUM_EXECUTORS).min
       }
     }

     /**
       * Adjust memory overhead by inceasing if any containers were killed by YARN
       * for exceeding memory limits.
       * TODO: adjust down if current value is too high, need to check historical settings
       * and/or total container memory usage
       *
       * @return the recommended value in bytes for executor memory overhead
       */
     private def calculateExecutorMemoryOverhead(): Option[Long] = {
       val overheadMemoryIncrement = 1L * FileUtils.ONE_GB

       if (stageAnalysis.exists { stage =>
         hasSignificantSeverity(stage.taskFailureResult.containerKilledSeverity)
       }) {
         val actualMemoryOverhead = sparkExecutorMemoryOverhead.getOrElse {
           Math.max(SPARK_MEMORY_OVERHEAD_MIN_DEFAULT.toLong,
             (sparkExecutorMemory * SPARK_MEMORY_OVERHEAD_PCT_DEFAULT).toLong)
         }
         Some(actualMemoryOverhead + overheadMemoryIncrement)
       } else {
         sparkExecutorMemoryOverhead
       }
     }

     /**
       * Adjust driver configuration parameters, and calculate the severity and score
       * for driver heuristics.
       *
       * @return the recommended values for driver cores, memory, severity and score
       */
     private def adjustDriverParameters(): (Int, Long, Severity, Int) = {
       val driverHeuristic =
         new DriverHeuristic(configurationParametersHeuristic.heuristicConfigurationData)
       val driverEvaluator = new DriverHeuristic.Evaluator(driverHeuristic, data)

       val driverCores = Math.min(sparkDriverCores, DEFAULT_MAX_DRIVER_CORES)

       val driverMemory = calculateRecommendedMemory(sparkDriverMemory,
         driverEvaluator.maxDriverPeakJvmUsedMemory, sparkDriverCores,
         driverCores)

       val driverSeverity = Severity.max(driverEvaluator.severityJvmUsedMemory,
         driverEvaluator.severityDriverCores)

       val driverScore = driverEvaluator.severityJvmUsedMemory.getValue +
       driverEvaluator.severityDriverCores.getValue

       (driverCores, driverMemory, driverSeverity, driverScore)
     }
   }

   /**
     * Calculate the severity for the stage level heuristics.
     *
     * @param analyses the analysis for all the stages
     * @return the stage heristics severity.
     */
   private def calculateStageSeverity(analyses: Seq[StageAnalysis]): Severity = {
     val stageSeverities = analyses.map(_.getStageAnalysisResults.maxBy(_.severity.getValue).severity)
     if (stageSeverities.size > 0) {
       stageSeverities.maxBy(_.getValue)
     } else {
       Severity.NONE
     }
   }

   /**
     * Calculate the recommended amount of memory, based on how much was used, and if there
     * are changes to the number of cores (assume memory used is proportional to number of cores).
     *
     * @param allocatedMemory allocated memory
     * @param jvmUsedMemory max JVM used memory
     * @param numCores current number of cores
     * @param recommendedCores recommended number of cores
     * @return the recommended memory in bytes.
     */
   private def calculateRecommendedMemory(
       allocatedMemory: Long,
       jvmUsedMemory: Long,
       numCores: Int,
       recommendedCores: Int): Long = {
     val calculatedMem = (jvmUsedMemory * Math.ceil(recommendedCores / numCores.toDouble) *
       (1 + DEFAULT_MEMORY_BUFFER_PERCENTAGE) + JvmUsedMemoryHeuristic.reservedMemory).toLong
     Math.min(Math.max(DEFAULT_MIN_MEMORY, calculatedMem), allocatedMemory)
   }

   /** If the severity is sigificant (not NONE or LOW). */
   private def hasSignificantSeverity(severity: Severity): Boolean = {
     severity != Severity.NONE && severity != Severity.LOW
   }

   /**
     * Calculate the total amount of unified memory allocated across all tasks for
     * the stage.
     *
     * @param executorMemory executor memory in bytes
     * @param memoryFraction spark.memory.fraction
     * @param executorCores number of executor cores
     * @param numTasks number of tasks/partitions
     * @return
     */
   private def calculateTotalUnifiedMemory(
       executorMemory: Long,
       memoryFraction: Double,
       executorCores: Int,
       numTasks: Int): Long = {
     // amount of unified memory available for each task
     val unifiedMemPerTask = (executorMemory - SPARK_RESERVED_MEMORY) * memoryFraction /
       executorCores
     (unifiedMemPerTask * numTasks).toLong
   }

    /**
     * Given a memory value in bytes, convert it to a string with the unit that round to a >0 integer part.
     *
     * @param size The memory value in long bytes
     * @return The formatted string, null if
     */
   private def bytesToString(size: Long): String = {
     val (value, unit) = {
       if (size >= 2L * FileUtils.ONE_GB) {
         (size.asInstanceOf[Double] / FileUtils.ONE_GB, "GB")
       } else {
         (size.asInstanceOf[Double] / FileUtils.ONE_MB, "MB")
       }
     }
     s"${Math.ceil(value).toInt}${unit}"
   }
 }