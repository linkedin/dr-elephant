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

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

/**
  * A heuristic for recommending configuration parameter values, based on metrics from the application run.
  * @param heuristicConfigurationData
  */
class ConfigurationParametersHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import JavaConverters._
  import ConfigurationParametersHeuristic._
  import ConfigurationUtils._

  // the maximum number of recommended partitions
  private val maxRecommendedPartitions = heuristicConfigurationData.getParamMap
    .getOrDefault(MAX_RECOMMENDED_PARTITIONS_KEY, DEFAULT_MAX_RECOMMENDED_PARTITIONS).toInt

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {

    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    val evaluator = new Evaluator(this, data)

    // add current configuration parameter values, and recommended parameter values to the result.
    var resultDetails = ArrayBuffer(
      new HeuristicResultDetails("Current spark.executor.memory",
        bytesToString(evaluator.sparkExecutorMemory)),
      new HeuristicResultDetails("Current spark.driver.memory",
        bytesToString(evaluator.sparkDriverMemory)),
      new HeuristicResultDetails("Current spark.executor.cores", evaluator.sparkExecutorCores.toString),
      new HeuristicResultDetails("Current spark.driver.cores", evaluator.sparkDriverCores.toString),
      new HeuristicResultDetails("Current spark.memory.fraction", evaluator.sparkMemoryFraction.toString))
    evaluator.sparkExecutorInstances.foreach { numExecutors =>
      resultDetails += new HeuristicResultDetails("Current spark.executor.instances", numExecutors.toString)
    }
    evaluator.sparkExecutorMemoryOverhead.foreach { memOverhead =>
      resultDetails += new HeuristicResultDetails("Current spark.yarn.executor.memoryOverhead",
        bytesToString(memOverhead))
    }
    evaluator.sparkDriverMemoryOverhead.foreach { memOverhead =>
      resultDetails += new HeuristicResultDetails("Current spark.yarn.driver.memoryOverhead",
        bytesToString(memOverhead))
    }

    resultDetails ++= Seq(
    new HeuristicResultDetails("Current spark.sql.shuffle.partitions", evaluator.sparkSqlShufflePartitions.toString),
      new HeuristicResultDetails("Recommended spark.executor.cores", evaluator.recommendedExecutorCores.toString),
      new HeuristicResultDetails("Recommended spark.executor.memory",
        bytesToString(evaluator.recommendedExecutorMemory)),
      new HeuristicResultDetails("Recommended spark.memory.fraction", evaluator.recommendedMemoryFraction.toString),
      new HeuristicResultDetails("Recommended spark.sql.shuffle.partitions", evaluator.recommendedNumPartitions.toString),
      new HeuristicResultDetails("Recommended spark.driver.cores", evaluator.recommendedDriverCores.toString),
      new HeuristicResultDetails("Recommended spark.driver.memory",
        bytesToString(evaluator.recommendedDriverMemory))
     )
    evaluator.recommendedExecutorInstances.foreach { numExecutors =>
      resultDetails += new HeuristicResultDetails("Recommended spark.executor.instances", numExecutors.toString)
    }
    evaluator.recommendedExecutorMemoryOverhead.foreach { memoryOverhead =>
      resultDetails += new HeuristicResultDetails("Recommended spark.yarn.executor.memoryOverhead",
        bytesToString(memoryOverhead))
    }
    evaluator.recommendedDriverMemoryOverhead.foreach { memoryOverhead =>
      resultDetails += new HeuristicResultDetails("Recommended spark.yarn.driver.memoryOverhead",
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
   import ConfigurationUtils._

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
     lazy val sparkExecutorMemory = stringToBytes(
       appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY)
         .getOrElse(SPARK_EXECUTOR_MEMORY_DEFAULT))
     lazy val sparkDriverMemory = stringToBytes(appConfigurationProperties
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
       .get(SPARK_EXECUTOR_MEMORY_OVERHEAD).map(stringToBytes(_))
     lazy val sparkDriverMemoryOverhead = appConfigurationProperties
       .get(SPARK_DRIVER_MEMORY_OVERHEAD).map(stringToBytes(_))

     // from observation of user applications, adjusting spark.memory.fraction has not had
     // much benefit, so always set to the default value.
     val recommendedMemoryFraction: Double = SPARK_MEMORY_FRACTION_DEFAULT

     // recommended executor configuration values, whose recommended values will be
     // adjusted as various metrics are analyzed. Initialize to current values.
     var recommendedNumPartitions: Int = sparkSqlShufflePartitions
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

     // check for long running tasks, and increase number of partitions if applicable
     adjustParametersForLongTasks()

     // check for execution memory spill for any stages, and adjust memory, cores, number of
     // partitions
     // (recommendedExecutorCores, recommendedExecutorMemory, recommendedNumPartitions) =
     adjustParametersForExecutionMemorySpill()

     // check for too much time in GC or OOM, and adjust memory, cores, number of partitions
     if (hasOOMorGC()) {
         adjustParametersForGCandOOM()
     } else {
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
         analysis.executionMemorySpillResult.details ++ analysis.stageGCResult.details
     }.toArray.mkString("\n")
     val stageDetails = new HeuristicResultDetails("stage details", stageDetailsStr)

     val score = stageAnalysis.map(_.getStageAnalysisResults.map(_.score).sum).sum +
       executorGcEvaluator.score + jvmUsedMemoryEvaluator.score + driverScore

     val severity = Severity.max(calculateStageSeverity(stageAnalysis), executorGcSeverity,
       jvmUsedMemoryEvaluator.severity, driverSeverity)


     /**
       * If there are any long tasks, calculate a good value for the number of partitions
       * to decrease run time.
       */
     private def adjustParametersForLongTasks() = {
       // Adjusting spark.sql.shuffle.partitions is only useful if a stage with long tasks is
       // using this value to determine the number of tasks, so check to see the number of tasks
       // match this value. Also, if the stage is reading input data, the number of tasks will be
       // determined by DaliSpark.SPLIT_SIZE, so filter out these stages as well.
       // As part of the stage analysis, information about these special cases are recorded, and
       // the information can be returned to the user, so that they can modify their application.
       if (stageAnalysis.exists(analysis => hasSignificantSeverity(analysis.longTaskResult.severity))) {
         val medianDurations = stageAnalysis.filter { stageInfo =>
           stageInfo.inputBytes == 0 && stageInfo.numTasks == sparkSqlShufflePartitions
         }.map(_.medianRunTime.map(_.toLong).getOrElse(0L))
         val maxMedianDuration = if (medianDurations.size > 0) {
           medianDurations.max
         } else {
           0L
         }
         recommendedNumPartitions = Math.max(recommendedNumPartitions,
           Math.min(DEFAULT_MAX_RECOMMENDED_PARTITIONS.toInt,
             (sparkSqlShufflePartitions * maxMedianDuration / DEFAULT_TARGET_TASK_DURATION.toLong).toInt))
       }
     }

     /**
       * Examine stages for execution memory spill, and adjust cores, memory and partitions
       * to try to keep execution memory from spilling.
       *  - Decreasing cores reduces the number of tasks running concurrently on the executor,
       *    so there is more executor memory available per task.
       *  - Increasing executor memory also proportionally increases the size of the unified
       *    memory region.
       *  - Increasing the number of partitions divides the total data across more tasks, so that
       *    there is less data (and memory needed to store it) per task.
       */
     private def adjustParametersForExecutionMemorySpill() = {

       // calculate recommended values for num partitions, executor memory and cores to
       // try to avoid/reduce spill
       if (stageAnalysis.exists { analysis =>
         hasSignificantSeverity(analysis.executionMemorySpillResult.severity)
       }) {
         // find the stage with the max amount of execution memory spill, that has tasks equal to
         // spark.sql.shuffle.partitions and no skew.
         val stagesWithSpill = stageAnalysis.filter { stageInfo =>
           !hasSignificantSeverity(stageInfo.taskSkewResult.severity) &&
             stageInfo.numTasks == sparkSqlShufflePartitions
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
                 sparkExecutorCores, sparkSqlShufflePartitions)

             // If the amount of unified memory allocated for all tasks with the recommended
             // memory value is less than the calculated needed value.
             def checkMem(modified: Boolean): Boolean = {
               calculateTotalUnifiedMemory(recommendedExecutorMemory, recommendedMemoryFraction,
                 recommendedExecutorCores, recommendedNumPartitions) < totalUnifiedMemoryNeeded
             }
             // Try incrementally adjusting the number of cores, memory, and partitions to try
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
       * Adjust cores, memory and/or partitions to reduce likelihood of OutOfMemory errors or
       * excessive time in GC.
       *  - Decreasing cores reduces the number of tasks running concurrently on the executor,
       *    so there is more executor memory available per task.
       *  - Increasing executor memory also proportionally increases memory available per task.
       *  - Increasing the number of partitions divides the total data across more tasks, so that
       *    there is less data processed (and memory needed to store it) per task.
       */
     private def adjustParametersForGCandOOM() = {
       // check if there are any stages with OOM errors, that have a non-default number
       // of tasks (adjusting the number of partitions won't hep in this case)
       val stageWithNonDefaultPartitionOOM = stageAnalysis.exists { stage =>
         hasSignificantSeverity(stage.taskFailureResult.oomSeverity) &&
           stage.numTasks != sparkSqlShufflePartitions
       }

       // check for stages with GC issues that have a non-default number of tasks.
       // There is some randomness in when GC is done, but in case of correlation,
       // avoid trying to adjust the number of partitions to fix.
       val stagesWithNonDefaultPartitionGC = stageAnalysis.exists { stage =>
         hasSignificantSeverity(stage.stageGCResult.severity) && stage.numTasks != sparkSqlShufflePartitions
       }

       // If there are stages with non-default partitions, that have OOM or GC issues,
       // avoid trying adjustments to the number of partitions, since this will not help
       // those stages.
       val adjustments = if (stageWithNonDefaultPartitionOOM || stagesWithNonDefaultPartitionGC) {
         OOM_GC_ADJUSTMENTS.filter{ adjustment =>
           adjustment match {
             case _: PartitionMultiplierAdjustment => false
             case _: PartitionSetAdjustment => false
             case _ => true
           }
         }
       } else {
         OOM_GC_ADJUSTMENTS
       }

       if (recommendedNumPartitions <= sparkSqlShufflePartitions &&
         recommendedExecutorCores >= sparkExecutorCores &&
         recommendedExecutorMemory <= sparkExecutorMemory) {
         // Configuration parameters haven't been adjusted for execution memory spill or long tasks.
         // Adjust them now to try to prevent OOM.
         adjustExecutorParameters(adjustments, (modified: Boolean) => !modified)
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
           case adjustment: CoreDivisorAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorCores)) {
               recommendedExecutorCores = adjustment.adjust(recommendedExecutorCores)
               modified = true
             }
           case adjustment: CoreSetAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorCores)) {
               recommendedExecutorCores = adjustment.adjust(recommendedExecutorCores)
               modified = true
             }
           case adjustment: MemoryMultiplierAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorMemory)) {
               recommendedExecutorMemory = adjustment.adjust(recommendedExecutorMemory)
               modified = true
             }
           case adjustment: MemorySetAdjustment =>
             if (adjustment.canAdjust(recommendedExecutorMemory)) {
               recommendedExecutorMemory = adjustment.adjust(recommendedExecutorMemory)
               modified = true
             }
           case adjustment: PartitionMultiplierAdjustment =>
             if (adjustment.canAdjust(recommendedNumPartitions)) {
               recommendedNumPartitions = adjustment.adjust(recommendedNumPartitions)
               modified = true
             }
           case adjustment: PartitionSetAdjustment =>
             if (adjustment.canAdjust(recommendedNumPartitions)) {
               recommendedNumPartitions = adjustment.adjust(recommendedNumPartitions)
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
           recommendedNumPartitions / recommendedExecutorCores, MAX_RECOMMENDED_NUM_EXECUTORS).min
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
       val overheadMemoryIncrement = 1L * GB_TO_BYTES

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

   private val REGEX_MATCHER = raw"(\d+)((?:[T|G|M|K])?B?)?"r

   /**
     * Given a memory value in bytes, convert it to a string with the unit that round to a >0 integer part.
     *
     * @param size The memory value in long bytes
     * @return The formatted string, null if
     */
   private def bytesToString(size: Long): String = {
     val (value, unit) = {
       if (size >= 2L * GB_TO_BYTES) {
         (size.asInstanceOf[Double] / GB_TO_BYTES, "GB")
       } else {
         (size.asInstanceOf[Double] / MB_TO_BYTES, "MB")
       }
     }
     s"${Math.ceil(value).toInt}${unit}"
   }

   /**
     * Convert a formatted string into a long value in bytes. If no units
     * are specified, then the default is MB.
     *
     * @param formattedString The string to convert
     * @return The bytes value
     */
   private def stringToBytes(formattedString: String): Long = {
     if (formattedString == null || formattedString.isEmpty) {
       return 0L
     }
     //handling if the string has , for eg. 1,000MB
     val regularizedString = formattedString.replace(",", "").toUpperCase
     regularizedString match {
       case REGEX_MATCHER(value, unit) =>
         val num = value.toLong
         if (unit.length == 0) {
           num * MB_TO_BYTES
         } else {
           unit.charAt(0) match {
             case 'T' => num * TB_TO_BYTES
             case 'G' => num * GB_TO_BYTES
             case 'M' => num * MB_TO_BYTES
             case 'K' => num * KB_TO_BYTES
             case 'B' => num
           }
         }
       case _ =>
         throw new IllegalArgumentException(s"Unable to parse memory size from formatted string [${formattedString}].")
     }
   }
 }