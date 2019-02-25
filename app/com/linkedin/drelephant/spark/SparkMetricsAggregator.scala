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

package com.linkedin.drelephant.spark

import com.linkedin.drelephant.analysis.{ HadoopAggregatedData, HadoopApplicationData, HadoopMetricsAggregator }
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.{ SparkApplicationData }
import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.util.Try
import java.util.Date
import com.linkedin.drelephant.AutoTuner
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus
import com.linkedin.drelephant.ElephantContext
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.conf.Configuration

class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData)
    extends HadoopMetricsAggregator {
  import SparkMetricsAggregator._

  private val logger: Logger = Logger.getLogger(classOf[SparkMetricsAggregator])

  private val allocatedMemoryWasteBufferPercentage: Double =
    Option(aggregatorConfigurationData.getParamMap.get(ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY))
      .flatMap { value => Try(value.toDouble).toOption }
      .getOrElse(DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE)

  private val hadoopAggregatedData: HadoopAggregatedData = new HadoopAggregatedData()

  override def getResult(): HadoopAggregatedData = hadoopAggregatedData

  override def aggregate(data: HadoopApplicationData): Unit = data match {
    case (data: SparkApplicationData) => aggregate(data)
    case _ => throw new IllegalArgumentException("data should be SparkApplicationData")
  }

  private def aggregate(data: SparkApplicationData): Unit = for {
    executorMemoryBytes <- executorMemoryBytesOf(data)
  } {
    val applicationDurationMillis = applicationDurationMillisOf(data)
    if (applicationDurationMillis < 0) {
      logger.warn(s"applicationDurationMillis is negative. Skipping Metrics Aggregation:${applicationDurationMillis}")
    } else {
      var (resourcesActuallyUsed, resourcesAllocatedForUse) = calculateResourceUsage(data)
      val resourcesActuallyUsedWithBuffer = resourcesActuallyUsed.doubleValue() * (1.0 + allocatedMemoryWasteBufferPercentage)
      val resourcesWastedMBSeconds = (resourcesActuallyUsedWithBuffer < resourcesAllocatedForUse.doubleValue()) match {
        case true => resourcesAllocatedForUse.doubleValue() - resourcesActuallyUsedWithBuffer
        case false => 0.0
      }
      //allocated is the total used resource from the cluster.
      if (resourcesAllocatedForUse.isValidLong) {
        hadoopAggregatedData.setResourceUsed(resourcesAllocatedForUse.toLong)
      } else {
        logger.warn(s"resourcesAllocatedForUse/resourcesWasted exceeds Long.MaxValue")
        logger.warn(s"ResourceUsed: ${resourcesAllocatedForUse}")
        logger.warn(s"executorMemoryBytes:${executorMemoryBytes}")
        logger.warn(s"applicationDurationMillis:${applicationDurationMillis}")
        logger.warn(s"resourcesActuallyUsedWithBuffer:${resourcesActuallyUsedWithBuffer}")
        logger.warn(s"resourcesWastedMBSeconds:${resourcesWastedMBSeconds}")
        logger.warn(s"allocatedMemoryWasteBufferPercentage:${allocatedMemoryWasteBufferPercentage}")
      }
      hadoopAggregatedData.setResourceWasted(resourcesWastedMBSeconds.toLong)
    }
  }

  //calculates the resource usage by summing up the resources used per executor
  private def calculateResourceUsage(data: SparkApplicationData): (BigInt, BigInt) = {
    val executorSummaries = data.executorSummaries
    var sumResourceUsage: BigInt = 0
    var sumResourcesAllocatedForUse: BigInt = 0
    executorSummaries.foreach(
      executorSummary => {
        val memoryOverhead = overheadMemoryBytesOf(data).get
        val roundedContainerBytes = getRoundedContainerBytes(data).get
        val memUsedBytes: Long = executorSummary.peakJvmUsedMemory.getOrElse(JVM_USED_MEMORY, 0).asInstanceOf[Number].longValue + MemoryFormatUtils.stringToBytes(SPARK_RESERVED_MEMORY) + memoryOverhead
        val timeSpent: Long = executorSummary.totalDuration
        var totalCores: Int = executorSummary.totalCores
        if (totalCores == 0) {
          totalCores = 1
        }
        val bytesMillisUsed = BigInt(memUsedBytes) * timeSpent / totalCores
        val bytesMillisAllocated = BigInt(roundedContainerBytes) * timeSpent / totalCores
        sumResourcesAllocatedForUse += (bytesMillisAllocated / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
        sumResourceUsage += (bytesMillisUsed / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
      })
    (sumResourceUsage, sumResourcesAllocatedForUse)
  }

  private def aggregateresourcesAllocatedForUse(
    executorInstances: Int,
    executorMemoryBytes: Long,
    applicationDurationMillis: Long): BigInt = {
    val bytesMillis = BigInt(executorInstances) * BigInt(executorMemoryBytes) * BigInt(applicationDurationMillis)
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def executorInstancesOf(data: SparkApplicationData): Option[Int] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
  }

  private def executorMemoryBytesOf(data: SparkApplicationData): Option[Long] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)
  }

  private def applicationDurationMillisOf(data: SparkApplicationData): Long = {
    require(data.applicationInfo.attempts.nonEmpty)
    val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
    lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime
  }

  private def totalExecutorTaskTimeMillisOf(data: SparkApplicationData): BigInt = {
    data.executorSummaries.map { executorSummary => BigInt(executorSummary.totalDuration) }.sum
  }
  private def overheadMemoryBytesOf(data: SparkApplicationData): Option[Long] = {
    val executorMemory = executorMemoryBytesOf(data)
    val appConfigurationProperties = data.appConfigurationProperties
    if (appConfigurationProperties.get(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD).isEmpty) {
      val overheadMemory = executorMemory.get * (appConfigurationProperties.get(SPARK_MEMORY_OVERHEAD_MULTIPLIER_PERCENT).getOrElse(DEFAULT_SPARK_MEMORY_OVERHEAD_MULTIPLIER_PERCENT)).toInt / 100
      Option(overheadMemory)
    } else {
      appConfigurationProperties.get(SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD).map(MemoryFormatUtils.stringToBytes)
    }
  }

  private def getRoundedContainerBytes(data: SparkApplicationData): Option[Long] = {
    val increment = getIncrementBytes()
    val executorMemory = executorMemoryBytesOf(data)
    val memoryOverHead = overheadMemoryBytesOf(data)
    val totalMemoryRequired = executorMemory.get + memoryOverHead.get
    val roundedContainerBytes = Math.ceil((totalMemoryRequired * 1.0) / increment.get) * increment.get
    Option(roundedContainerBytes.longValue())
  }

  private def getIncrementBytes(): Option[Long] = {
    val config = new Configuration
    val incrementMB = config.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB)
    Option(incrementMB * 1024 * 1024)
  }
}

object SparkMetricsAggregator {
  /** The percentage of allocated memory we expect to waste because of overhead. */
  val DEFAULT_SPARK_MEMORY_OVERHEAD_MULTIPLIER_PERCENT = "10"
  val DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE = 0.5D
  val ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY = "allocated_memory_waste_buffer_percentage"
  val SPARK_RESERVED_MEMORY: String = "300M"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"
  val SPARK_MEMORY_OVERHEAD_MULTIPLIER_PERCENT = "spark.memoryOverhead.multiplier.percent"
  val JVM_USED_MEMORY = "jvmUsedMemory"
}
