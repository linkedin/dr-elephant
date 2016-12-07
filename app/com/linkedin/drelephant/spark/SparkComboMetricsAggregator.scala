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

import com.linkedin.drelephant.analysis.{HadoopAggregatedData, HadoopApplicationData, HadoopMetricsAggregator}
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.{SparkComboApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import scala.util.Try


class SparkComboMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData)
    extends HadoopMetricsAggregator {
  import SparkComboMetricsAggregator._

  private val logger: Logger = Logger.getLogger(classOf[SparkComboMetricsAggregator])

  private val allocatedMemoryWasteBufferPercentage: Double =
    Option(aggregatorConfigurationData.getParamMap.get(ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY))
      .flatMap { value => Try(value.toDouble).toOption }
      .getOrElse(DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE)

  private val hadoopAggregatedData: HadoopAggregatedData = new HadoopAggregatedData()

  override def getResult(): HadoopAggregatedData = hadoopAggregatedData

  override def aggregate(data: HadoopApplicationData): Unit = data match {
    case (data: SparkComboApplicationData) => aggregate(data)
    case _ => throw new IllegalArgumentException("data should be SparkComboApplicationData")
  }

  private def aggregate(data: SparkComboApplicationData): Unit = {
    val (executorInstances, executorMemoryBytes) = executorInstancesAndMemoryBytesOf(data)
    val applicationDurationMillis = applicationDurationMillisOf(data)
    val totalExecutorTaskTimeMillis = totalExecutorTaskTimeMillisOf(data)

    val resourcesAllocatedMBSeconds =
      aggregateResourcesAllocatedMBSeconds(executorInstances, executorMemoryBytes, applicationDurationMillis)
    val resourcesUsedMBSeconds = aggregateResourcesUsedMBSeconds(executorMemoryBytes, totalExecutorTaskTimeMillis)

    val resourcesWastedMBSeconds =
      ((BigDecimal(resourcesAllocatedMBSeconds) * (1.0 - allocatedMemoryWasteBufferPercentage)) - BigDecimal(resourcesUsedMBSeconds))
        .toBigInt

    if (resourcesUsedMBSeconds.isValidLong) {
      hadoopAggregatedData.setResourceUsed(resourcesUsedMBSeconds.toLong)
    } else {
      logger.info(s"resourcesUsedMBSeconds exceeds Long.MaxValue: ${resourcesUsedMBSeconds}")
    }

    if (resourcesWastedMBSeconds.isValidLong) {
      hadoopAggregatedData.setResourceWasted(resourcesWastedMBSeconds.toLong)
    } else {
      logger.info(s"resourcesWastedMBSeconds exceeds Long.MaxValue: ${resourcesWastedMBSeconds}")
    }
  }

  private def aggregateResourcesUsedMBSeconds(executorMemoryBytes: Long, totalExecutorTaskTimeMillis: BigInt): BigInt = {
    val bytesMillis = BigInt(executorMemoryBytes) * totalExecutorTaskTimeMillis
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def aggregateResourcesAllocatedMBSeconds(
    executorInstances: Int,
    executorMemoryBytes: Long,
    applicationDurationMillis: Long
  ): BigInt = {
    val bytesMillis = BigInt(executorInstances) * BigInt(executorMemoryBytes) * BigInt(applicationDurationMillis)
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def executorInstancesAndMemoryBytesOf(data: SparkComboApplicationData): (Int, Long) = {
    val appConfigurationProperties = data.appConfigurationProperties
    val executorInstances = appConfigurationProperties("spark.executor.instances").toInt
    val executorMemoryBytes = MemoryFormatUtils.stringToBytes(appConfigurationProperties("spark.executor.memory"))
    (executorInstances, executorMemoryBytes)
  }

  private def applicationDurationMillisOf(data: SparkComboApplicationData): Long = {
    require(data.applicationInfo.attempts.nonEmpty)
    val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
    lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime
  }

  private def totalExecutorTaskTimeMillisOf(data: SparkComboApplicationData): BigInt = {
    data.executorSummaries.map { executorSummary => BigInt(executorSummary.totalDuration) }.sum
  }
}

object SparkComboMetricsAggregator {
  /** The percentage of allocated memory we expect to waste because of overhead. */
  val DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE = 0.5D

  val ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY = "allocated_memory_waste_buffer_percentage"
}
