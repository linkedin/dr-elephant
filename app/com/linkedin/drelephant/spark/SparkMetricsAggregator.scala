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
import com.linkedin.drelephant.spark.data.{SparkApplicationData, SparkLogDerivedData, SparkRestDerivedData}
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import scala.collection.Map
import scala.util.Try


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
    executorInstances <- executorInstancesOf(data)
    executorMemoryBytes <- executorMemoryBytesOf(data)
  } {
    val applicationDurationMillis = applicationDurationMillisOf(data)
    if( applicationDurationMillis < 0) {
      logger.warn(s"applicationDurationMillis is negative. Skipping Metrics Aggregation:${applicationDurationMillis}")
    }  else {
      val totalExecutorTimeMillis = totalExecutorTimeMillisOf(data)
      val totalTaskTimeMillis = totalTaskTimeMillisOf(data)

      logger.info(s"${data.getAppId}=> totalExecutorTime: ${totalExecutorTimeMillis}, totalTaskTime: ${totalTaskTimeMillis} executorMemoryBytes: ${executorMemoryBytes}")

      val resourcesAllocatedForUse = aggregateResourceUsage(executorMemoryBytes, totalExecutorTimeMillis)
      val resourcesActuallyUsed = aggregateResourceUsage(executorMemoryBytes, totalTaskTimeMillis)

      val resourcesActuallyUsedWithBuffer = resourcesActuallyUsed.doubleValue() * (1.0 + allocatedMemoryWasteBufferPercentage)
      val resourcesWastedMBSeconds =  (resourcesActuallyUsedWithBuffer < resourcesAllocatedForUse.doubleValue()) match {
        case true => resourcesAllocatedForUse.doubleValue() - resourcesActuallyUsedWithBuffer
        case false => 0.0
      }
      //allocated is the total used resource from the cluster.
      if (resourcesAllocatedForUse.isValidLong) {
        hadoopAggregatedData.setResourceUsed(resourcesAllocatedForUse.toLong)
      } else {
        logger.warn(s"resourcesAllocatedForUse/resourcesWasted exceeds Long.MaxValue")
        logger.warn(s"ResourceUsed: ${resourcesAllocatedForUse}")
        logger.warn(s"executorInstances: ${executorInstances}")
        logger.warn(s"executorMemoryBytes:${executorMemoryBytes}")
        logger.warn(s"applicationDurationMillis:${applicationDurationMillis}")
        logger.warn(s"totalExecutorTaskTimeMillis:${totalExecutorTaskTimeMillis}")
        logger.warn(s"resourcesActuallyUsedWithBuffer:${resourcesActuallyUsedWithBuffer}")
        logger.warn(s"resourcesWastedMBSeconds:${resourcesWastedMBSeconds}")
        logger.warn(s"allocatedMemoryWasteBufferPercentage:${allocatedMemoryWasteBufferPercentage}")
      }
      hadoopAggregatedData.setResourceWasted(resourcesWastedMBSeconds.toLong)
    }
  }

  private def aggregateResourceUsage(executorMemoryBytes: Long, totalExecutorTaskTimeMillis: BigInt): BigInt = {
    val bytesMillis = BigInt(executorMemoryBytes) * totalExecutorTaskTimeMillis
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def aggregateresourcesAllocatedForUse(
    executorInstances: Int,
    executorMemoryBytes: Long,
    applicationDurationMillis: Long
  ): BigInt = {
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

  private def totalExecutorTimeMillisOf(data: SparkApplicationData): BigInt = {
    data.executorSummaries.map { executorSummary => BigInt(executorSummary.totalDuration) }.sum
  }

  private def totalTaskTimeMillisOf(data: SparkApplicationData): BigInt = {
    var totalTaskTime = 0;
    data.stageDatas.map { stageData => {
      stageData.tasks match {
        case None => 0
        case _ => stageData.tasks.get.values.map( taskData => taskData.taskMetrics match {
          case None => 0
          case _ => {
            logger.info(s"${taskData.taskMetrics.get.executorRunTime} ${taskData.taskMetrics.get.executorDeserializeTime}" +
              s"${taskData.taskMetrics.get.resultSerializationTime}")
            taskData.taskMetrics.get.executorRunTime +
              taskData.taskMetrics.get.executorDeserializeTime +
              taskData.taskMetrics.get.resultSerializationTime
            }
          }).sum
        }
      }
    }.sum
  }
}

object SparkMetricsAggregator {
  /** The percentage of allocated memory we expect to waste because of overhead. */
  val DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE = 0.5D

  val ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY = "allocated_memory_waste_buffer_percentage"

  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
}
