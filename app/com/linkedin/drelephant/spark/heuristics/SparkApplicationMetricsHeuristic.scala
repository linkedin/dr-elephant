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

package com.linkedin.drelephant.spark.heuristics

import scala.Long
import scala.collection.JavaConverters

import org.apache.commons.io.FileUtils

import com.linkedin.drelephant.analysis.Heuristic
import com.linkedin.drelephant.analysis.HeuristicResult
import com.linkedin.drelephant.analysis.HeuristicResultDetails
import com.linkedin.drelephant.analysis.Severity
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData

/**
 * A heuristic based on metrics for a Spark app's stages.
 * This heuristic reports metrics at application level like total input data size, total input records etc.
 */
class SparkApplicationMetricsHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
    extends Heuristic[SparkApplicationData] {
  import SparkApplicationMetricsHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    val resultDetails = Seq(
      new HeuristicResultDetails(TOTAL_INPUT_SIZE_IN_MB, "%.4f".format(evaluator.totalInputBytes * 1.0 / FileUtils.ONE_MB)),
      new HeuristicResultDetails(TOTAL_INPUT_RECORDS, evaluator.totalInputRecords.toString),
      new HeuristicResultDetails(TOTAL_OUTPUT_SIZE, FileUtils.byteCountToDisplaySize(evaluator.totalOutputBytes)),
      new HeuristicResultDetails(TOTAL_SHUFFLE_READ_SIZE, FileUtils.byteCountToDisplaySize(evaluator.totalShuffleReadBytes)),
      new HeuristicResultDetails(TOTAL_SHUFFLE_READ_RECORDS, evaluator.totalShuffleReadRecords.toString),
      new HeuristicResultDetails(TOTAL_SHUFFLE_WRITE_SIZE, FileUtils.byteCountToDisplaySize(evaluator.totalShuffleWriteBytes)),
      new HeuristicResultDetails(TOTAL_SHUFFLE_WRITE_RECORDS, evaluator.totalShuffleWriteRecords.toString),
      new HeuristicResultDetails(TOTAL_MEMORY_SPILL_SIZE, FileUtils.byteCountToDisplaySize(evaluator.totalMemoryBytesSpilled)),
      new HeuristicResultDetails(TOTAL_DISK_SPILL_SIZE, FileUtils.byteCountToDisplaySize(evaluator.totalDiskBytesSpilled)))

    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      Severity.NONE,
      0,
      resultDetails.asJava)

    result
  }
}

object SparkApplicationMetricsHeuristic {
  private val maxSchedulerDelayIndex = 4
  val TOTAL_INPUT_SIZE_IN_MB = "Total Input Size in MB"
  val TOTAL_INPUT_RECORDS = "Total Input Records"
  val TOTAL_OUTPUT_SIZE = "Total Output Size"
  val TOTAL_SHUFFLE_READ_SIZE = "Total Shuffle Read Size"
  val TOTAL_SHUFFLE_READ_RECORDS = "Total Shuffle Read Records"
  val TOTAL_SHUFFLE_WRITE_SIZE = "Total Shuffle Write Size"
  val TOTAL_SHUFFLE_WRITE_RECORDS = "Total Shuffle Write Records"
  val TOTAL_MEMORY_SPILL_SIZE = "Total Memory Spill Size"
  val TOTAL_DISK_SPILL_SIZE = "Total Disk Spill Size"

  class Evaluator(sparkApplicationMetricsHeuristic: SparkApplicationMetricsHeuristic, data: SparkApplicationData) {
    lazy val stageDatas: Seq[StageData] = data.stageDatas
    lazy val totalInputBytes: Long = stageDatas.map(_.inputBytes).sum
    lazy val totalInputRecords: Long = stageDatas.map(_.inputRecords).sum
    lazy val totalOutputBytes: Long = stageDatas.map(_.outputBytes).sum
    lazy val totalShuffleReadBytes: Long = stageDatas.map(_.shuffleReadBytes).sum
    lazy val totalShuffleReadRecords: Long = stageDatas.map(_.shuffleReadRecords).sum
    lazy val totalShuffleWriteBytes: Long = stageDatas.map(_.shuffleWriteBytes).sum
    lazy val totalShuffleWriteRecords: Long = stageDatas.map(_.shuffleWriteRecords).sum
    lazy val totalMemoryBytesSpilled: Long = stageDatas.map(_.memoryBytesSpilled).sum
    lazy val totalDiskBytesSpilled: Long = stageDatas.map(_.diskBytesSpilled).sum
  }
}
