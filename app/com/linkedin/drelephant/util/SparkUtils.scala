package com.linkedin.drelephant.util

import com.linkedin.drelephant.spark.data.SparkApplicationData
import org.apache.spark.network.util.JavaUtils

/**
  * Include some spark util methods.
  */
object SparkUtils {

  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"

  val SPARK_MASTER = "spark.master"

  val SPARK_MASTER_DEFAULT = "local[*]"

  val MEMORY_OVERHEAD_FACTOR = "spark.yarn.executor.memoryOverhead.factor"

  val MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"

  /**
    * Calculate Memory Request Resource which from configuration according to YARN mode or Standalone mode,
    * instead of coming from `getRuntime().maxMemory`. <br/>
    * In YARN mode, Memory Request Resource should be `spark.executor.memory` + `spark.yarn.executor.memoryOverhead`. <br/>
    * In Standalone mode, Memory Request Resource should be `spark.executor.memory`
    * @param data
    * @param default default value
    * @return Memory Request Resource(MB)
    */
  def getExecutorReqMemoryMB(data: SparkApplicationData, default: String) = {
    val envData = data.getEnvironmentData

    val executorMemory: Long = memoryStringToMb(envData.getSparkProperty(SPARK_EXECUTOR_MEMORY, default))

    if (isYARNMode(envData.getSparkProperty(SPARK_MASTER, SPARK_MASTER_DEFAULT))) {
      val memoryOverheadFactor = envData.getSparkProperty(MEMORY_OVERHEAD_FACTOR, "0.20").toDouble
      val memoryOverhead: Int = envData.getSparkProperty(MEMORY_OVERHEAD,
        math.max((memoryOverheadFactor * executorMemory).toInt, 384).toString).toInt
      executorMemory + memoryOverhead
    } else {
      executorMemory
    }
  }

  def isYARNMode(sparkMaster: String) = sparkMaster.startsWith("yarn")

  /**
    * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
    */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MB, because when no units are specified the unit
    // is assumed to be bytes
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

}
