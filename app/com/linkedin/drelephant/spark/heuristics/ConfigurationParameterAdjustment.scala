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

/**
  * Adjustments to configuration parameters for fixing flagged issues.
  */
private[heuristics] sealed trait ConfigurationParameterAdjustment[T] {

  /**
    * Determine if the value should be adjusted.
    *
    * @param value the value to adjust.
    * @return true if the value should be adjusted, false otherwise.
    */
  def canAdjust(value: T): Boolean

  /** Adjust the value.
    *
    * @param value the value to adjust.
    * @return the adjusted recommended value.
    */
  def adjust(value: T): T
}

/** Adjustement for number of cores */
private[heuristics] trait CoreAdjustment extends ConfigurationParameterAdjustment[Int]

/** Adjustment for amount of memory */
private[heuristics] trait MemoryAdjustment extends ConfigurationParameterAdjustment[Long]

/** If the number of cores is greater than the threshold, then divide by divisor. */
private[heuristics] case class CoreDivisorAdjustment(
    threshold: Int,
    divisor: Double) extends CoreAdjustment {
  override def canAdjust(numCores: Int): Boolean =  (numCores > threshold)
  override def adjust(numCores: Int): Int = Math.ceil(numCores / divisor).toInt
}

/** Set the number of cores to threshold, if the number of cores is greater. */
private[heuristics] case class CoreSetAdjustment(
    threshold: Int) extends CoreAdjustment {
  override def canAdjust(numCores: Int): Boolean =  (numCores > threshold)
  override def adjust(numCores: Int): Int = threshold
}

/** If the memory is less than the threshold, then multiply by multiplier. */
private[heuristics] case class MemoryMultiplierAdjustment(
    threshold: Long,
    multiplier: Double) extends MemoryAdjustment {
  override def canAdjust(memBytes: Long): Boolean =  (memBytes < threshold)
  override def adjust(memBytes: Long): Long = (memBytes * multiplier).toLong
}

/** If the memory is less than the threshold, then set to the theshold. */
private[heuristics] case class MemorySetAdjustment(
    threshold: Long) extends MemoryAdjustment {
  override def canAdjust(memBytes: Long): Boolean =  (memBytes < threshold)
  override def adjust(memBytes: Long): Long = threshold
}
