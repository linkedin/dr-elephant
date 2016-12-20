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

package com.linkedin.drelephant.analysis

import com.linkedin.drelephant.util.Utils


/**
  * A convenience case class for containing severity thresholds and calculating severity.
  */
case class SeverityThresholds(low: Number, moderate: Number, severe: Number, critical: Number, ascending: Boolean) {
  if (ascending) {
    require(low.doubleValue <= moderate.doubleValue)
    require(moderate.doubleValue <= severe.doubleValue)
    require(severe.doubleValue <= critical.doubleValue)
  } else {
    require(low.doubleValue >= moderate.doubleValue)
    require(moderate.doubleValue >= severe.doubleValue)
    require(severe.doubleValue >= critical.doubleValue)
  }

  def severityOf(value: Number): Severity = if (ascending) {
    Severity.getSeverityAscending(value, low, moderate, severe, critical)
  } else {
    Severity.getSeverityDescending(value, low, moderate, severe, critical)
  }
}

object SeverityThresholds {
  val NUM_THRESHOLDS = 4

  /** Returns a SeverityThresholds object from a Dr. Elephant configuration string parseable by Utils.getParam(String, int). */
  def parse(
    rawString: String,
    ascending: Boolean
  ): Option[SeverityThresholds] = Option(Utils.getParam(rawString, NUM_THRESHOLDS)).map { thresholds =>
    SeverityThresholds(low = thresholds(0), moderate = thresholds(1), severe = thresholds(2), critical = thresholds(3), ascending)
  }
}
