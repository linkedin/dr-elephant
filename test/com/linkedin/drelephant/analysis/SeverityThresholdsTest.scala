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

import org.scalatest.{FunSpec, Matchers}

class SeverityThresholdsTest extends FunSpec with Matchers {
  describe("SeverityThresholds") {
    it("can be used to represent thresholds considered in ascending order") {
      val thresholds = SeverityThresholds(low = 0.2D, moderate = 0.4D, severe = 0.6D, critical = 0.8D, ascending = true)
      thresholds.severityOf(0.1D) should be(Severity.NONE)
      thresholds.severityOf(0.2D) should be(Severity.LOW)
      thresholds.severityOf(0.3D) should be(Severity.LOW)
      thresholds.severityOf(0.4D) should be(Severity.MODERATE)
      thresholds.severityOf(0.5D) should be(Severity.MODERATE)
      thresholds.severityOf(0.6D) should be(Severity.SEVERE)
      thresholds.severityOf(0.7D) should be(Severity.SEVERE)
      thresholds.severityOf(0.8D) should be(Severity.CRITICAL)
      thresholds.severityOf(0.9D) should be(Severity.CRITICAL)
    }

    it("can be used to represent thresholds considered in descending order") {
      val thresholds = SeverityThresholds(low = 0.8D, moderate = 0.6D, severe = 0.4D, critical = 0.2D, ascending = false)
      thresholds.severityOf(0.1D) should be(Severity.CRITICAL)
      thresholds.severityOf(0.2D) should be(Severity.CRITICAL)
      thresholds.severityOf(0.3D) should be(Severity.SEVERE)
      thresholds.severityOf(0.4D) should be(Severity.SEVERE)
      thresholds.severityOf(0.5D) should be(Severity.MODERATE)
      thresholds.severityOf(0.6D) should be(Severity.MODERATE)
      thresholds.severityOf(0.7D) should be(Severity.LOW)
      thresholds.severityOf(0.8D) should be(Severity.LOW)
      thresholds.severityOf(0.9D) should be(Severity.NONE)
    }

    it("can be parsed as ascending thresholds from a string that can be processed by Utils.getParam") {
      SeverityThresholds.parse("0.2,0.4,0.6,0.8", ascending = true) should be(
        Some(SeverityThresholds(low = 0.2D, moderate = 0.4D, severe = 0.6D, critical = 0.8D, ascending = true))
      )
    }

    it("can be parsed as descending thresholds from a string that can be processed by Utils.getParam") {
      SeverityThresholds.parse("0.8,0.6,0.4,0.2", ascending = false) should be(
        Some(SeverityThresholds(low = 0.8D, moderate = 0.6D, severe = 0.4D, critical = 0.2D, ascending = false))
      )
    }

    it("cannot be created as ascending thresholds with unordered values") {
      an[IllegalArgumentException] should be thrownBy(
        SeverityThresholds(low = 0.8D, moderate = 0.6D, severe = 0.4D, critical = 0.2D, ascending = true)
      )
    }

    it("cannot be created as descending thresholds with unordered values") {
      an[IllegalArgumentException] should be thrownBy(
        SeverityThresholds(low = 0.2D, moderate = 0.4D, severe = 0.6D, critical = 0.8D, ascending = false)
      )
    }
  }
}
