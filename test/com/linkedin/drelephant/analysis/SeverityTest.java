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

package com.linkedin.drelephant.analysis;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SeverityTest {

  @Test
  public void testSeverityMax() {
    assertEquals(Severity.CRITICAL, Severity.max(Severity.CRITICAL));
    assertEquals(Severity.CRITICAL, Severity.max(Severity.CRITICAL, Severity.SEVERE));
    assertEquals(Severity.CRITICAL, Severity.max(Severity.LOW, Severity.LOW, Severity.CRITICAL));
  }

  @Test
  public void testSeverityMin() {
    assertEquals(Severity.NONE, Severity.min(Severity.NONE, Severity.LOW));
    assertEquals(Severity.LOW, Severity.min(Severity.LOW, Severity.LOW));
  }

  @Test
  public void testSeverityAscending() {
    assertEquals(Severity.CRITICAL, Severity.getSeverityAscending(8, 2, 4, 6, 8));
    assertEquals(Severity.SEVERE, Severity.getSeverityAscending(10, 2, 4, 6, 12));
  }

  @Test
  public void testSeverityDescending() {
    assertEquals(Severity.CRITICAL, Severity.getSeverityDescending(2, 10, 8, 4, 2));
    assertEquals(Severity.MODERATE, Severity.getSeverityDescending(5, 10, 8, 4, 2));
  }
}
