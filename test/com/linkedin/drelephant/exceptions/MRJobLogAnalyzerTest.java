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

package com.linkedin.drelephant.exceptions;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class MRJobLogAnalyzerTest {

  private String failedMRJobDiagnostic = "Task failed task_1466048666726_979739_r_000000\n"
      + "Job failed as tasks failed. failedMaps:0 failedReduces:1";

  private String killedMRJobDiagnostic = "Kill job job_1466048666726_978316 received from zfu@LINKEDIN.BIZ (auth:TOKEN) at 10.150.4.50\n"
      + "Job received Kill while in RUNNING state.";

  private MRJobLogAnalyzer analyzedFailedJobDiagnostic;
  private MRJobLogAnalyzer analyzedKilledJobDiagnostic;

  public MRJobLogAnalyzerTest(){
    analyzedFailedJobDiagnostic = new MRJobLogAnalyzer(failedMRJobDiagnostic);
    analyzedKilledJobDiagnostic = new MRJobLogAnalyzer(killedMRJobDiagnostic);
  }

  @Test
  public void getFailedSubEventsTest(){
    assertEquals(analyzedFailedJobDiagnostic.getFailedSubEvents().size(),1 );
    assertTrue(analyzedKilledJobDiagnostic.getFailedSubEvents().isEmpty());
  }

  @Test
  public void getExceptionTest(){

  }
}