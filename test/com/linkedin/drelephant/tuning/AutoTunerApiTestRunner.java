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

package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import models.FlowExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import org.junit.Test;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;


public class AutoTunerApiTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initParamGenerater();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    ExecutorService executor = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 50; i++) {
      final TuningInput tuningInput = new TuningInput();
      tuningInput.setFlowExecId(1 + "");
      tuningInput.setFlowExecUrl(1 + "");
      tuningInput.setFlowDefId(i + "");
      tuningInput.setFlowDefUrl(i + "");
      final AutoTuningAPIHelper autoTuningAPIHelper = new AutoTuningAPIHelper();
      Runnable worker = new Thread() {
        public void run() {
          autoTuningAPIHelper.getFlowExecution(tuningInput);
        }
      };
      executor.execute(worker);
    }
    executor.shutdown();
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<FlowExecution> flowExecution = FlowExecution.find.where().eq(FlowExecution.TABLE.flowExecId, 1).findList();
    assertTrue(" Flow Execution ", flowExecution.size()==1);

    populateTestData();
    testJobSuggestedParamValueListToMap();
  }


  private void testJobSuggestedParamValueListToMap(){
    List<JobSuggestedParamValue> jobSuggestedParamValues = JobSuggestedParamValue.find.where()
        .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id, 1137)
        .findList();

    AutoTuningAPIHelper autoTuningAPIHelper = new AutoTuningAPIHelper();
    Map<String, Double> paramValues = autoTuningAPIHelper.jobSuggestedParamValueListToMap(jobSuggestedParamValues);
    assertTrue("Param values : " + paramValues, paramValues.isEmpty());
  }
}
