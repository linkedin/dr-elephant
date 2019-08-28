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
import models.TuningAlgorithm;
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
    testGetCurrentRunParameters();
  }

  /**
   * Testing processParameterTuningEnabled
   */

  private void testGetCurrentRunParameters() {
    TuningInput tuningInput = new TuningInput();
    tuningInput.setFlowDefId("https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow");
    tuningInput.setJobDefId("https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry");
    tuningInput.setFlowDefUrl("https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow");
    tuningInput.setJobDefUrl("https://elephant.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry");
    tuningInput.setFlowExecId("https://elephant.linkedin.com:8443/executor?execid=5416293");
    tuningInput.setJobExecId("https://elephant.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0");
    tuningInput.setFlowExecUrl("https://elephant.linkedin.com:8443/executor?execid=5416293");
    tuningInput.setJobExecUrl("https://elephant.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0");
    tuningInput.setJobName("countByCountryFlow_countByCountry");
    tuningInput.setUserName("dukumar");
    tuningInput.setClient("azkaban");
    tuningInput.setScheduler("azkaban");
//    tuningInput.setDefaultParams(defaultParams);
    tuningInput.setVersion(1);
    tuningInput.setRetry(true);
//    tuningInput.setSkipExecutionForOptimization(skipExecutionForOptimization);
    tuningInput.setJobType("PIG");
    tuningInput.setOptimizationAlgo("HBT");
    tuningInput.setOptimizationAlgoVersion("4");
    tuningInput.setOptimizationMetric("RESOURCE");
    tuningInput.setAllowedMaxExecutionTimePercent(null);
    tuningInput.setAllowedMaxResourceUsagePercent(null);

    TuningAlgorithm tuningAlgorithm = TuningAlgorithm.find.select("*")
        .where()
        .eq(TuningAlgorithm.TABLE.jobType, tuningInput.getJobType())
        .eq(TuningAlgorithm.TABLE.optimizationMetric, "RESOURCE")
        .eq(TuningAlgorithm.TABLE.optimizationAlgo, tuningInput.getOptimizationAlgo())
        .findUnique();
    tuningInput.setTuningAlgorithm(tuningAlgorithm);

    AutoTuningAPIHelper autoTuningAPIHelper = new AutoTuningAPIHelper();
    Map<String, Double> paramValues = null;
    try {
      paramValues = autoTuningAPIHelper.getCurrentRunParameters(tuningInput);
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertTrue("Param values : " + paramValues, paramValues.isEmpty());
  }
}
