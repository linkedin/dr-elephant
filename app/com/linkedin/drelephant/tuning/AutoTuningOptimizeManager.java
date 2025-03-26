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

import java.util.List;
import java.util.Map;
import models.AppResult;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;

/*
Class should extend interface to incorporate optimization in Tuning  algorithm
 */

public interface AutoTuningOptimizeManager {
  /*
    Intialize any prequisite require for Optimizer
    Calls once in lifetime of the flow
   */
  public void intializePrerequisite(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet);

  /*
    Extract parameter Information of previous executions
    calls after each exectuion of flow
   */
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults);

  /*
    Optimize search space
    call after each execution of flow
   */
  public void parameterOptimizer(Integer jobID);

  /*
    apply Intelligence on Parameter.
    calls after swarm size number of executions
   */
  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job);

  /*
    Constraint violation check on optimizations
    calls after swarm size number of executions
   */
  public int numberOfConstraintsViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList);

  /*
  Swarm size depends algorithm
   */
  public int getSwarmSize();
}
