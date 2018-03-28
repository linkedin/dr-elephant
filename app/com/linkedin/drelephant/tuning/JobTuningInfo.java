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

import models.TuningParameter;
import models.JobDefinition;
import java.util.List;


/**
 * This class holds the tuning information for the job.
 */
class JobTuningInfo {

  private JobDefinition _tuningJob;

  /**
   * Comprises of:
   *  - archive: best configuration encountered
   *  - prev_population: pen-ultimate configuration
   *  - current_population: current configuration
   *  - rnd_state: state of random number generator
   */
  private String _tunerState;
  private List<TuningParameter> _parametersToTune;

  /**
   * Sets the job being tuned
   * @param tuningJob Job
   */
  public void setTuningJob(JobDefinition tuningJob) {
    this._tuningJob = tuningJob;
  }

  /**
   * Returns the job being tuned
   * @return Job
   */
  public JobDefinition getTuningJob() {
    return _tuningJob;
  }

  /**
   * Sets the string tuner state
   * @param stringTunerState String tuner state
   */
  public void setTunerState(String stringTunerState) {
    this._tunerState = stringTunerState;
  }

  /**
   * Returns string tuner state
   * @return String tuner state
   */
  public String getTunerState() {
    return _tunerState;
  }

  /**
   * Sets parameters to tune
   * @param parameters Parameters to tune
   */
  public void setParametersToTune(List<TuningParameter> parameters) {
    this._parametersToTune = parameters;
  }

  /**
   * Returns parameters to tune
   * @return Parameters to tune
   */
  public List<TuningParameter> getParametersToTune() {
    return _parametersToTune;
  }
}
