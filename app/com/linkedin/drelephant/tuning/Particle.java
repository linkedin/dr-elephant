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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


/**
 * Particle class represents a configuration set for a job
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Particle {

  @JsonProperty("_candidate")

  // Todo: Candidate should be a Map<String, Double>?
  // todo: rename _candidate to _configurationValues?
  private List<Double> _candidate;
  private double _fitness;
  private double _birthdate;
  private boolean _maximize;

  @JsonProperty("paramSetId")
  private Long _paramSetId;

  /**
   * Sets the configuration values
   * @param candidate Configuration values
   */
  public void setCandidate(List<Double> candidate) {
    this._candidate = candidate;
  }

  /**
   * Returns the configuration values
   * @return Configuration values
   */
  public List<Double> getCandidate() {
    return _candidate;
  }

  /**
   * Sets fitness of the configuration
   * @param fitness Fitness
   */
  public void setFitness(double fitness) {
    this._fitness = fitness;
  }

  /**
   * Returns the fitness of the configuration
   * @return fitness
   */
  public double getFitness() {
    return _fitness;
  }

  /**
   * Sets the birthdate of the configuration
   * @param birthDate Birthdate
   */
  public void setBirthdate(double birthDate) {
    this._birthdate = birthDate;
  }

  /**
   * Returns the birthdate of the configuration
   * @return birth date
   */
  public double getBirthdate() {
    return _birthdate;
  }

  /**
   * Sets maximize which represents whether the objective of optimization is to maximize or minimize the fitness
   * @param maximize true if the ojective is to maximize fitness, false otherwise
   */
  public void setMaximize(boolean maximize) {
    this._maximize = maximize;
  }

  /**
   * Returns maximize
   * @return Maximize
   */
  public boolean getMaximize() {
    return _maximize;
  }

  /**
   * Sets the param Set Id
   * @param paramSetId Param Set Id
   */
  public void setPramSetId(Long paramSetId) {
    this._paramSetId = paramSetId;
  }

  /**
   * Returns the param set id
   * @return the param set id
   */
  public Long getParamSetId() {
    return _paramSetId;
  }
}
