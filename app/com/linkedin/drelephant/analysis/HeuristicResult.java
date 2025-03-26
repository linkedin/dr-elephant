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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;


/**
 * Holds the Heuristic analysis result Information
 */
public class HeuristicResult {
  public static final HeuristicResult NO_DATA = new HeuristicResult("NoDataReceived", "No Data Received", Severity.LOW,
      0, Collections.singletonList(new HeuristicResultDetails("No Data Received", "", null)));

  private String _heuristicClass;
  private String _heuristicName;
  private Severity _severity;
  private int _score;
  private List<HeuristicResultDetails> _heuristicResultDetails;

  /**
   * Heuristic Result Constructor
   *
   * @param heuristicClass The Heuristic class
   * @param heuristicName The name of the Heursitic
   * @param severity The severity of the result
   * @param score The computed score
   */
  public HeuristicResult(String heuristicClass, String heuristicName, Severity severity, int score) {
    this._heuristicClass = heuristicClass;
    this._heuristicName = heuristicName;
    this._severity = severity;
    this._score = score;
    this._heuristicResultDetails = new ArrayList<HeuristicResultDetails>();
  }

  /**
   * Heuristic Result Constructor
   *
   * @param heuristicClass The Heuristic class
   * @param heuristicName The name of the Heursitic
   * @param severity The severity of the result
   * @param score The computed score
   * @param heuristicResultDetails more information on the heuristic details.
   */
  public HeuristicResult(String heuristicClass, String heuristicName, Severity severity, int score,
      List<HeuristicResultDetails> heuristicResultDetails) {
    this._heuristicClass = heuristicClass;
    this._heuristicName = heuristicName;
    this._severity = severity;
    this._score = score;
    this._heuristicResultDetails = heuristicResultDetails;
  }

  /**
   * Returns the heuristic analyser class name
   *
   * @return the heursitic class name
   */
  public String getHeuristicClassName() {
    return _heuristicClass;
  }

  /**
   * Returns the heuristic analyser name
   *
   * @return the heuristic name
   */
  public String getHeuristicName() {
    return _heuristicName;
  }

  /**
   * Returns the severity of the Heuristic
   *
   * @return The severity
   */
  public Severity getSeverity() {
    return _severity;
  }

  public int getScore() {
    return _score;
  }

  /**
   * Gets a list of HeuristicResultDetails
   *
   * @return
   */
  public List<HeuristicResultDetails> getHeuristicResultDetails() {
    return _heuristicResultDetails;
  }

  /**
   * Add the App Heuristic Result Detail entry
   */
  public void addResultDetail(String name, String value, String details) {
    _heuristicResultDetails.add(new HeuristicResultDetails(name, value, details));
  }

  /**
   * Add the App Heuristic Result Detail without details
   */
  public void addResultDetail(String name, String value) {
    _heuristicResultDetails.add(new HeuristicResultDetails(name, value, null));
  }

  /**
   * Set the severity of the heuristic
   *
   * @param severity The severity to be set
   */
  public void setSeverity(Severity severity) {
    this._severity = severity;
  }

  @Override
  public String toString() {
    return "{analysis: " + _heuristicClass + ", severity: " + _severity + ", details: ["
        + StringUtils.join(_heuristicResultDetails, "    ") + "]}";
  }
}
