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

import com.avaje.ebean.annotation.EnumValue;


/**
 * The severities that you can use
 */
public enum Severity {
  @EnumValue("4")
  CRITICAL(4, "Critical", "danger", "#d9534f"),

  @EnumValue("3")
  SEVERE(3, "Severe", "severe", "#e4804e"),

  @EnumValue("2")
  MODERATE(2, "Moderate", "warning", "#f0ad4e"),

  @EnumValue("1")
  LOW(1, "Low", "success", "#5cb85c"),

  @EnumValue("0")
  NONE(0, "None", "success", "#5cb85c");

  private int _value;
  private String _text;
  private String _bootstrapColor;
  private String _bootstrapColorValue;

  /**
   * @param value The severity value
   * @param text The severity name
   * @param bootstrapColor The severity level for color coding
   */
  Severity(int value, String text, String bootstrapColor, String bootstrapColorValue) {
    this._value = value;
    this._text = text;
    this._bootstrapColor = bootstrapColor;
    this._bootstrapColorValue = bootstrapColorValue;
  }

  /**
   * Returns the severity level
   *
   * @return The severity value (0 to 5)
   */
  public int getValue() {
    return _value;
  }

  /**
   * Returns the Severity level Name
   *
   * @return Severity level (None, Low, Moderate, Sever, Critical)
   */
  public String getText() {
    return _text;
  }

  /**
   * Returns the severity level for color coding
   *
   * @return The severity level (color)
   */
  public String getBootstrapColor() {
    return _bootstrapColor;
  }

  public String getBootstrapColorValue() {
    return _bootstrapColorValue;
  }
  /**
   * Returns the Severity corresponding to the severity value, NONE severity otherwise
   *
   * @param value The severity values (0 to 5)
   * @return The severity
   */
  public static Severity byValue(int value) {
    for (Severity severity : values()) {
      if (severity._value == value) {
        return severity;
      }
    }
    return NONE;
  }

  /**
   * Returns the maximum of the severities
   *
   * @param a One severity
   * @param b The other severity
   * @return Max(a,b)
   */
  public static Severity max(Severity a, Severity b) {
    if (a._value > b._value) {
      return a;
    }
    return b;
  }

  /**
   * Returns the maximum of the severities in the array
   *
   * @param severities Arbitrary number of severities
   * @return Max(severities)
   */
  public static Severity max(Severity... severities) {
    Severity currentSeverity = NONE;
    for (Severity severity : severities) {
      currentSeverity = max(currentSeverity, severity);
    }
    return currentSeverity;
  }

  /**
   * Returns the minimum of the severities
   *
   * @param a One severity
   * @param b The other severity
   * @return Min(a,b)
   */
  public static Severity min(Severity a, Severity b) {
    if (a._value < b._value) {
      return a;
    }
    return b;
  }

  /**
   * Returns the severity level of the value in the given thresholds
   * low < moderate < severe < critical
   *
   * Critical when value is greater than the critical threshold
   * None when the value is less than the low threshold.
   *
   * @param value The value being tested
   * @return One of the 5 severity levels
   */
  public static Severity getSeverityAscending(Number value, Number low, Number moderate, Number severe,
      Number critical) {
    if (value.doubleValue() >= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() >= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() >= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() >= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }

  /**
   * Returns the severity level of the value in the given thresholds
   * low > moderate > severe > critical
   *
   * Critical when value is less than the critical threshold
   * None when the value is greater than the low threshold.
   *
   * @param value The value being tested
   * @return One of the 5 severity levels
   */
  public static Severity getSeverityDescending(Number value, Number low, Number moderate, Number severe,
      Number critical) {
    if (value.doubleValue() <= critical.doubleValue()) {
      return CRITICAL;
    }
    if (value.doubleValue() <= severe.doubleValue()) {
      return SEVERE;
    }
    if (value.doubleValue() <= moderate.doubleValue()) {
      return MODERATE;
    }
    if (value.doubleValue() <= low.doubleValue()) {
      return LOW;
    }
    return NONE;
  }
}
