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
import java.util.List;


/**
 * Holds the analysis details for each Heuristic
 */
public class HeuristicResultDetails {

  private String _name;
  private String _value;
  private String _details;

  public HeuristicResultDetails(String name, String value) {
    this(name, value, null);
  }

  public HeuristicResultDetails(String name, String value, String details) {
    this._name = name;
    this._value = value;
    this._details = details;
  }

  public String getDetails() {
    return _details;
  }

  public String getValue() {
    return _value;
  }

  public String getName() {
    return _name;
  }
}
