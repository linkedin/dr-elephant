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

package com.linkedin.drelephant.spark.legacyheuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;


/**
 * This is a safeguard heuristic rule that makes sure if a log size passes the limit, we do not automatically
 * approve it.
 */
public class EventLogLimitHeuristic implements Heuristic<SparkApplicationData> {
  private HeuristicConfigurationData _heuristicConfData;

  public EventLogLimitHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    Severity severity = getSeverity(data);
    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, 0);
    if (severity == Severity.CRITICAL) {
      result.addResultDetail("Large Log File", "Spark job's event log passes the limit. No actual log data is fetched."
          + " All other heuristic rules will not make sense.", null);
    }
    return result;
  }

  private Severity getSeverity(SparkApplicationData data) {
    if (data.isThrottled()) {
      return Severity.CRITICAL;
    } else {
      return Severity.NONE;
    }
  }
}
