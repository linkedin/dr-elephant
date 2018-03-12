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

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Analyses the efficiency of Shuffle and Sort
 */
public class ShuffleSortHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ShuffleSortHeuristic.class);

  // Severity parameters.
  private static final String RUNTIME_RATIO_SEVERITY = "runtime_ratio_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] runtimeRatioLimits = {1, 2, 4, 8};       // Avg Shuffle or Sort Time * 2 / Avg Exec Time
  private double[] runtimeLimits = {1, 5, 10, 30};          // Shuffle/Sort Runtime in milli sec

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confRatioLimitsd = Utils.getParam(paramMap.get(RUNTIME_RATIO_SEVERITY), runtimeRatioLimits.length);
    if (confRatioLimitsd != null) {
      runtimeRatioLimits = confRatioLimitsd;
    }
    logger.info(heuristicName + " will use " + RUNTIME_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(runtimeRatioLimits));

    double[] confRuntimeLimits = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeLimits != null) {
      runtimeLimits = confRuntimeLimits;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public ShuffleSortHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    MapReduceTaskData[] tasks = data.getReducerData();

    List<Long> execTimeMs = new ArrayList<Long>();
    List<Long> shuffleTimeMs = new ArrayList<Long>();
    List<Long> sortTimeMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {
      if (task.isTimeDataPresent()) {
        execTimeMs.add(task.getCodeExecutionTimeMs());
        shuffleTimeMs.add(task.getShuffleTimeMs());
        sortTimeMs.add(task.getSortTimeMs());
      }
    }

    //Analyze data
    long avgExecTimeMs = Statistics.average(execTimeMs);
    long avgShuffleTimeMs = Statistics.average(shuffleTimeMs);
    long avgSortTimeMs = Statistics.average(sortTimeMs);

    Severity shuffleSeverity = getShuffleSortSeverity(avgShuffleTimeMs, avgExecTimeMs);
    Severity sortSeverity = getShuffleSortSeverity(avgSortTimeMs, avgExecTimeMs);
    Severity severity = Severity.max(shuffleSeverity, sortSeverity);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

    result.addResultDetail("Number of tasks", Integer.toString(data.getReducerData().length));
    result.addResultDetail("Average code runtime", Statistics.readableTimespan(avgExecTimeMs));
    String shuffleFactor = Statistics.describeFactor(avgShuffleTimeMs, avgExecTimeMs, "x");
    result.addResultDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTimeMs) + " " + shuffleFactor);
    String sortFactor = Statistics.describeFactor(avgSortTimeMs, avgExecTimeMs, "x");
    result.addResultDetail("Average sort time", Statistics.readableTimespan(avgSortTimeMs) + " " + sortFactor);

    return result;
  }

  private Severity getShuffleSortSeverity(long runtimeMs, long codetimeMs) {
    Severity runtimeSeverity = Severity.getSeverityAscending(
        runtimeMs, runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);

    if (codetimeMs <= 0) {
      return runtimeSeverity;
    }
    long value = runtimeMs * 2 / codetimeMs;

    Severity runtimeRatioSeverity = Severity.getSeverityAscending(
        value, runtimeRatioLimits[0], runtimeRatioLimits[1], runtimeRatioLimits[2], runtimeRatioLimits[3]);

    return Severity.min(runtimeSeverity, runtimeRatioSeverity);
  }
}
