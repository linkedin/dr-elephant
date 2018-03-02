/*
 * Copyright 2017 Electronic Arts Inc.
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
 *
 */
package com.linkedin.drelephant.tez.heuristics;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Analyses garbage collection efficiency
 */
public abstract class GenericGCHeuristic implements Heuristic<TezApplicationData> {
  private static final Logger logger = Logger.getLogger(GenericGCHeuristic.class);

  // Severity Parameters
  private static final String GC_RATIO_SEVERITY = "gc_ratio_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] gcRatioLimits = {0.01d, 0.02d, 0.03d, 0.04d};   // Garbage Collection Time / CPU Time
  private double[] runtimeLimits = {5, 10, 12, 15};                // Task Runtime in milli sec

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confGcRatioThreshold = Utils.getParam(paramMap.get(GC_RATIO_SEVERITY), gcRatioLimits.length);
    if (confGcRatioThreshold != null) {
      gcRatioLimits = confGcRatioThreshold;
    }
    logger.info(heuristicName + " will use " + GC_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(gcRatioLimits));

    double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeThreshold != null) {
      runtimeLimits = confRuntimeThreshold;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public GenericGCHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  protected abstract TezTaskData[] getTasks(TezApplicationData data);

  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  public HeuristicResult apply(TezApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    TezTaskData[] tasks = getTasks(data) ;
    List<Long> gcMs = new ArrayList<Long>();
    List<Long> cpuMs = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (TezTaskData task : tasks) {
      if (task.isSampled()) {
        runtimesMs.add(task.getTotalRunTimeMs());
        gcMs.add(task.getCounters().get(TezCounterData.CounterName.GC_TIME_MILLIS));
        cpuMs.add(task.getCounters().get(TezCounterData.CounterName.CPU_MILLISECONDS));
      }
    }

    long avgRuntimeMs = Statistics.average(runtimesMs);
    long avgCpuMs = Statistics.average(cpuMs);
    long avgGcMs = Statistics.average(gcMs);
    double ratio = avgCpuMs != 0 ? avgGcMs*(1.0)/avgCpuMs: 0;

    Severity severity;
    if (tasks.length == 0) {
      severity = Severity.NONE;
    } else {
      severity = getGcRatioSeverity(avgRuntimeMs, avgCpuMs, avgGcMs);
    }

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

    result.addResultDetail("Number of tasks", Integer.toString(tasks.length));
    result.addResultDetail("Avg task runtime (ms)", Long.toString(avgRuntimeMs));
    result.addResultDetail("Avg task CPU time (ms)", Long.toString(avgCpuMs));
    result.addResultDetail("Avg task GC time (ms)", Long.toString(avgGcMs));
    result.addResultDetail("Task GC/CPU ratio", Double.toString(ratio));
    return result;
  }

  private Severity getGcRatioSeverity(long runtimeMs, long cpuMs, long gcMs) {
    double gcRatio = ((double)gcMs)/cpuMs;
    Severity ratioSeverity = Severity.getSeverityAscending(
        gcRatio, gcRatioLimits[0], gcRatioLimits[1], gcRatioLimits[2], gcRatioLimits[3]);

    // Severity is reduced if task runtime is insignificant
    Severity runtimeSeverity = getRuntimeSeverity(runtimeMs);

    return Severity.min(ratioSeverity, runtimeSeverity);
  }

  private Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs, runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);
  }

}