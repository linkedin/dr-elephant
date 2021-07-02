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

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Analyzes mapper task speed and efficiency
 */
public class MapperSpeedHeuristic implements Heuristic<TezApplicationData> {
  private static final Logger logger = Logger.getLogger(MapperSpeedHeuristic.class);

  // Severity parameters.
  private static final String DISK_SPEED_SEVERITY = "disk_speed_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] diskSpeedLimits = {1d/2, 1d/4, 1d/8, 1d/32};  // Fraction of HDFS block size
  private double[] runtimeLimits = {5, 10, 15, 30};              // The Map task runtime in milli sec

  private List<TezCounterData.CounterName> _counterNames = Arrays.asList(
      TezCounterData.CounterName.HDFS_BYTES_READ,
      TezCounterData.CounterName.S3A_BYTES_READ,
      TezCounterData.CounterName.S3N_BYTES_READ
  );

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confDiskSpeedThreshold = Utils.getParam(paramMap.get(DISK_SPEED_SEVERITY), diskSpeedLimits.length);
    if (confDiskSpeedThreshold != null) {
      diskSpeedLimits = confDiskSpeedThreshold;
    }
    logger.info(heuristicName + " will use " + DISK_SPEED_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(diskSpeedLimits));
    for (int i = 0; i < diskSpeedLimits.length; i++) {
      diskSpeedLimits[i] = diskSpeedLimits[i] * HDFSContext.DISK_READ_SPEED;
    }

    double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeThreshold != null) {
      runtimeLimits = confRuntimeThreshold;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public MapperSpeedHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  public HeuristicResult apply(TezApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    TezTaskData[] tasks = data.getMapTaskData();

    List<Long> inputSizes = new ArrayList<Long>();
    List<Long> speeds = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (TezTaskData task : tasks) {

      if (task.isSampled()) {

        long inputBytes = 0;

        for (TezCounterData.CounterName counterName: _counterNames) {
          inputBytes += task.getCounters().get(counterName);
        }

        long runtimeMs = task.getTotalRunTimeMs();
        inputSizes.add(inputBytes);
        runtimesMs.add(runtimeMs);
        //Speed is records per second
        speeds.add((1000 * inputBytes) / (runtimeMs));
      }
    }

    long medianSpeed;
    long medianSize;
    long medianRuntimeMs;

    if (tasks.length != 0) {
      medianSpeed = Statistics.median(speeds);
      medianSize = Statistics.median(inputSizes);
      medianRuntimeMs = Statistics.median(runtimesMs);
    } else {
      medianSpeed = 0;
      medianSize = 0;
      medianRuntimeMs = 0;
    }

    Severity severity = getDiskSpeedSeverity(medianSpeed);

    //This reduces severity if task runtime is insignificant
    severity = Severity.min(severity, getRuntimeSeverity(medianRuntimeMs));

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

    result.addResultDetail("Number of tasks", Integer.toString(tasks.length));
    result.addResultDetail("Median task input ", FileUtils.byteCountToDisplaySize(medianSize));
    result.addResultDetail("Median task runtime", Statistics.readableTimespan(medianRuntimeMs));
    result.addResultDetail("Median task speed", FileUtils.byteCountToDisplaySize(medianSpeed) + "/s");

    return result;
  }

  private Severity getDiskSpeedSeverity(long speed) {
    return Severity.getSeverityDescending(
        speed, diskSpeedLimits[0], diskSpeedLimits[1], diskSpeedLimits[2], diskSpeedLimits[3]);
  }

  private Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs,  runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);
  }
}