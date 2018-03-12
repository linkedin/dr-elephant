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
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class MapperSpeedHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(MapperSpeedHeuristic.class);

  // Severity parameters.
  private static final String DISK_SPEED_SEVERITY = "disk_speed_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] diskSpeedLimits = {1d/2, 1d/4, 1d/8, 1d/32};  // Fraction of HDFS block size
  private double[] runtimeLimits = {5, 10, 15, 30};              // The Map task runtime in milli sec

  private List<MapReduceCounterData.CounterName> _counterNames = Arrays.asList(
      MapReduceCounterData.CounterName.HDFS_BYTES_READ,
      MapReduceCounterData.CounterName.S3_BYTES_READ,
      MapReduceCounterData.CounterName.S3A_BYTES_READ,
      MapReduceCounterData.CounterName.S3N_BYTES_READ
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

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }
    long totalInputByteSize=0;

    MapReduceTaskData[] tasks = data.getMapperData();

    List<Long> inputByteSizes = new ArrayList<Long>();
    List<Long> speeds = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {

      if (task.isTimeAndCounterDataPresent()) {
        long inputBytes = 0;
        for (MapReduceCounterData.CounterName counterName: _counterNames) {
          inputBytes += task.getCounters().get(counterName);
        }
        long runtimeMs = task.getTotalRunTimeMs();
        inputByteSizes.add(inputBytes);
        totalInputByteSize += inputBytes;
        runtimesMs.add(runtimeMs);
        //Speed is bytes per second
        speeds.add((1000 * inputBytes) / (runtimeMs));
      }
    }

    long medianSpeed;
    long medianSize;
    long medianRuntimeMs;
    if (tasks.length != 0) {
      medianSpeed = Statistics.median(speeds);
      medianSize = Statistics.median(inputByteSizes);
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
    result.addResultDetail("Median task input size", FileUtils.byteCountToDisplaySize(medianSize));
    result.addResultDetail("Median task runtime", Statistics.readableTimespan(medianRuntimeMs));
    result.addResultDetail("Median task speed", FileUtils.byteCountToDisplaySize(medianSpeed) + "/s");
    result.addResultDetail(CommonConstantsHeuristic.TOTAL_INPUT_SIZE_IN_MB, totalInputByteSize*1.0/(FileUtils.ONE_MB) + "");


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
