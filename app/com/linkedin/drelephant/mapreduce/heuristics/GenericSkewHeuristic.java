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

import com.google.common.primitives.Longs;
import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.log4j.Logger;


/**
 * This Heuristic analyses the skewness in the task input data
 */
public abstract class GenericSkewHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(GenericSkewHeuristic.class);

  // Severity Parameters
  private static final String NUM_TASKS_SEVERITY = "num_tasks_severity";
  private static final String DEVIATION_SEVERITY = "deviation_severity";
  private static final String FILES_SEVERITY = "files_severity";

  // Default value of parameters
  private double[] numTasksLimits = {10, 50, 100, 200};   // Number of map or reduce tasks
  private double[] deviationLimits = {2, 4, 8, 16};       // Deviation in i/p bytes btw 2 groups
  private double[] filesLimits = {1d / 8, 1d / 4, 1d / 2, 1d};  // Fraction of HDFS Block Size

  private List<MapReduceCounterData.CounterName> _counterNames;
  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confNumTasksThreshold = Utils.getParam(paramMap.get(NUM_TASKS_SEVERITY), numTasksLimits.length);
    if (confNumTasksThreshold != null) {
      numTasksLimits = confNumTasksThreshold;
    }
    logger.info(heuristicName + " will use " + NUM_TASKS_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(numTasksLimits));

    double[] confDeviationThreshold = Utils.getParam(paramMap.get(DEVIATION_SEVERITY), deviationLimits.length);
    if (confDeviationThreshold != null) {
      deviationLimits = confDeviationThreshold;
    }
    logger.info(heuristicName + " will use " + DEVIATION_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(deviationLimits));

    double[] confFilesThreshold = Utils.getParam(paramMap.get(FILES_SEVERITY), filesLimits.length);
    if (confFilesThreshold != null) {
      filesLimits = confFilesThreshold;
    }
    logger.info(
        heuristicName + " will use " + FILES_SEVERITY + " with the following threshold settings: " + Arrays.toString(
            filesLimits));
    for (int i = 0; i < filesLimits.length; i++) {
      filesLimits[i] = filesLimits[i] * HDFSContext.HDFS_BLOCK_SIZE;
    }
  }

  protected GenericSkewHeuristic(List<MapReduceCounterData.CounterName> counterNames,
      HeuristicConfigurationData heuristicConfData) {
    this._counterNames = counterNames;
    this._heuristicConfData = heuristicConfData;

    loadParameters();
  }

  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if (!data.getSucceeded()) {
      return null;
    }

    MapReduceTaskData[] tasks = getTasks(data);

    //Gathering data for checking time skew
    List<Long> timeTaken = new ArrayList<Long>();

    for (int i = 0; i < tasks.length; i++) {
      if (tasks[i].isTimeDataPresent()) {
        timeTaken.add(tasks[i].getTotalRunTimeMs());
      }
    }

    long[][] groupsTime = Statistics.findTwoGroups(Longs.toArray(timeTaken));

    long timeAvg1 = Statistics.average(groupsTime[0]);
    long timeAvg2 = Statistics.average(groupsTime[1]);

    //seconds are used for calculating deviation as they provide a better idea than millisecond.
    long timeAvgSec1 = TimeUnit.MILLISECONDS.toSeconds(timeAvg1);
    long timeAvgSec2 = TimeUnit.MILLISECONDS.toSeconds(timeAvg2);

    long minTime = Math.min(timeAvgSec1, timeAvgSec2);
    long diffTime = Math.abs(timeAvgSec1 - timeAvgSec2);

    //using the same deviation limits for time skew as for data skew. It can be changed in the fututre.
    Severity severityTime = getDeviationSeverity(minTime, diffTime);

    //This reduces severity if number of tasks is insignificant
    severityTime = Severity.min(severityTime,
        Severity.getSeverityAscending(groupsTime[0].length, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2],
            numTasksLimits[3]));

    //Gather data
    List<Long> inputBytes = new ArrayList<Long>();

    for (int i = 0; i < tasks.length; i++) {
      if (tasks[i].isCounterDataPresent()) {
        long inputByte = 0;
        for (MapReduceCounterData.CounterName counterName : _counterNames) {
          inputByte += tasks[i].getCounters().get(counterName);
        }
        inputBytes.add(inputByte);
      }
    }

    // Ratio of total tasks / sampled tasks
    double scale = ((double) tasks.length) / inputBytes.size();
    //Analyze data. TODO: This is a temp fix. findTwogroups should support list as input
    long[][] groups = Statistics.findTwoGroups(Longs.toArray(inputBytes));

    long avg1 = Statistics.average(groups[0]);
    long avg2 = Statistics.average(groups[1]);

    long min = Math.min(avg1, avg2);
    long diff = Math.abs(avg2 - avg1);

    Severity severityData = getDeviationSeverity(min, diff);

    //This reduces severity if the largest file sizes are insignificant
    severityData = Severity.min(severityData, getFilesSeverity(avg2));

    //This reduces severity if number of tasks is insignificant
    severityData = Severity.min(severityData,
        Severity.getSeverityAscending(groups[0].length, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2],
            numTasksLimits[3]));

    Severity severity = Severity.max(severityData, severityTime);

    HeuristicResult result =
        new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), severity,
            Utils.getHeuristicScore(severityData, tasks.length));

    result.addResultDetail("Data skew (Number of tasks)", Integer.toString(tasks.length));
    result.addResultDetail("Data skew (Group A)",
        groups[0].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg1) + " avg");
    result.addResultDetail("Data skew (Group B)",
        groups[1].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg2) + " avg");

    result.addResultDetail("Time skew (Number of tasks)", Integer.toString(tasks.length));
    result.addResultDetail("Time skew (Group A)",
        groupsTime[0].length + " tasks @ " + convertTimeMs(timeAvg1) + " avg");
    result.addResultDetail("Time skew (Group B)",
        groupsTime[1].length + " tasks @ " + convertTimeMs(timeAvg2) + " avg");

    return result;
  }

  private String convertTimeMs(long timeMs) {
    if (timeMs < 1000) {
      return Long.toString(timeMs) + " msec";
    }
    return DurationFormatUtils.formatDuration(timeMs, "HH:mm:ss") + " HH:MM:SS";
  }

  private Severity getDeviationSeverity(long averageMin, long averageDiff) {
    if (averageMin <= 0) {
      averageMin = 1;
    }
    long value = averageDiff / averageMin;
    return Severity.getSeverityAscending(value, deviationLimits[0], deviationLimits[1], deviationLimits[2],
        deviationLimits[3]);
  }

  private Severity getFilesSeverity(long value) {
    return Severity.getSeverityAscending(value, filesLimits[0], filesLimits[1], filesLimits[2], filesLimits[3]);
  }
}
