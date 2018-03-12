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
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;

public class JobQueueLimitHeuristic implements Heuristic<MapReduceApplicationData> {

  private HeuristicConfigurationData _heuristicConfData;

  protected JobQueueLimitHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), Severity.NONE, 0);
    Properties jobConf = data.getConf();
    long queueTimeoutLimitMs = TimeUnit.MINUTES.toMillis(15);

    // Fetch the Queue to which the job is submitted.
    String queueName = jobConf.getProperty("mapred.job.queue.name");
    if (queueName == null) {
      throw new IllegalStateException("Queue Name not found.");
    }

    // Compute severity if job is submitted to default queue else set severity to NONE.
    MapReduceTaskData[] mapTasks = data.getMapperData();
    MapReduceTaskData[] redTasks = data.getReducerData();
    Severity[] mapTasksSeverity = new Severity[mapTasks.length];
    Severity[] redTasksSeverity = new Severity[redTasks.length];
    if (queueName.equals("default")) {
      result.addResultDetail("Queue: ", queueName, null);
      result.addResultDetail("Number of Map tasks", Integer.toString(mapTasks.length));
      result.addResultDetail("Number of Reduce tasks", Integer.toString(redTasks.length));

      // Calculate Severity of Mappers
      mapTasksSeverity = getTasksSeverity(mapTasks, queueTimeoutLimitMs);
      result.addResultDetail("Number of Map tasks that are in severe state (14 to 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.SEVERE, mapTasksSeverity)));
      result.addResultDetail("Number of Map tasks that are in critical state (over 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.CRITICAL, mapTasksSeverity)));

      // Calculate Severity of Reducers
      redTasksSeverity = getTasksSeverity(redTasks, queueTimeoutLimitMs);
      result.addResultDetail("Number of Reduce tasks that are in severe state (14 to 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.SEVERE, redTasksSeverity)));
      result.addResultDetail("Number of Reduce tasks that are in critical state (over 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.CRITICAL, redTasksSeverity)));

      // Calculate Job severity
      result.setSeverity(Severity.max(Severity.max(mapTasksSeverity), Severity.max(redTasksSeverity)));

    } else {
      result.addResultDetail("Not Applicable", "This Heuristic is not applicable to " + queueName + " queue");
      result.setSeverity(Severity.NONE);
    }
    return result;
  }

  private Severity[] getTasksSeverity(MapReduceTaskData[] tasks, long queueTimeout) {
    List<Severity> taskSeverityList = new ArrayList<Severity>();
    for (MapReduceTaskData task : tasks) {
      if (task.isTimeDataPresent()) {
        taskSeverityList.add(getQueueLimitSeverity(task.getTotalRunTimeMs(), queueTimeout));
      }
    }
    return taskSeverityList.toArray(new Severity[taskSeverityList.size()]);
  }

  private long getSeverityFrequency(Severity severity, Severity[] tasksSeverity) {
    long count = 0;
    for (Severity taskSeverity : tasksSeverity) {
      if (taskSeverity.equals(severity)) {
        count++;
      }
    }
    return count;
  }

  private Severity getQueueLimitSeverity(long taskTime, long queueTimeout) {
    long timeUnitMs = TimeUnit.SECONDS.toMillis(30); // 30s
    if (queueTimeout == 0) {
      return Severity.NONE;
    }
    return Severity.getSeverityAscending(taskTime, queueTimeout - 4 * timeUnitMs, queueTimeout - 3 * timeUnitMs,
        queueTimeout - 2 * timeUnitMs, queueTimeout - timeUnitMs);
  }

}
