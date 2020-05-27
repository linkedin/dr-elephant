/*
 * Copyright 2019 LinkedIn Corp.
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
package com.linkedin.drelephant.tony.data;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.events.TaskFinished;
import com.linkedin.tony.events.TaskStarted;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;


public class TonyApplicationData implements HadoopApplicationData {
  private String _appId;
  private ApplicationType _appType;
  private Configuration _configuration;
  private Properties _props;
  private Map<String, Map<Integer, TonyTaskData>> _taskMap;

  /**
   * Constructor for {@code TonyApplicationData}.
   * @param appId  application id
   * @param appType  application type (should be TONY)
   * @param configuration  the configuration for this application
   * @param events  the events.scala emitted by this application
   */
  public TonyApplicationData(String appId, ApplicationType appType, Configuration configuration, List<Event> events) {
    _appId = appId;
    _appType = appType;

    _configuration = configuration;
    _props = new Properties();
    for (Map.Entry<String, String> entry : configuration) {
      _props.setProperty(entry.getKey(), entry.getValue());
    }

    _taskMap = new HashMap<>();
    processEvents(events);
  }

  @Override
  public String getAppId() {
    return _appId;
  }

  /**
   * Returns the {@link Configuration} for this application.
   * @return  the configuration for this application
   */
  public Configuration getConfiguration() {
    return _configuration;
  }

  @Override
  public Properties getConf() {
    return _props;
  }

  @Override
  public ApplicationType getApplicationType() {
    return _appType;
  }

  /**
   * Returns a map of task data.
   * @return  a map from task type to a map of task index to task data
   */
  public Map<String, Map<Integer, TonyTaskData>> getTaskMap() {
    return _taskMap;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  private void initTaskMap(String taskType, int taskIndex) {
    if (!_taskMap.containsKey(taskType)) {
      _taskMap.put(taskType, new HashMap<>());
    }

    if (!_taskMap.get(taskType).containsKey(taskIndex)) {
      _taskMap.get(taskType).put(taskIndex, new TonyTaskData(taskType, taskIndex));
    }
  }

  private void processEvents(List<Event> events) {
    long appFinishedTime = 0;
    for (Event event : events) {
      if (event.getType().equals(EventType.TASK_STARTED)) {
        TaskStarted taskStartedEvent = (TaskStarted) event.getEvent();
        String taskType = taskStartedEvent.getTaskType();
        int taskIndex = taskStartedEvent.getTaskIndex();
        initTaskMap(taskType, taskIndex);
        _taskMap.get(taskType).get(taskIndex).setTaskStartTime(event.getTimestamp());
      } else if (event.getType().equals(EventType.TASK_FINISHED)) {
        TaskFinished taskFinishedEvent = (TaskFinished) event.getEvent();
        String taskType = taskFinishedEvent.getTaskType();
        int taskIndex = taskFinishedEvent.getTaskIndex();
        initTaskMap(taskType, taskIndex);
        _taskMap.get(taskType).get(taskIndex).setTaskEndTime(event.getTimestamp());
        _taskMap.get(taskType).get(taskIndex).setMetrics(taskFinishedEvent.getMetrics());
      } else if (event.getType().equals(EventType.APPLICATION_FINISHED)) {
        appFinishedTime = event.getTimestamp();
      }
    }

    // Set end time for any tasks that don't have end times to application finish time
    if (appFinishedTime > 0) {
      for (Map<Integer, TonyTaskData> taskDataMap : _taskMap.values()) {
        for (TonyTaskData taskData : taskDataMap.values()) {
          if (taskData.getTaskEndTime() == 0) {
            taskData.setTaskEndTime(appFinishedTime);
          }
        }
      }
    }
  }
}
