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

package com.linkedin.drelephant.spark.legacydata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * This class contains Spark executor information.
 */
public class SparkExecutorData {
  public static final String EXECUTOR_DRIVER_NAME = "driver";

  public static class ExecutorInfo {
    public String execId;
    public String hostPort;
    public int rddBlocks = 0;
    public long memUsed = 0L;
    public long maxMem = 0L;
    public long diskUsed = 0L;

    public int activeTasks = 0;
    public int completedTasks = 0;
    public int failedTasks = 0;
    public int totalTasks = 0;
    public int maxTasks = 0;
    public long duration = 0L;
    public long inputBytes = 0L;
    public long outputBytes = 0L;
    public long shuffleRead = 0L;
    public long totalGCTime = 0L;
    public long totalMemoryBytesSpilled = 0L;
    public long shuffleWrite = 0L;

    public String toString() {
      return "{execId: " + execId + ", hostPort:" + hostPort + " , rddBlocks: " + rddBlocks + ", memUsed: " + memUsed
          + ", maxMem: " + maxMem + ", diskUsed: " + diskUsed + ", totalTasks" + totalTasks + ", maxTasks" + maxTasks + ", tasksActive: "
          + activeTasks + ", tasksComplete: " + completedTasks + ", tasksFailed: " + failedTasks + ", duration: "
          + duration + ", inputBytes: " + inputBytes + ", outputBytes:" + outputBytes + ", shuffleRead: " + shuffleRead
          + ", shuffleWrite: " + shuffleWrite + ", totalGCTime: " + totalGCTime + ", totalMemoryBytesSpilled: " + totalMemoryBytesSpilled + "}";
    }
  }

  private final Map<String, ExecutorInfo> _executorInfoMap = new HashMap<String, ExecutorInfo>();

  public void setExecutorInfo(String executorId, ExecutorInfo info) {
    _executorInfoMap.put(executorId, info);
  }

  public ExecutorInfo getExecutorInfo(String executorId) {
    return _executorInfoMap.get(executorId);
  }

  public Set<String> getExecutors() {
    return _executorInfoMap.keySet();
  }
}
