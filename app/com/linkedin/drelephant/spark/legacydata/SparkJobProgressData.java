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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * This class represents information contained in a job runtime process.
 */
public class SparkJobProgressData {
  private static final Logger logger = Logger.getLogger(SparkJobProgressData.class);
  private final Map<Integer, JobInfo> _jobIdToInfo = new HashMap<Integer, JobInfo>();
  private final Set<Integer> _completedJobs = new HashSet<Integer>();
  private final Set<Integer> _failedJobs = new HashSet<Integer>();

  private final Map<StageAttemptId, StageInfo> _stageIdToInfo = new HashMap<StageAttemptId, StageInfo>();
  private final Set<StageAttemptId> _completedStages = new HashSet<StageAttemptId>();
  private final Set<StageAttemptId> _failedStages = new HashSet<StageAttemptId>();

  public void addJobInfo(int jobId, JobInfo info) {
    _jobIdToInfo.put(jobId, info);
  }

  public void addCompletedJob(int jobId) {
    _completedJobs.add(jobId);
  }

  public void addFailedJob(int jobId) {
    _failedJobs.add(jobId);
  }

  public void addStageInfo(int stageId, int attemptId, StageInfo info) {
    _stageIdToInfo.put(new StageAttemptId(stageId, attemptId), info);
  }

  public void addCompletedStages(int stageId, int attemptId) {
    _completedStages.add(new StageAttemptId(stageId, attemptId));
  }

  public void addFailedStages(int stageId, int attemptId) {
    _failedStages.add(new StageAttemptId(stageId, attemptId));
  }

  public Set<Integer> getJobIds() {
    return _jobIdToInfo.keySet();
  }

  public Set<StageAttemptId> getStageAttemptIds() {
    return _stageIdToInfo.keySet();
  }

  public Set<Integer> getCompletedJobs() {
    return _completedJobs;
  }

  public Set<Integer> getFailedJobs() {
    return _failedJobs;
  }

  private static double getFailureRate(int numCompleted, int numFailed) {
    int num = numCompleted + numFailed;

    if (num == 0) {
      return 0d;
    }

    return numFailed * 1.0d / num;
  }

  public double getJobFailureRate() {
    return getFailureRate(_completedJobs.size(), _failedJobs.size());
  }

  public double getStageFailureRate() {
    return getFailureRate(_completedStages.size(), _failedStages.size());
  }

  public JobInfo getJobInfo(int jobId) {
    return _jobIdToInfo.get(jobId);
  }

  public StageInfo getStageInfo(int stageId, int attemptId) {
    return _stageIdToInfo.get(new StageAttemptId(stageId, attemptId));
  }

  public Set<StageAttemptId> getCompletedStages() {
    return _completedStages;
  }

  public Set<StageAttemptId> getFailedStages() {
    return _failedStages;
  }

  /**
   * Job itself does not have a name, it will use its latest stage as the name.
   *
   * @param jobId
   * @return
   */
  public String getJobDescription(int jobId) {
    List<Integer> stageIds = _jobIdToInfo.get(jobId).stageIds;
    int id = -1;
    for (int stageId : stageIds) {
      id = Math.max(id, stageId);
    }
    if (id == -1) {
      logger.error("Spark Job id [" + jobId + "] does not contain any stage.");
      return null;
    }
    return _stageIdToInfo.get(new StageAttemptId(id, 0)).name;
  }

  public List<String> getFailedJobDescriptions() {
    List<String> result = new ArrayList<String>();
    for (int id : _failedJobs) {
      result.add(getJobDescription(id));
    }
    return result;
  }

  // For debug purpose
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("JobInfo: [");

    for (Map.Entry<Integer, JobInfo> entry : _jobIdToInfo.entrySet()) {
      s.append("{id:" + entry.getKey() + ", value: " + entry.getValue() + "}");
    }

    s.append("]\nStageInfo: [");
    for (Map.Entry<StageAttemptId, StageInfo> entry : _stageIdToInfo.entrySet()) {
      s.append("{id:" + entry.getKey() + ", value: " + entry.getValue() + "}");
    }
    s.append("]");

    return s.toString();
  }

  public static class StageAttemptId {
    public int stageId;
    public int attemptId;

    public StageAttemptId(int stageId, int attemptId) {
      this.stageId = stageId;
      this.attemptId = attemptId;
    }

    @Override
    public int hashCode() {
      return new Integer(stageId).hashCode() * 31 + new Integer(attemptId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof StageAttemptId) {
        StageAttemptId other = (StageAttemptId) obj;
        return stageId == other.stageId && attemptId == other.attemptId;
      }
      return false;
    }

    public String toString() {
      return "id: " + stageId + " # attemptId: " + attemptId;
    }
  }

  public static class JobInfo {
    public int jobId;
    public String jobGroup;
    public long startTime;
    public long endTime;
    public final List<Integer> stageIds = new ArrayList<Integer>();

    /* Tasks */
    public int numTasks = 0;
    public int numActiveTasks = 0;
    public int numCompletedTasks = 0;
    public int numSkippedTasks = 0;
    public int numFailedTasks = 0;

    /* Stages */
    public int numActiveStages = 0;
    // This needs to be a set instead of a simple count to prevent double-counting of rerun stages:
    public final Set<Integer> completedStageIndices = new HashSet<Integer>();
    public int numSkippedStages = 0;
    public int numFailedStages = 0;

    public void addStageId(int stageId) {
      stageIds.add(stageId);
    }

    public double getFailureRate() {
      return SparkJobProgressData.getFailureRate(numCompletedTasks, numFailedTasks);
    }

    public String toString() {
      return String.format("{jobId:%s, jobGroup:%s, startTime:%s, endTime:%s, numTask:%s, numActiveTasks:%s, "
              + "numCompletedTasks:%s, numSkippedTasks:%s, numFailedTasks:%s, numActiveStages:%s, "
              + "completedStageIndices:%s, stages:%s, numSkippedStages:%s, numFailedStages:%s}", jobId, jobGroup,
          startTime, endTime, numTasks, numActiveTasks, numCompletedTasks, numSkippedTasks, numFailedTasks,
          numActiveStages, getListString(completedStageIndices), getListString(stageIds), numSkippedStages,
          numFailedStages);
    }
  }

  public static class StageInfo {
    public int numActiveTasks;
    public int numCompleteTasks;
    public final Set<Integer> completedIndices = new HashSet<Integer>();
    public int numFailedTasks;

    // Total accumulated executor runtime
    public long executorRunTime;
    // Total stage duration
    public long duration;

    // Note, currently calculating I/O speed on stage level does not make sense
    // since we do not have information about specific I/O time.
    public long inputBytes = 0;
    public long outputBytes = 0;
    public long shuffleReadBytes = 0;
    public long shuffleWriteBytes = 0;
    public long memoryBytesSpilled = 0;
    public long diskBytesSpilled = 0;

    public String name;
    public String description;

    public double getFailureRate() {
      return SparkJobProgressData.getFailureRate(numCompleteTasks, numFailedTasks);
    }

    // TODO: accumulables info seem to be unnecessary, might might be useful later on
    // sample code from Spark source: var accumulables = new HashMap[Long, AccumulableInfo]

    @Override
    public String toString() {
      return String.format("{numActiveTasks:%s, numCompleteTasks:%s, completedIndices:%s, numFailedTasks:%s,"
              + " executorRunTime:%s, inputBytes:%s, outputBytes:%s, shuffleReadBytes:%s, shuffleWriteBytes:%s,"
              + " memoryBytesSpilled:%s, diskBytesSpilled:%s, name:%s, description:%s}",
          numActiveTasks, numCompleteTasks, getListString(completedIndices), numFailedTasks, executorRunTime,
          inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes, memoryBytesSpilled, diskBytesSpilled, name,
          description);
    }
  }

  private static String getListString(Collection collection) {
    return "[" + StringUtils.join(collection, ",") + "]";
  }
}
