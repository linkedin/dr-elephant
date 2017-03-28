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
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.spark.legacydata.SparkJobProgressData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * Spark heuristic that detects stage runtime anomalies.
 *
 */
public class StageRuntimeHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(StageRuntimeHeuristic.class);

  // Severity parameters
  private static final String STAGE_FAILURE_SEVERITY = "stage_failure_rate_severity";
  private static final String SINGLE_STAGE_FAILURE_SEVERITY = "single_stage_tasks_failure_rate_severity";
  private static final String STAGE_RUNTIME_SEVERITY = "stage_runtime_severity_in_min";

  // Default value of parameters
  private double[] stageFailRateLimits = {0.3d, 0.3d, 0.5d, 0.5d};
  private double[] singleStageFailLimits = {0.0d, 0.3d, 0.5d, 0.5d};
  private double[] stageRuntimeLimits = {15, 30, 60, 60};

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confStageFailRateLimits = Utils.getParam(paramMap.get(STAGE_FAILURE_SEVERITY),
        stageFailRateLimits.length);
    if (confStageFailRateLimits != null) {
      stageFailRateLimits = confStageFailRateLimits;
    }
    logger.info(heuristicName + " will use " + STAGE_FAILURE_SEVERITY + " with the following threshold settings: "
            + Arrays.toString(stageFailRateLimits));

    double[] confSingleFailLimits = Utils.getParam(paramMap.get(SINGLE_STAGE_FAILURE_SEVERITY),
        singleStageFailLimits.length);
    if (confSingleFailLimits != null) {
      singleStageFailLimits = confSingleFailLimits;
    }
    logger.info(heuristicName + " will use " + SINGLE_STAGE_FAILURE_SEVERITY + " with the following threshold"
        + " settings: " + Arrays.toString(singleStageFailLimits));

    double[] confStageRuntimeLimits = Utils.getParam(paramMap.get(STAGE_RUNTIME_SEVERITY), stageRuntimeLimits.length);
    if (confStageRuntimeLimits != null) {
      stageRuntimeLimits = confStageRuntimeLimits;
    }
    logger.info(heuristicName + " will use " + STAGE_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(stageRuntimeLimits));
    for (int i = 0; i < stageRuntimeLimits.length; i++) {
      stageRuntimeLimits[i] = stageRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public StageRuntimeHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkJobProgressData jobProgressData = data.getJobProgressData();
    Severity endSeverity = Severity.NONE;

    Set<SparkJobProgressData.StageAttemptId> completedStages = jobProgressData.getCompletedStages();
    Set<SparkJobProgressData.StageAttemptId> failedStages = jobProgressData.getFailedStages();

    // Average stage failure rate
    double avgStageFailureRate = jobProgressData.getStageFailureRate();
    endSeverity = Severity.max(endSeverity, getStageFailureRateSeverity(avgStageFailureRate));

    // For each completed stage, the task failure rate
    List<String> problematicStages = new ArrayList<String>();

    for (SparkJobProgressData.StageAttemptId id : completedStages) {
      SparkJobProgressData.StageInfo info = jobProgressData.getStageInfo(id.stageId, id.attemptId);
      double stageTasksFailureRate = info.getFailureRate();
      Severity tasksFailureRateSeverity = getSingleStageTasksFailureRate(stageTasksFailureRate);

      if (tasksFailureRateSeverity.getValue() > Severity.MODERATE.getValue()) {
        problematicStages.add(String.format("%s (task failure rate: %1.3f)", id, stageTasksFailureRate));
      }

      long duration = info.duration;
      Severity runtimeSeverity = getStageRuntimeSeverity(duration);
      if (runtimeSeverity.getValue() > Severity.MODERATE.getValue()) {
        problematicStages
            .add(String.format("%s (runtime: %s)", id, Statistics.readableTimespan(duration)));
      }

      endSeverity = Severity.max(endSeverity, tasksFailureRateSeverity, runtimeSeverity);
    }

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), endSeverity, 0);

    result.addResultDetail("Spark stage completed", String.valueOf(completedStages.size()));
    result.addResultDetail("Spark stage failed", String.valueOf(failedStages.size()));
    result.addResultDetail("Spark average stage failure rate", String.format("%.3f", avgStageFailureRate));
    result.addResultDetail("Spark problematic stages", getStagesAsString(problematicStages));

    return result;
  }

  private Severity getStageRuntimeSeverity(long runtime) {
    return Severity.getSeverityAscending(
        runtime, stageRuntimeLimits[0], stageRuntimeLimits[1], stageRuntimeLimits[2], stageRuntimeLimits[3]);
  }

  private Severity getStageFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(
        rate, stageFailRateLimits[0], stageFailRateLimits[1], stageFailRateLimits[2], stageFailRateLimits[3]);
  }

  private Severity getSingleStageTasksFailureRate(double rate) {
    return Severity.getSeverityAscending(
        rate, singleStageFailLimits[0], singleStageFailLimits[1], singleStageFailLimits[2], singleStageFailLimits[3]);
  }

  private static String getStagesAsString(Collection<String> names) {
    return StringUtils.join(names, "\n");
  }
}
