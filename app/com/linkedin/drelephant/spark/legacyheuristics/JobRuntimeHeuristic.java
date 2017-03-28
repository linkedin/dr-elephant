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
 * This heuristic rule observes the runtime characteristics of the spark application run.
 */
public class JobRuntimeHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(JobRuntimeHeuristic.class);

  // Severity parameters.
  private static final String AVG_JOB_FAILURE_SEVERITY = "avg_job_failure_rate_severity";
  private static final String SINGLE_JOB_FAILURE_SEVERITY = "single_job_failure_rate_severity";

  // Default value of parameters
  private double[] avgJobFailureLimits = {0.1d, 0.3d, 0.5d, 0.5d};  // The avg job failure rate
  private double[] jobFailureLimits = {0.0d, 0.3d, 0.5d, 0.5d};

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confAvgJobFailureLimits = Utils.getParam(paramMap.get(AVG_JOB_FAILURE_SEVERITY),
        avgJobFailureLimits.length);
    if (confAvgJobFailureLimits != null) {
      avgJobFailureLimits = confAvgJobFailureLimits;
    }
    logger.info(heuristicName + " will use " + AVG_JOB_FAILURE_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(avgJobFailureLimits));

    double[] confJobFailureLimits = Utils.getParam(paramMap.get(SINGLE_JOB_FAILURE_SEVERITY),
        jobFailureLimits.length);
    if (confJobFailureLimits != null) {
      jobFailureLimits = confJobFailureLimits;
    }
    logger.info(heuristicName + " will use " + SINGLE_JOB_FAILURE_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(jobFailureLimits));
  }

  public JobRuntimeHeuristic(HeuristicConfigurationData heuristicConfData) {
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

    Set<Integer> completedJobs = jobProgressData.getCompletedJobs();
    Set<Integer> failedJobs = jobProgressData.getFailedJobs();

    // Average job failure rate
    double avgJobFailureRate = jobProgressData.getJobFailureRate();
    Severity jobFailureRateSeverity = getAvgJobFailureRateSeverity(avgJobFailureRate);
    endSeverity = Severity.max(endSeverity, jobFailureRateSeverity);

    // For each completed individual job
    List<String> highFailureRateJobs = new ArrayList<String>();
    for (int jobId : completedJobs) {
      SparkJobProgressData.JobInfo job = jobProgressData.getJobInfo(jobId);
      double jobFailureRate = job.getFailureRate();
      Severity severity = getSingleJobFailureRateSeverity(jobFailureRate);
      if (severity.getValue() > Severity.MODERATE.getValue()) {
        highFailureRateJobs.add(
            jobProgressData.getJobDescription(jobId) + " (task failure rate:" + String.format("%1.3f", jobFailureRate)
                + ")");
      }
      endSeverity = Severity.max(endSeverity, severity);
    }

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), endSeverity, 0);

    result.addResultDetail("Spark completed jobs number", String.valueOf(completedJobs.size()));
    result.addResultDetail("Spark failed jobs number", String.valueOf(failedJobs.size()));
    result.addResultDetail("Spark failed jobs list", getJobsAsString(jobProgressData.getFailedJobDescriptions()));
    result.addResultDetail("Spark average job failure rate", String.format("%.3f", avgJobFailureRate));
    result.addResultDetail("Spark jobs with high task failure rate", getJobsAsString(highFailureRateJobs));

    return result;
  }

  private Severity getAvgJobFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(
        rate, avgJobFailureLimits[0], avgJobFailureLimits[1], avgJobFailureLimits[2], avgJobFailureLimits[3]);
  }

  private Severity getSingleJobFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(
        rate, jobFailureLimits[0], jobFailureLimits[1], jobFailureLimits[2], jobFailureLimits[3]);
  }

  private static String getJobsAsString(Collection<String> names) {
    return StringUtils.join(names, "\n");
  }
}
