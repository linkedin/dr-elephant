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

package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;


/**
 * This class wraps some basic meta data of a completed application run (notice that the information is generally the
 * same regardless of hadoop versions and application types), and then promises to return the analyzed result later.
 * This class implements Delayed so that a running job can be added back to the Queue after a set delay.
 */
public class AnalyticJob implements Delayed {
  private static final Logger logger = Logger.getLogger(AnalyticJob.class);

  private static final String UNKNOWN_JOB_TYPE = "Unknown";   // The default job type when the data matches nothing.
  private static final int _RETRY_LIMIT = 3;                  // Number of times a job needs to be tried before dropping

  private int _retries = 0;
  private ApplicationType _type;
  private String _appId;
  private String _name;
  private String _queueName;
  private String _user;
  private String _trackingUrl;
  private String _finalStatus;
  private String _jobStatus;
  private long _startTime;
  private long _finishTime;
  private long _severity;
  private long _expiryTime;
  private long _analysisStartTime;

  public enum _status {
    ANY,
    RUNNING,
    FAILED,
    SUCCEEDED
  }

  public AnalyticJob setJobStatus(String jobStatus) {
    _jobStatus = jobStatus;
    return this;
  }

  public String getJobStatus() {
    return _jobStatus;
  }

  public void updateExpiryTime(long expiry) {
    _expiryTime = System.currentTimeMillis() + expiry;
  }

  public long getExpiryTime() {
    return _expiryTime;
  }

  /**
   * Returns the application type
   * E.g., Mapreduce or Spark
   *
   * @return The application type
   */
  public ApplicationType getAppType() {
    return _type;
  }

  /**
   * Set the application type of this job.
   *
   * @param type The Application type
   * @return The analytic job
   */
  public AnalyticJob setAppType(ApplicationType type) {
    _type = type;
    return this;
  }

  /**
   * Set the application id of this job
   *
   * @param appId The application id of the job obtained resource manager
   * @return The analytic job
   */
  public AnalyticJob setAppId(String appId) {
    _appId = appId;
    return this;
  }

  /**
   * Set the name of the analytic job
   *
   * @param name
   * @return The analytic job
   */
  public AnalyticJob setName(String name) {
    _name = name;
    return this;
  }

  /**
   * Set the queue name in which the analytic jobs was submitted
   *
   * @param name the name of the queue
   * @return The analytic job
   */
  public AnalyticJob setQueueName(String name) {
    _queueName = name;
    return this;
  }

  /**
   * Sets the user who ran the job
   *
   * @param user The username of the user
   * @return The analytic job
   */
  public AnalyticJob setUser(String user) {
    _user = user;
    return this;
  }

  /**
   * Sets the start time of the job
   * Start time is the time at which the job was submitted by the resource manager
   *
   * @param startTime
   * @return The analytic job
   */
  public AnalyticJob setStartTime(long startTime) {
    // TIMESTAMP range starts from FROM_UNIXTIME(1) = 1970-01-01 00:00:01
    if (startTime <= 0) {
      startTime = 1000; // 1 sec
    }
    _startTime = startTime;
    return this;
  }

  /**
   * Sets the finish time of the job
   *
   * @param finishTime
   * @return The analytic job
   */
  public AnalyticJob setFinishTime(long finishTime) {
    // TIMESTAMP range starts from FROM_UNIXTIME(1) = 1970-01-01 00:00:01
    if (finishTime <= 0) {
      finishTime = 1000; // 1 sec
    }
    _finishTime = finishTime;
    return this;
  }

  /**
   * Sets the severity of the job.
   *
   * @return The analytic job
   */
  public AnalyticJob setSeverity(long severity) {
    _severity = severity;
    return this;
  }

  /**
   * Returns the application id
   *
   * @return The analytic job
   */
  public String getAppId() {
    return _appId;
  }

  /**
   * Returns the name of the analytic job
   *
   * @return the analytic job's name
   */
  public String getName() {
    return _name;
  }

  /**
   * Returns the user who ran the job
   *
   * @return The user who ran the analytic job
   */
  public String getUser() {
    return _user;
  }

  /**
   * Returns the time at which the job was submitted by the resource manager
   *
   * @return The start time
   */
  public long getStartTime() {
    return _startTime;
  }

  /**
   * Returns the finish time of the job.
   *
   * @return The finish time
   */
  public long getFinishTime() {
    return _finishTime;
  }

  /**
   * Returns the current severity of the job.
   *
   * @return The severity
   */
  public long getSeverity() {
    return _severity;
  }

  /**
   * Returns the tracking url of the job
   *
   * @return The tracking url in resource manager
   */
  public String getTrackingUrl() {
    return _trackingUrl;
  }

  /**
   * Returns the queue in which the application was submitted
   *
   * @return The queue name
   */
  public String getQueueName() {
    return _queueName;
  }

  /**
   * Sets the tracking url for the job
   *
   * @param trackingUrl The url to track the job
   * @return The analytic job
   */
  public AnalyticJob setTrackingUrl(String trackingUrl) {
    _trackingUrl = trackingUrl;
    return this;
  }

  public String getFinalStatus() {
    return _finalStatus;
  }

  /**
   * Updates the finalStatus of the job
   *
   * @param finalStatus The final status of the job
   */
  public AnalyticJob setFinalStatus(String finalStatus) {
    _finalStatus = finalStatus;
    return this;
  }

  public long getAnalysisStartTime() {
    return _analysisStartTime;
  }

  public void setAnalysisStartTime(long analysisStartTime) {
    _analysisStartTime = analysisStartTime;
  }

  /**
   * Returns the analysed AppResult that could be directly serialized into DB.
   *
   * This method fetches the data using the appropriate application fetcher, runs all the heuristics on them and
   * loads it into the AppResult model.
   *
   * @throws Exception if the analysis process encountered a problem.
   * @return the analysed AppResult
   */
  public AppResult getAnalysis(Job job) throws Exception {
    ElephantFetcher fetcher = ElephantContext.instance().getFetcherForApplicationType(getAppType());
    HadoopApplicationData data = fetcher.fetchData(this, job);

    // Run all heuristics over the fetched data
    List<HeuristicResult> analysisResults = new ArrayList<HeuristicResult>();
    if (data == null || data.isEmpty()) {
      // Example: a MR job has 0 mappers and 0 reducers
      logger.info("No Data Received for analytic job: " + getAppId());
      analysisResults.add(HeuristicResult.NO_DATA);
    } else {
      this.setFinishTime(data.getFinishTime()).setJobStatus(data.getStatus());
      List<Heuristic> heuristics = ElephantContext.instance().getHeuristicsForApplicationType(getAppType());
      for (Heuristic heuristic : heuristics) {
        HeuristicResult result = heuristic.apply(data);
        if (result != null) {
          analysisResults.add(result);
        }
      }
    }

    JobType jobType = ElephantContext.instance().matchJobType(data);
    String jobTypeName = jobType == null ? UNKNOWN_JOB_TYPE : jobType.getName();

    HadoopMetricsAggregator hadoopMetricsAggregator = ElephantContext.instance().getAggregatorForApplicationType(getAppType());
    hadoopMetricsAggregator.aggregate(data);
    HadoopAggregatedData hadoopAggregatedData = hadoopMetricsAggregator.getResult();

    // Load app information
    AppResult result = new AppResult();
    result.id = Utils.truncateField(getAppId(), AppResult.ID_LIMIT, getAppId());
    result.trackingUrl = Utils.truncateField(getTrackingUrl(), AppResult.TRACKING_URL_LIMIT, getAppId());
    result.queueName = Utils.truncateField(getQueueName(), AppResult.QUEUE_NAME_LIMIT, getAppId());
    result.username = Utils.truncateField(getUser(), AppResult.USERNAME_LIMIT, getAppId());
    result.startTime = getStartTime();
    result.finishTime = getFinishTime();
    result.name = Utils.truncateField(getName(), AppResult.APP_NAME_LIMIT, getAppId());
    result.jobType = Utils.truncateField(jobTypeName, AppResult.JOBTYPE_LIMIT, getAppId());
    result.resourceUsed = hadoopAggregatedData.getResourceUsed();
    result.totalDelay = hadoopAggregatedData.getTotalDelay();
    result.resourceWasted = hadoopAggregatedData.getResourceWasted();
    result.status = Utils.truncateField(getJobStatus(), AppResult.STATUS_LIMIT, getAppId());

    // Load App Heuristic information
    int jobScore = 0;
    result.yarnAppHeuristicResults = new ArrayList<AppHeuristicResult>();
    Severity worstSeverity = Severity.UNDEFINED;
    for (HeuristicResult heuristicResult : analysisResults) {
      AppHeuristicResult detail = new AppHeuristicResult();
      detail.heuristicClass = Utils.truncateField(heuristicResult.getHeuristicClassName(),
          AppHeuristicResult.HEURISTIC_CLASS_LIMIT, getAppId());
      detail.heuristicName = Utils.truncateField(heuristicResult.getHeuristicName(),
          AppHeuristicResult.HEURISTIC_NAME_LIMIT, getAppId());
      detail.severity = heuristicResult.getSeverity();
      detail.score = heuristicResult.getScore();

      // Load Heuristic Details
      for (HeuristicResultDetails heuristicResultDetails : heuristicResult.getHeuristicResultDetails()) {
        AppHeuristicResultDetails heuristicDetail = new AppHeuristicResultDetails();
        heuristicDetail.yarnAppHeuristicResult = detail;
        heuristicDetail.name = Utils.truncateField(heuristicResultDetails.getName(),
            AppHeuristicResultDetails.NAME_LIMIT, getAppId());
        heuristicDetail.value = Utils.truncateField(heuristicResultDetails.getValue(),
            AppHeuristicResultDetails.VALUE_LIMIT, getAppId());
        heuristicDetail.details = Utils.truncateField(heuristicResultDetails.getDetails(),
            AppHeuristicResultDetails.DETAILS_LIMIT, getAppId());
        // This was added for AnalyticTest. Commenting this out to fix a bug. Also disabling AnalyticJobTest.
        //detail.yarnAppHeuristicResultDetails = new ArrayList<AppHeuristicResultDetails>();
        detail.yarnAppHeuristicResultDetails.add(heuristicDetail);
      }
      result.yarnAppHeuristicResults.add(detail);
      worstSeverity = Severity.max(worstSeverity, detail.severity);
      jobScore += detail.score;
    }
    result.severity = worstSeverity;
    result.score = jobScore;

    // Retrieve information from job configuration like scheduler information and store them into result.
    InfoExtractor.loadInfo(result, data);

    return result;
  }

  /**
   * Indicate this promise should retry itself again.
   *
   * @return true if should retry, else false
   */
  public boolean retry() {
    return (_retries++) < _RETRY_LIMIT;
  }

  public int getRetries() {
    return _retries;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long diff = _expiryTime - System.currentTimeMillis();
    return unit.convert(diff, TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    if (this._expiryTime < ((AnalyticJob) o)._expiryTime) {
      return -1;
    }
    if (this._expiryTime > ((AnalyticJob) o)._expiryTime) {
      return 1;
    }
    return 0;
  }
}
