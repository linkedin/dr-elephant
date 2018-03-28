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

package com.linkedin.drelephant.tuning;

import models.TuningAlgorithm;


/**
 * This class holds the parameters passed to rest api from the _client.
 */
public class TuningInput {

  private String _flowDefId;
  private String _jobDefId;
  private String _flowDefUrl;
  private String _jobDefUrl;
  private String _flowExecId;
  private String _jobExecId;
  private String _flowExecUrl;
  private String _jobExecUrl;
  private String _jobName;
  private String _userName;
  private String _client;
  private String _scheduler;
  private String _defaultParams;
  private Boolean _isRetry;
  private Boolean _skipExecutionForOptimization;
  private String _jobType;
  private String _optimizationAlgo;
  private String _optimizationAlgoVersion;
  private String _optimizationMetric;
  private Double _allowedMaxResourceUsagePercent;
  private Double _allowedMaxExecutionTimePercent;
  private TuningAlgorithm _tuningAlgorithm;

  public TuningAlgorithm getTuningAlgorithm() {
    return _tuningAlgorithm;
  }

  public void setTuningAlgorithm(TuningAlgorithm tuningAlgorithm) {
    this._tuningAlgorithm = tuningAlgorithm;
  }

  public Boolean getIsRetry() {
    return _isRetry;
  }

  public void setIsRetry(Boolean isRetry) {
    this._isRetry = isRetry;
  }

  public Double getAllowedMaxResourceUsagePercent() {
    return _allowedMaxResourceUsagePercent;
  }

  public void setAllowedMaxResourceUsagePercent(Double allowedMaxResourceUsagePercent) {
    this._allowedMaxResourceUsagePercent = allowedMaxResourceUsagePercent;
  }

  public Double getAllowedMaxExecutionTimePercent() {
    return _allowedMaxExecutionTimePercent;
  }

  public void setAllowedMaxExecutionTimePercent(Double allowedMaxExecutionTimePercent) {
    this._allowedMaxExecutionTimePercent = allowedMaxExecutionTimePercent;
  }

  /**
   * Returns the flow definition id
   * @return Flow definition id
   */
  public String getFlowDefId() {
    return _flowDefId;
  }

  /**
   * Sets the flow definition id
   * @param flowDefId Flow definition id
   */
  public void setFlowDefId(String flowDefId) {
    this._flowDefId = flowDefId;
  }

  /**
   * Returns the job definition id
   * @return Job definition id
   */
  public String getJobDefId() {
    return _jobDefId;
  }

  /**
   * Sets the job definition id
   * @param jobDefId JOb definition id
   */
  public void setJobDefId(String jobDefId) {
    this._jobDefId = jobDefId;
  }

  /**
   * Returns the flow definition url
   * @return Flow definition url
   */
  public String getFlowDefUrl() {
    return _flowDefUrl;
  }

  /**
   * Sets the flow definition url
   * @param flowDefUrl Flow definition url
   */
  public void setFlowDefUrl(String flowDefUrl) {
    this._flowDefUrl = flowDefUrl;
  }

  /**
   * Returns the job definition url
   * @return Job definition url
   */
  public String getJobDefUrl() {
    return _jobDefUrl;
  }

  /**
   * Sets the job definition url
   * @param jobDefUrl Job definition url
   */
  public void setJobDefUrl(String jobDefUrl) {
    this._jobDefUrl = jobDefUrl;
  }

  /**
   * Returns the flow execution id
   * @return Flow execution id
   */
  public String getFlowExecId() {
    return _flowExecId;
  }

  /**
   * Sets the flow execution id
   * @param flowExecId Flow execution id
   */
  public void setFlowExecId(String flowExecId) {
    this._flowExecId = flowExecId;
  }

  /**
   * Returns the job execution id
   * @return Job execution id
   */
  public String getJobExecId() {
    return _jobExecId;
  }

  /**
   * Sets the job execution id
   * @param jobExecId Job execution id
   */
  public void setJobExecId(String jobExecId) {
    this._jobExecId = jobExecId;
  }

  /**
   * Returns the flow execution url
   * @return Flow execution url
   */
  public String getFlowExecUrl() {
    return _flowExecUrl;
  }

  /**
   * Sets the flow execution url
   * @param flowExecUrl Flow execution url
   */
  public void setFlowExecUrl(String flowExecUrl) {
    this._flowExecUrl = flowExecUrl;
  }

  /**
   * Returns the job execution url
   * @return Job execution url
   */
  public String getJobExecUrl() {
    return _jobExecUrl;
  }

  /**
   * Sets the job execution url
   * @param jobExecUrl Job execution url
   */
  public void setJobExecUrl(String jobExecUrl) {
    this._jobExecUrl = jobExecUrl;
  }

  /**
   * Returns the job name
   * @return Job name
   */
  public String getJobName() {
    return _jobName;
  }

  /**
   * Sets the job name
   * @param jobName Job name
   */
  public void setJobName(String jobName) {
    this._jobName = jobName;
  }

  /**
   * Returns the username of the owner of the job
   * @return Username
   */
  public String getUserName() {
    return _userName;
  }

  /**
   * Sets the username of the owner of the job
   * @param userName Username
   */
  public void setUserName(String userName) {
    this._userName = userName;
  }

  /**
   * Returns the _client. For example: UMP, Azkaban
   * @return Client
   */
  public String getClient() {
    return _client;
  }

  /**
   * Sets the _client
   * @param client Client
   */
  public void setClient(String client) {
    this._client = client;
  }

  /**
   * Returns the _scheduler
   * @return Scheduler
   */
  public String getScheduler() {
    return _scheduler;
  }

  /**
   * Sets the _scheduler
   * @param scheduler Scheduler
   */
  public void setScheduler(String scheduler) {
    this._scheduler = scheduler;
  }

  /**
   * Returns the default parameters
   * @return default parameters
   */
  public String getDefaultParams() {
    return _defaultParams;
  }

  /**
   * Sets the default parameters
   * @param defaultParams default parameters
   */
  public void setDefaultParams(String defaultParams) {
    this._defaultParams = defaultParams;
  }

  /**
   * Returns true if the execution is a retry, false otherwise
   * @return _isRetry
   */
  public Boolean getRetry() {
    return _isRetry;
  }

  /**
   * Sets the _isRetry
   * @param retry
   */
  public void setRetry(Boolean retry) {
    _isRetry = retry;
  }

  /**
   * Returns true if this execution is to be skipped for learning by optimization algorithm, false otherwise
   * @return _skipExecutionForOptimization
   */
  public Boolean getSkipExecutionForOptimization() {
    return _skipExecutionForOptimization;
  }

  /**
   * Sets the skipExecution for optimization param
   * @param skipExecutionForOptimization
   */
  public void setSkipExecutionForOptimization(Boolean skipExecutionForOptimization) {
    this._skipExecutionForOptimization = skipExecutionForOptimization;
  }

  /**
   * Returns the job type
   * @return Job type
   */
  public String getJobType() {
    return _jobType;
  }

  /**
   * Sets the job type
   * @param jobType Job type
   */
  public void setJobType(String jobType) {
    this._jobType = jobType;
  }

  /**
   * Returns the optimization algorithm
   * @return optimization algorithm
   */
  public String getOptimizationAlgo() {
    return _optimizationAlgo;
  }

  /**
   * Sets the optimization algorithm
   * @param optimizationAlgo Optimization algorithm
   */
  public void setOptimizationAlgo(String optimizationAlgo) {
    this._optimizationAlgo = optimizationAlgo;
  }

  /**
   * Returns the optimization algorithm version
   * @return Optimization algorithm version
   */
  public String getOptimizationAlgoVersion() {
    return _optimizationAlgoVersion;
  }

  /**
   * Sets the optimization algorithm version
   * @param optimizationAlgoVersion Optimization algorithm version
   */
  public void setOptimizationAlgoVersion(String optimizationAlgoVersion) {
    this._optimizationAlgoVersion = optimizationAlgoVersion;
  }

  /**
   * Returns the optimization metric
   * @return Optimization metric
   */
  public String getOptimizationMetric() {
    return _optimizationMetric;
  }

  /**
   * Sets the optimization metric
   * @param optimizationMetric Optimization metric
   */
  public void setOptimizationMetric(String optimizationMetric) {
    this._optimizationMetric = optimizationMetric;
  }
}
