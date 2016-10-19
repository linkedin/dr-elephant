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

package com.linkedin.drelephant.mapreduce.data;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import java.util.Properties;


/**
 * This class contains the MapReduce Application Information
 */
public class MapReduceApplicationData implements HadoopApplicationData {
  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("MAPREDUCE");

  private boolean _succeeded = true;
  private String _diagnosticInfo = "";
  private String _appId = "";
  private String _jobId = "";
  private String _username = "";
  private String _url = "";
  private String _jobName = "";
  private long _submitTime = 0;
  private long _startTime = 0;
  private long _finishTime = 0;

  private MapReduceCounterData _counterHolder;
  private MapReduceTaskData[] _mapperData;
  private MapReduceTaskData[] _reducerData;
  private Properties _jobConf;
  private boolean _isRetry = false;

  public MapReduceApplicationData setSucceeded(boolean succeeded) {
    this._succeeded = succeeded;
    return this;
  }

  public MapReduceApplicationData setDiagnosticInfo(String diagnosticInfo) {
    this._diagnosticInfo = diagnosticInfo;
    return this;
  }

  public MapReduceApplicationData setRetry(boolean isRetry) {
    this._isRetry = isRetry;
    return this;
  }

  public MapReduceApplicationData setAppId(String appId) {
    this._appId = appId;
    return this;
  }

  public MapReduceApplicationData setJobId(String jobId) {
    this._jobId = jobId;
    return this;
  }

  public MapReduceApplicationData setJobName(String jobName) {
    this._jobName = jobName;
    return this;
  }

  public MapReduceApplicationData setUsername(String username) {
    this._username = username;
    return this;
  }

  public MapReduceApplicationData setSubmitTime(long submitTime) {
    this._submitTime = submitTime;
    return this;
  }

  public MapReduceApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public MapReduceApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public MapReduceApplicationData setUrl(String url) {
    this._url = url;
    return this;
  }

  public MapReduceApplicationData setCounters(MapReduceCounterData counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public MapReduceApplicationData setMapperData(MapReduceTaskData[] mappers) {
    this._mapperData = mappers;
    return this;
  }

  public MapReduceApplicationData setReducerData(MapReduceTaskData[] reducers) {
    this._reducerData = reducers;
    return this;
  }

  public MapReduceApplicationData setJobConf(Properties jobConf) {
    this._jobConf = jobConf;
    return this;
  }

  public MapReduceCounterData getCounters() {
    return _counterHolder;
  }

  public MapReduceTaskData[] getMapperData() {
    return _mapperData;
  }

  public MapReduceTaskData[] getReducerData() {
    return _reducerData;
  }

  @Override
  public String getAppId() {
    return _appId;
  }

  @Override
  public Properties getConf() {
    return _jobConf;
  }

  @Override
  public ApplicationType getApplicationType() {
    return APPLICATION_TYPE;
  }

  @Override
  public boolean isEmpty() {
    return _succeeded && getMapperData().length == 0 && getReducerData().length == 0;
  }

  public String getUsername() {
    return _username;
  }

  public long getSubmitTime() {
    return _submitTime;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public String getUrl() {
    return _url;
  }

  public String getJobName() {
    return _jobName;
  }

  public boolean isRetryJob() {
    return _isRetry;
  }

  public String getJobId() {
    return _jobId;
  }

  public boolean getSucceeded() {
    return _succeeded;
  }

  public String getDiagnosticInfo() {
    return _diagnosticInfo;
  }

  @Override
  public String toString() {
    return "id: " + getJobId() + ", name:" + getJobName();
  }
}
