/*
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
 *
 */
package com.linkedin.drelephant.tez.data;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;

import java.util.Properties;

/**
 * Tez Application level data structure which hold all task data
 */
public class TezApplicationData implements HadoopApplicationData {

  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("TEZ");

  private String _appId = "";
  private Properties _conf;
  private boolean _succeeded = true;
  private TezTaskData[] _reduceTasks;
  private TezTaskData[] _mapTasks;
  private TezCounterData _counterHolder;

  private long _submitTime = 0;
  private long _startTime = 0;
  private long _finishTime = 0;

  public boolean getSucceeded() {
    return _succeeded;
  }

  @Override
  public String getAppId() {
    return _appId;
  }

  @Override
  public Properties getConf() {
    return _conf;
  }

  @Override
  public ApplicationType getApplicationType() {
    return APPLICATION_TYPE;
  }

  @Override
  public boolean isEmpty() {
    return _succeeded && getMapTaskData().length == 0 && getReduceTaskData().length == 0;
  }

  public TezTaskData[] getReduceTaskData() {
    return _reduceTasks;
  }

  public TezTaskData[] getMapTaskData() {
    return _mapTasks;
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

  public TezCounterData getCounters() {
    return _counterHolder;
  }

  public TezApplicationData setCounters(TezCounterData counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public TezApplicationData setAppId(String appId) {
    this._appId = appId;
    return this;
  }

  public TezApplicationData setConf(Properties conf) {
    this._conf = conf;
    return this;
  }

  public TezApplicationData setSucceeded(boolean succeeded) {
    this._succeeded = succeeded;
    return this;
  }

  public TezApplicationData setReduceTaskData(TezTaskData[] reduceTasks) {
    this._reduceTasks = reduceTasks;
    return this;
  }

  public TezApplicationData setMapTaskData(TezTaskData[] mapTasks) {
    this._mapTasks = mapTasks;
    return this;
  }

  public TezApplicationData setSubmitTime(long submitTime) {
    this._submitTime = submitTime;
    return this;
  }

  public TezApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public TezApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public String toString(){
    return APPLICATION_TYPE.toString() + " " + _appId;
  }
}