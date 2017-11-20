package com.linkedin.drelephant.tez.data;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;

import java.util.Properties;

public class TezApplicationData implements HadoopApplicationData {

  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("TEZ");

  private String _appId = "";
  private Properties _conf;
  private boolean _succeeded = true;
  private TezTaskData[] _redudeTasks;
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
    return _redudeTasks;
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
    this._redudeTasks = reduceTasks;
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
}