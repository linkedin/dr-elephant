package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * This class provides methods to load information specific to the Pinball scheduler.
 */
public class PinballScheduler implements Scheduler {

  private static final Logger logger = Logger.getLogger(PinballScheduler.class);

  public static final String PINBALL_WORKFLOW = "pinball.workflow";
  public static final String PINBALL_INSTANCE = "pinball.instance";
  public static final String PINBALL_JOB = "pinball.job";
  public static final String PINBALL_EXECUTION = "pinball.execution";
  public static final String PINBALL_BASE_URL = "scheduler.url";
  public static final String PINBALL_BASE_URL_DEFAULT = "http://localhost:8080";

  private String _schedulerName;
  private String _jobName;
  private String _jobExecutionId;
  private String _workflowName;
  private String _workflowInstanceId;
  private String _baseUrl;

  public PinballScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData) {
    _schedulerName = schedulerConfData.getSchedulerName();
    if (properties != null) {
      loadInfo(appId, properties);
    }
  }

  private void loadInfo(String appId, Properties properties) {
    _workflowName = properties.getProperty(PINBALL_WORKFLOW);
    _workflowInstanceId = properties.getProperty(PINBALL_INSTANCE);
    _jobName = properties.getProperty(PINBALL_JOB); //
    _jobExecutionId = properties.getProperty(PINBALL_EXECUTION);
    _baseUrl = Utils.formatStringOrNull("%s", properties.getProperty(PINBALL_BASE_URL));
  }

  @Override
  public String getSchedulerName() {
    return _schedulerName;
  }

  @Override
  public boolean isEmpty() {
    return _jobName == null || _jobExecutionId == null || _workflowName == null || _workflowInstanceId == null;
  }

  @Override
  public String getJobDefId() {
    return Utils.formatStringOrNull("%s/%s", _workflowName, _jobName);
  }

  @Override
  public String getJobExecId() {
    return Utils.formatStringOrNull("%s/%s/%s/%s", _workflowName, _workflowInstanceId, _jobName, _jobExecutionId);
  }

  @Override
  public String getFlowDefId() {
    return Utils.formatStringOrNull("%s", _workflowName);
  }

  @Override
  public String getFlowExecId() {
    return Utils.formatStringOrNull("%s/%s", _workflowName, _workflowInstanceId);
  }

  @Override
  public String getJobDefUrl() {
    return Utils.formatStringOrNull("%s/executions/?workflow=%s&instance=%s&job=%s",
        _baseUrl, _workflowName, _workflowInstanceId, _jobName);
  }

  @Override
  public String getJobExecUrl() {
    return Utils.formatStringOrNull("%s/execution/?workflow=%s&instance=%s&job=%s&execution=%s",
        _baseUrl, _workflowName, _workflowInstanceId, _jobName, _jobExecutionId);
  }

  @Override
  public String getFlowDefUrl() {
    return Utils.formatStringOrNull("%s/instances/?workflow=%s", _baseUrl, _workflowName);
  }

  @Override
  public String getFlowExecUrl() {
    return Utils.formatStringOrNull("%s/jobs/?workflow=%s&instance=%s", _baseUrl, _workflowName, _workflowInstanceId);
  }

  // Sub-workflow is not supported on Pinball
  @Override
  public int getWorkflowDepth() {
    return 0;
  }

  @Override
  public String getJobName() { return _jobName; }
}
