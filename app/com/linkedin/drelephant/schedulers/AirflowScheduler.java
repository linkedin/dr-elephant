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

package com.linkedin.drelephant.schedulers;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.Properties;
import org.apache.log4j.Logger;


/**
 * This class provides methods to load information specific to the Airflow scheduler.
 */
public class AirflowScheduler implements Scheduler {

  private static final Logger logger = Logger.getLogger(AirflowScheduler.class);

  public static final String AIRFLOW_TASK_ID = "airflow.ctx.task.task_id";
  public static final String AIRFLOW_TASK_INSTANCE_EXECUTION_DATE = "airflow.ctx.task_instance.execution_date";
  public static final String AIRFLOW_DAG_ID = "airflow.ctx.dag.dag_id";
  public static final String AIRFLOW_DAG_RUN_EXECUTION_DATE = "airflow.ctx.dag_run.execution_date";

  public static final String AIRFLOW_BASE_URL_PARAM_NAME = "airflowbaseurl";
  private static final String AIRFLOW_BASE_URL_DEFAULT = "http://localhost:8000";

  private String _schedulerName;
  private String _taskId;
  private String _taskInstanceExecutionDate;
  private String _dagId;
  private String _dagRunExecutionDate;
  private int _subdagDepth;
  private String _baseUrl;


  public AirflowScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData) {
    _schedulerName = schedulerConfData.getSchedulerName();
    _baseUrl = schedulerConfData.getParamMap().get(AIRFLOW_BASE_URL_PARAM_NAME);
    if (_baseUrl == null) {
      _baseUrl = AIRFLOW_BASE_URL_DEFAULT;
    }

    if (properties != null) {
      loadInfo(appId, properties);
    } else {
      // Use default value of data type
    }
  }

  private void loadInfo(String appId, Properties properties) {
    // examples:
    // my_amazing_task_id
    _taskId = properties.getProperty(AIRFLOW_TASK_ID);
    // 2016-06-27T01:30:00
    _taskInstanceExecutionDate = properties.getProperty(AIRFLOW_TASK_INSTANCE_EXECUTION_DATE);
    // my_amazing_dag_id
    _dagId = properties.getProperty(AIRFLOW_DAG_ID); //
    // 2016-06-27T00:00:00
    _dagRunExecutionDate = properties.getProperty(AIRFLOW_DAG_RUN_EXECUTION_DATE);

    _subdagDepth = 0; // TODO: Add sub-dag support
  }

  @Override
  public String getSchedulerName() {
    return _schedulerName;
  }

  @Override
  public boolean isEmpty() {
    return _taskId == null || _taskInstanceExecutionDate == null || _dagId == null || _dagRunExecutionDate == null;
  }

  @Override
  public String getJobDefId() {
    return Utils.formatStringOrNull("%s/%s", _dagId, _taskId);
  }

  @Override
  public String getJobExecId() {
    return Utils.formatStringOrNull("%s/%s/%s/%s", _dagId, _dagRunExecutionDate, _taskId, _taskInstanceExecutionDate);
  }

  @Override
  public String getFlowDefId() {
    return Utils.formatStringOrNull("%s", _dagId);
  }

  @Override
  public String getFlowExecId() {
    return Utils.formatStringOrNull("%s/%s", _dagId, _dagRunExecutionDate);
  }

  @Override
  public String getJobDefUrl() {
    return Utils.formatStringOrNull("%s/admin/airflow/code?dag_id=%s&task_id=%s", _baseUrl, _dagId, _taskId);
  }

  @Override
  public String getJobExecUrl() {
    return Utils.formatStringOrNull("%s/admin/airflow/log?dag_id=%s&task_id=%s&execution_date=%s",
            _baseUrl, _dagId, _taskId, _taskInstanceExecutionDate);

  }

  @Override
  public String getFlowDefUrl() {
    return Utils.formatStringOrNull("%s/admin/airflow/graph?dag_id=%s", _baseUrl, _dagId);
  }

  @Override
  public String getFlowExecUrl() {
    return Utils.formatStringOrNull("%s/admin/airflow/graph?dag_id=%s&execution_date=%s", _baseUrl, _dagId, _dagRunExecutionDate);
  }

  @Override
  public int getWorkflowDepth() {
    return _subdagDepth;
  }

  @Override
  public String getJobName() { return _taskId; }
}
