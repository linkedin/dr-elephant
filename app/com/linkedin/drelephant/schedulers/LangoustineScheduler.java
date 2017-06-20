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
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * This class provides methods to load information specific to the Langoustine scheduler.
 */
public class LangoustineScheduler implements Scheduler {

  private static final Logger logger = Logger.getLogger(LangoustineScheduler.class);

  public static final String LANGOUSTINE_JOB_ID = "com.criteo.langoustine.name";
  public static final String LANGOUSTINE_START_DATE = "com.criteo.langoustine.calc_start";

  private String schedulerName;
  private String jobId;
  private String jobDate;
  private String jobWorkflow;
  private String jobName;

  private String jobDefUrl   = ""; // TODO
  private String jobExecUrl  = ""; // TODO
  private String flowDefUrl  = ""; // TODO
  private String flowExecUrl = ""; // TODO


  public LangoustineScheduler(String appId, Properties properties, SchedulerConfigurationData schedulerConfData) {

    String maybeWorkflow = schedulerConfData.getParamMap().get(properties.getProperty("userName"));
    if (maybeWorkflow == null) {
      jobWorkflow = "all";
    } else {
      jobWorkflow = maybeWorkflow;
    }

    schedulerName = schedulerConfData.getSchedulerName();
    if (properties != null) {
      loadInfo(appId, properties);
    } else {
      // Use default value of data type
    }
  }

  private void loadInfo(String appId, Properties properties) {
    jobId = properties.getProperty(LANGOUSTINE_JOB_ID);
    jobDate = properties.getProperty(LANGOUSTINE_START_DATE);
    jobName = jobId;
  }

  @Override
  public String getSchedulerName() {
    return schedulerName;
  }

  @Override
  public boolean isEmpty() {
    boolean result = jobId == null || jobDate == null || jobWorkflow == null;
    return result;
  }

  @Override
  public String getJobDefId() {
    return jobId;
  }

  @Override
  public String getJobExecId() {
    return jobId + "_" + jobDate;
  }

  @Override
  public String getFlowDefId() {
    return jobWorkflow;
  }

  @Override
  public String getFlowExecId() {
      return jobDate;
  }

  @Override
  public String getJobDefUrl() {
    return jobDefUrl;
  }

  @Override
  public String getJobExecUrl() {
    return jobExecUrl;
  }

  @Override
  public String getFlowDefUrl() {
    return flowDefUrl;
  }

  @Override
  public String getFlowExecUrl() {
    return flowExecUrl;
  }

  @Override
  public int getWorkflowDepth() {
    return 0;
  } // TODO

  @Override
  public String getJobName() {
    return jobName;
  }
}