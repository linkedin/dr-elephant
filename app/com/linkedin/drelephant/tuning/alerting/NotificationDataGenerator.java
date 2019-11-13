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

package com.linkedin.drelephant.tuning.alerting;

import com.linkedin.drelephant.tuning.NotificationData;
import com.linkedin.drelephant.util.Utils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import models.AppResult;
import models.JobExecution;
import models.JobSuggestedParamSet;

import static com.linkedin.drelephant.tuning.alerting.Constant.*;

import models.TuningJobDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This class queries DB and generates notification data based on the desired rule
 */
public class NotificationDataGenerator {
  private static final Logger logger = Logger.getLogger(NotificationDataGenerator.class);
  boolean debugEnabled = logger.isDebugEnabled();
  private long windowStartTimeMS;
  private long windowEndTimeMS;

//  Time for which a parameter set is in EXECUTED state before it gets alerted
  private long paramExecutedStateStaleTime;
  private static String DEVELOPERS_RECIPIENT_ADDRESS = null;
  private List<NotificationData> notificationMessages = null;
  private static String EMAIL_DOMAIN_NAME = null;
  private static long RECENCY_WINDOW_IN_MS = 259200000;
  private static final String PARAM_EXECUTED_STATE_STALE_TIME = "tuning.stale.data.timeout.ms";
  private static long DEFAULT_PARAM_EXECUTED_STATE_STALE_TIME = 172800000;

  public NotificationDataGenerator(long windowStartTimeMS, long windowEndTimeMS, Configuration configuration) {
    this.windowStartTimeMS = windowStartTimeMS;
    this.windowEndTimeMS = windowEndTimeMS;
    DEVELOPERS_RECIPIENT_ADDRESS = configuration.get(ALERTING_DEVELOPERS_RECIPIENT_ADDRESS_PROPERTY);
    EMAIL_DOMAIN_NAME = configuration.get(EMAIL_DOMAIN_NAME_PROPERTY);
    notificationMessages = new ArrayList<NotificationData>();
    paramExecutedStateStaleTime =
        Utils.getNonNegativeLong(configuration, PARAM_EXECUTED_STATE_STALE_TIME, DEFAULT_PARAM_EXECUTED_STATE_STALE_TIME);
  }

  //todo: Create Rule Interface and then extends specific rule from that interface .
  public List<NotificationData> generateNotificationData() {
    try {
      bestParameterPenaltyDeveloperRule();
      jobTunedSKRule();
      failureBecauseOfAutotuningDeveloperRule();
      paramFitnessNotComputedRule();
    } catch (Exception e) {
      logger.error(" Error generating notification data ", e);
    }
    return notificationMessages;
  }

  /**
   *   This rule checks if the best parameters for the job
   *   is also the penalty parameter . This situation should not happen
   *   and developer should be informed about the same.
   */
  private void bestParameterPenaltyDeveloperRule() {
    long recencyWindow = windowEndTimeMS - RECENCY_WINDOW_IN_MS;
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .between(JobSuggestedParamSet.TABLE.updatedTs, new Timestamp(windowStartTimeMS), new Timestamp(windowEndTimeMS))
        .between(JobSuggestedParamSet.TABLE.createdTs,new Timestamp(recencyWindow),new Timestamp(windowEndTimeMS))
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, true)
        .eq(JobSuggestedParamSet.TABLE.fitness, 10000)
        .findList();
    if (jobSuggestedParamSets.size() > 0) {
      NotificationData data = new NotificationData(DEVELOPERS_RECIPIENT_ADDRESS);
      data.setSubject(" Following jobs have penalty parameter as the best parameter . Please fix this");
      data.setNotificationType(NotificationType.DEVELOPER);
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        data.addContent(jobSuggestedParamSet.toString() + "\t" + jobSuggestedParamSet.jobDefinition.jobDefId);
      }
      notificationMessages.add(data);
    }
    logger.debug(" Best Parameter Penalty Rule  " + jobSuggestedParamSets.size());
  }

  /**
   *   This rule alerts if the execution is failed because of autotuining.
   */
  private void failureBecauseOfAutotuningDeveloperRule() {
    List<JobExecution> jobExecutions = JobExecution.find.select("*")
        .fetch(JobExecution.TABLE.jobExecId, "*")
        .where()
        .between(JobExecution.TABLE.updatedTs, new Timestamp(windowStartTimeMS), new Timestamp(windowEndTimeMS))
        .eq(JobExecution.TABLE.autoTuningFault, true)
        .findList();
    if (jobExecutions.size() > 0) {
      NotificationData data = new NotificationData(DEVELOPERS_RECIPIENT_ADDRESS);
      data.setSubject(" Following executions have failed because of autotuning");
      data.setNotificationType(NotificationType.DEVELOPER);
      for (JobExecution jobExecution : jobExecutions) {
        data.addContent(jobExecution.jobExecUrl);
      }
      notificationMessages.add(data);
    }
    logger.debug(" Failure Because of AutoTuning " + jobExecutions.size());
  }

  /**
   *  This rule alerts if param is in EXECUTED state for last couple of days
   */

  private void paramFitnessNotComputedRule() {
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.EXECUTED)
        .between(JobSuggestedParamSet.TABLE.updatedTs,
            new Timestamp(windowStartTimeMS - paramExecutedStateStaleTime), new Timestamp(windowEndTimeMS - paramExecutedStateStaleTime))
        .findList();

    if (jobSuggestedParamSets != null && jobSuggestedParamSets.size() > 0) {
      NotificationData data = new NotificationData(DEVELOPERS_RECIPIENT_ADDRESS);
      data.setSubject(" Following are the parameter Ids which are in EXECUTED state from couple of days");
      data.setNotificationType(NotificationType.DEVELOPER);
      for (JobSuggestedParamSet jobSuggestedParamSet : jobSuggestedParamSets) {
        data.addContent(String.valueOf(jobSuggestedParamSet.id));
      }
      notificationMessages.add(data);
    }

    if (debugEnabled) {
      logger.debug(" No of parameters are in EXECUTED state : " + jobSuggestedParamSets.size());
    }
  }

  /**
   *  This rule checks if the there is any job which are de boarded from tuning.
   *  Notification should be send to developer if the job is de boarded.
   */
  private void jobTunedSKRule() {
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.select("*")
        .where()
        .between(TuningJobDefinition.TABLE.updatedTs, new Timestamp(windowStartTimeMS), new Timestamp(windowEndTimeMS))
        .eq(TuningJobDefinition.TABLE.autoApply, true)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, false)
        .findList();
    if (tuningJobDefinitions.size() > 0) {
      for (TuningJobDefinition tuningJobDefinition : tuningJobDefinitions) {
        String emailSendToAddress = getUserEmailAddress(tuningJobDefinition);
        if (emailSendToAddress != null) {
          logger.info(" Sending email to developer "+emailSendToAddress);
          /*NotificationData data =
              new NotificationData(emailSendToAddress + DELIMITER_BETWEEN_USERNAME_EMAIL + EMAIL_DOMAIN_NAME);
          data.setSubject(" Job is tuned and deboarded  ");
          data.setNotificationType(NotificationType.STAKEHOLDER);
          data.addContent(tuningJobDefinition.job.jobDefId + "\t" + tuningJobDefinition.job.jobName);
          notificationMessages.add(data);*/
        }
      }
      logger.info(" Job Tuned  " + tuningJobDefinitions.size());
    }
  }

  private String getUserEmailAddress(TuningJobDefinition tuningJobDefinition) {
    AppResult appResult = AppResult.find.select("*")
        .where()
        .eq(AppResult.TABLE.JOB_DEF_ID, tuningJobDefinition.job.jobDefId)
        .setMaxRows(1)
        .findUnique();
    if (appResult != null) {
      String userName = appResult.username;
      if (userName != null && userName.length() > 1) {
        logger.info(" User name is " + userName);
        return userName;
      }
    }
    logger.info(" No username is found");
    return null;
  }
}
