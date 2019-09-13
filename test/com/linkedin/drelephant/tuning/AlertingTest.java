package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.alerting.EmailNotificationManager;
import java.sql.Timestamp;
import java.util.List;
import models.JobExecution;
import models.JobSuggestedParamSet;
import com.linkedin.drelephant.ElephantContext;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;

import models.TuningJobDefinition;
import models.JobDefinition;
import org.apache.hadoop.conf.Configuration;
import static com.linkedin.drelephant.tuning.alerting.Constant.*;

public class AlertingTest implements Runnable {
  private void populateTestData() {
    try {
      initParamGenerater();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testDeveloperAlerting();
    testSKAlerting();
  }

  private void testDeveloperAlerting() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis() + 1000;
    assertTrue(" Alerting is not enabled . So generate data should be null ",
        new EmailNotificationManager(configuration).generateNotificationData(startTime, endTime) == null);

    configuration.setBoolean("alerting.enabled", true);

    List<NotificationData> notificationData =
        new EmailNotificationManager(configuration).generateNotificationData(startTime, endTime);

    assertTrue("No data within the range provided ", notificationData.size() == 0);



    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*").where().eq(JobSuggestedParamSet.TABLE.id, "1137").findUnique();
    jobSuggestedParamSet.updatedTs = new Timestamp(startTime + 100);
    jobSuggestedParamSet.createdTs = new Timestamp(endTime+1-259200000);
    jobSuggestedParamSet.update();

    JobExecution jobExecution = JobExecution.find.select("*").where().eq(JobExecution.TABLE.id,"1541").findUnique();
    jobExecution.autoTuningFault=true;
    jobExecution.updatedTs = new Timestamp(startTime + 100);
    jobExecution.update();





    NotificationManager manager = new EmailNotificationManager(configuration);

    List<NotificationData> notificationDataAfterUpdate = manager.generateNotificationData(startTime, endTime);

    assertTrue(" Notification data size "+notificationDataAfterUpdate.size(), notificationDataAfterUpdate.size() == 2);

    NotificationType notificationType = notificationDataAfterUpdate.get(0).getNotificationType();
    assertTrue(" Developers Notification  ",
        notificationType.name().equals(NotificationType.DEVELOPER.name()));




    /**
     * If user want to test email functionality
     */
   // assertTrue(" Email send successfully ", manager.sendNotification(notificationDataAfterUpdate));
  }

  private void testSKAlerting(){
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();

    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis() + 1000;
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*").where().eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, 100003).findUnique();
    tuningJobDefinition.updatedTs = new Timestamp(startTime + 100);
    tuningJobDefinition.tuningEnabled=false;
    tuningJobDefinition.autoApply=true;
    tuningJobDefinition.update();

    NotificationManager manager = new EmailNotificationManager(configuration);

    List<NotificationData> notificationData =
        manager.generateNotificationData(startTime, endTime);

    assertTrue(" Notification data size "+notificationData.size(), notificationData.size() == 0);

   /* NotificationType notificationType = notificationData.get(0).getNotificationType();
    assertTrue(" Developers Notification  ",
        notificationType.name().equals(NotificationType.STAKEHOLDER.name()));*/

    /**
     * If user want to test email functionality
     */
    //assertTrue(" Email send successfully ", manager.sendNotification(notificationData));
  }

}
