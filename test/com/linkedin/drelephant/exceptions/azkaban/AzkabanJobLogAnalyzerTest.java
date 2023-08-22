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

package com.linkedin.drelephant.exceptions.azkaban;

import com.linkedin.drelephant.exceptions.JobState;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class AzkabanJobLogAnalyzerTest {

  private String succeededAzkabanJobLog = "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Starting job sample_sample at 1466763173873\n"
      + "24-06-2016 03:12:53 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - job JVM args: -Dazkaban.flowid=sampleFlow -Dazkaban.execid=557260 -Dazkaban.jobid=sample_jobr\n"
      + "24-06-2016 03:12:55 PDT extractCrawlCompanyIndustryClassificationFlow_extractionFinalizer INFO - Finishing job sample_job attempt: 0 at 1466763175040 with status SUCCEEDED";

  private String killedAzkabanJobLog = "28-06-2016 16:58:20 PDT feature-exploration_create-index-map INFO - Starting job sample at 1467158300703\n"
      + "28-06-2016 17:58:05 PDT feature-exploration_create-index-map ERROR - Kill has been called.\n"
      + "28-06-2016 17:58:05 PDT feature-exploration_create-index-map INFO - 16/06/29 00:58:05 INFO util.Utils: Shutdown hook called\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - caught error running the job\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Token service: sample-localhostrm01.grid.linkedin.com:8032\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Cancelling mr job tracker token \n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - Job run failed!\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - Finishing job feature-exploration_create-index-map attempt: 0 at 1467161886022 with status KILLED\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - applicationIds to kill: [application_1466048666726_642278]\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - start klling application: application_1466048666726_642278\n"
      + "28-06-2016 17:58:06 PDT feature-exploration_create-index-map INFO - successfully killed application: application_1466048666726_642278";

  private String mrLevelFailedAzkabanJobLog = "24-06-2016 03:12:19 PDT help_center_sessions INFO - Starting job help_center_sessions at 1466763139993\n"
      + "24-06-2016 03:12:25 PDT help_center_sessions INFO - INFO Kind: HDFS_DELEGATION_TOKEN, Service: sample-localhostnn01.grid.linkedin.com:9000, Ident: (HDFS_DELEGATION_TOKEN token 5017233 for username)\n"
      + "24-06-2016 03:12:26 PDT help_center_sessions INFO - INFO Submitted application application_1466048666726_410150\n"
      + "24-06-2016 03:12:26 PDT help_center_sessions INFO - INFO Running job: job_1466048666726_410150\n"
      + "24-06-2016 03:12:33 PDT help_center_sessions INFO - INFO Job job_1466048666726_410150 running in uber mode : false\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - Error: java.io.FileNotFoundException: Path is not a file: /data/databases/sample/Sample/1466675602538-PT-472724050\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:70)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1671)\n"
      + "24-06-2016 03:12:40 PDT help_center_sessions INFO - \tat org.apache.hadoop.ipc.Server$Handler.run(Server.java:2038\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions ERROR - Job run failed!\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "24-06-2016 03:13:00 PDT help_center_sessions INFO - Finishing job help_center_sessions attempt: 0 at 1466763180242 with status FAILED";

  private String scriptLevelFailedAzkabanJobLog="28-06-2016 16:23:10 PDT job_search_trigger INFO - Starting job job_search_trigger at 1467156190329\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - INFO Last attempt: false\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Exception in thread \"main\" java.lang.reflect.UndeclaredThrowableException\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1686)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \t... 3 more\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Caused by: java.lang.RuntimeException: Backfill requires start and end date\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat com.linkedin.metrics.feeder.TriggerJob.generateDaily(TriggerJob.java:143)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \tat com.linkedin.metrics.feeder.TriggerJob.run(TriggerJob.java:135)\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - \t... 14 more\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Process completed unsuccessfully in 1 seconds.\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger ERROR - Job run failed!\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger ERROR - java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException cause: java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException\n"
      + "28-06-2016 16:23:12 PDT job_search_trigger INFO - Finishing job job_search_trigger attempt: 0 at 1467156192215 with status FAILED";

  private String azkabanLevelFailedAzkabanJobLog = "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Starting job feature-exploration_create-index-map at 1467146727699\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - job JVM args: -Dazkaban.flowid=feature-exploration -Dazkaban.execid=593197 -Dazkaban.jobid=feature-exploration_create-index-map\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Building spark job executor. \n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Failed to build job executor for job feature-exploration_create-index-mapCould not find variable substitution for variable(s) [global.jvm.args->user.to.proxy]\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Failed to build job type\n"
      + "azkaban.jobtype.JobTypeManagerException: Failed to build job executor for job feature-exploration_create-index-map\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map ERROR - Job run failed preparing the job.\n"
      + "28-06-2016 13:45:27 PDT feature-exploration_create-index-map INFO - Finishing job feature-exploration_create-index-map attempt: 0 at 1467146727702 with status FAILED";

  private AzkabanJobLogAnalyzer analyzedSucceededLog;
  private AzkabanJobLogAnalyzer analyzedKilledLog;
  private AzkabanJobLogAnalyzer analyzedMRLevelFailedLog;
  private AzkabanJobLogAnalyzer analyzedScriptLevelFailedLog;
  private AzkabanJobLogAnalyzer analyzedAzkabanLevelFailedLog;

  public AzkabanJobLogAnalyzerTest(){
    analyzedSucceededLog = new AzkabanJobLogAnalyzer(succeededAzkabanJobLog);
    analyzedKilledLog = new AzkabanJobLogAnalyzer(killedAzkabanJobLog);
    analyzedMRLevelFailedLog = new AzkabanJobLogAnalyzer(mrLevelFailedAzkabanJobLog);
    analyzedScriptLevelFailedLog = new AzkabanJobLogAnalyzer(scriptLevelFailedAzkabanJobLog);
    analyzedAzkabanLevelFailedLog = new AzkabanJobLogAnalyzer(azkabanLevelFailedAzkabanJobLog);
  }
  @Test
  public void getStateTest(){
    assertTrue(analyzedSucceededLog.getState() == JobState.SUCCEEDED);
    assertTrue(analyzedKilledLog.getState() == JobState.KILLED);
    assertTrue(analyzedMRLevelFailedLog.getState() == JobState.MRFAIL);
    assertTrue(analyzedScriptLevelFailedLog.getState() == JobState.SCRIPTFAIL);
    assertTrue(analyzedAzkabanLevelFailedLog.getState() == JobState.SCHEDULERFAIL);
  }

  @Test
  public void getSubEventsTest(){
    assertTrue("Succeeded sub events test failed",analyzedSucceededLog.getSubEvents().isEmpty());
    assertTrue("Script level failed sub events test failed",analyzedScriptLevelFailedLog.getSubEvents().isEmpty());
    assertTrue("Azkaban level failed sub events test failed",analyzedAzkabanLevelFailedLog.getSubEvents().isEmpty());
    assertTrue(analyzedMRLevelFailedLog.getSubEvents().size() == 1);
    assertTrue(analyzedMRLevelFailedLog.getSubEvents().iterator().next().equals("job_1466048666726_410150"));
    assertTrue("Killed sub events test failed",analyzedKilledLog.getSubEvents().isEmpty());
  }

  @Test
  public void getExceptionsTest(){
    assertTrue(analyzedSucceededLog.getException() == null);
    assertTrue(analyzedKilledLog.getException() == null);
  }
}