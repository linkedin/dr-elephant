/*
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

package com.linkedin.drelephant.exceptions;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.exceptions.util.ExceptionUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;
import play.test.Helpers;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class TonYExceptionFingerprintingTest {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private static FakeApplication fakeApp;
  private WireMockServer _wireMockServer;

  private String stderrContainerLogParameters = "/amstderr.log?start=0";
  private String stdoutContainerLogParameters = "/amstdout.log?start=0";

  private final String MOCK_RM_HOST = "localhost";
  private final Integer MOCK_RM_PORT = 8042;
  private final String TEST_APPLICATION_ID_1 = "app_1";
  private final String TEST_WORKFLOW_URL_1 = "https://elephant.linkedin.com:8443/executor?execid=1";
  private final String TEST_JOB_EXEC_URL_1 = "https://elephant.linkedin.com:8443/executor?execid=1&job=job_1&attempt=0";
  private final String TEST_JOB_NAME_1 = "job_1";
  private final String TEST_AM_LOG_CONTAINER_URL_1 = "http://localhost:8042/node/containerlogs/container_e42_1576097000949_30598_01_000001/user1";


  private final String TEST_APPLICATION_ID_2 = "app_2";
  private final String TEST_WORKFLOW_URL_2 = "https://elephant.linkedin.com:8443/executor?execid=2";
  private final String TEST_JOB_EXEC_URL_2 = "https://elephant.linkedin.com:8443/executor?execid=2&job=job_2&attempt=0";
  private final String TEST_JOB_NAME_2 = "job_2";
  private final String TEST_AM_LOG_CONTAINER_URL_2 = "http://localhost:8042/node/containerlogs/container_e42_157609980900_57489_01_000001/user2";

  private final String TEST_APPLICATION_ID_3 = "app_3";
  private final String TEST_WORKFLOW_URL_3 = "https://elephant.linkedin.com:8443/executor?execid=3";
  private final String TEST_JOB_EXEC_URL_3 = "https://elephant.linkedin.com:8443/executor?execid=3&job=job_3&attempt=0";
  private final String TEST_JOB_NAME_3 = "job_3";
  private final String TEST_AM_LOG_CONTAINER_URL_3 = "http://localhost:8042/node/containerlogs/container_e42_157609980900_38479_01_000001/user3";

  private final String MOCK_RESPONSE_1 = new StringBuilder()
      .append("<html>\n<body>\n<table>\n<pre>\n")
      .append("20/01/24 21:28:46 INFO tony.ApplicationMaster: Unregistering task [worker:5] from Heartbeat monitor..\n")
      .append("20/01/24 21:28:47 INFO tony.ApplicationMaster: Received result registration request with exit code 1 from worker 0\n")
      .append("20/01/24 21:28:47 INFO tony.ApplicationMaster: Unregistering task [worker:0] from Heartbeat monitor..\n")
      .append("20/01/24 21:28:47 INFO tony.ApplicationMaster: Completed containers: 2\n")
      .append("20/01/24 21:28:47 INFO tony.ApplicationMaster: ContainerID = container_e42_1576097000949_30598_01_000013, state = COMPLETE, exitStatus = 1\n")
      .append("20/01/24 21:28:47 ERROR tony.ApplicationMaster: [2020-01-24 21:28:46.848]Exception from container-launch.\n")
      .append("Container id: container_e42_1576097000949_30598_01_000013\n")
      .append("Exit code: 1\n").append("Shell output: main : command provided 1\n")
      .append("main : run as user is user_1\n")
      .append("main : requested yarn user is user_1\n")
      .append("Getting exit code file...\n")
      .append("Creating script paths...\n")
      .append("Writing pid file...\n")
      .append("Writing to tmp file /grid/c/tmp/yarn/nmPrivate/application_1576097000949_30598/container_e42_1576097000949_30598_01_000013/container_e42_1576097000949_30598_01_000013.pid.tmp\n")
      .append("Writing to cgroup task files...\n")
      .append("Creating local dirs...\n")
      .append("Launching container...\n")
      .append("Getting exit code file...\n")
      .append("Creating script paths...\n") .append("\n").append("\n")
      .append("[2020-01-24 21:28:46.869]Container exited with a non-zero exit code 1. Error file: prelaunch.err.\n")
      .append("Last 4096 bytes of prelaunch.err :\n") .append("Last 4096 bytes of stderr :\n")
      .append("20/01/24 21:28:45 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable\n")
      .append("20/01/24 21:28:46 WARN shortcircuit.DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.\n")
      .append("Traceback (most recent call last):\n")
      .append("Sample_traceback\n")
      .append("tensorflow.python.framework.errors_impl.NotFoundError: hdfs://hostname.linkedin.com:9000/trainingData; No such file or directory\n\n")
      .append("20/01/24 21:28:46 INFO util.Utils: pollTillNonNull function finished within 60 seconds\n")
      .append("20/01/24 21:28:46 INFO tony.TaskExecutor: AM response for result execution run: RECEIVED\n")
      .append("20/01/24 21:28:46 INFO tony.TaskExecutor: Child process exited with exit code 1\n")
      .append("</pre>\n</table>\n</body>\n</html>\n").toString();

  private final String MOCK_RESPONSE_2 = new StringBuilder()
      .append("<html>\n<body>\n<table>\n<pre>\n")
      .append("2020-02-05 02:56:18 INFO  ApplicationMaster:978 - ContainerID = container_e42_1576097000949_37909_01_000002, state = COMPLETE, exitStatus = 1\n")
      .append("2020-02-05 02:56:18 ERROR ApplicationMaster:983 - [2020-02-05 02:56:18.039]Exception from container-launch.\n")
      .append("Container id: container_e42_1576097000949_37909_01_000002\n")
      .append("Exit code: 1\n")
      .append("Shell output: main : command provided 1\n")
      .append("main : run as user is sqf\n")
      .append("main : requested yarn user is user2\n")
      .append("Getting exit code file...\n")
      .append("Creating script paths...\n")
      .append("Writing pid file...\n")
      .append("Writing to tmp file /grid/a/tmp/yarn/nmPrivate/application_1576097000949_37909/container_e42_1576097000949_37909_01_000002/container_e42_1576097000949_37909_01_000002.pid.tmp\n")
      .append("Writing to cgroup task files...\n")
      .append("Creating local dirs...\n")
      .append("Launching container...\n")
      .append("Getting exit code file...\n")
      .append("Creating script paths...\n")
      .append("\n")
      .append("\n")
      .append("[2020-02-05 02:56:18.081]Container exited with a non-zero exit code 1. Error file: prelaunch.err.\n")
      .append("Last 4096 bytes of prelaunch.err :\n")
      .append("Last 4096 bytes of stderr :\n")
      .append("ib/python3.6/site-packages/tensorflow/python/estimator/estimator.py\", line 354, in train\n")
      .append("    loss = self._train_model(input_fn, hooks, saving_listeners)\n")
      .append("  File \"/grid/b/tmp/yarn/usercache/user2/appcache/application_1576097000949_37909/container_e42_1576097000949_37909_01_000002/venv/Python/lib/python3.6/site-packages/tensorflow/python/framework/errors_impl.py\", line 528, in __exit__\n")
      .append("    c_api.TF_GetCode(self.status.status))\n")
      .append("tensorflow.python.framework.errors_impl.InvalidArgumentError: Unsuccessful TensorSliceReader constructor: Failed to get matching files on hdfs://namenode.linkedin.com:9000/folder/GBert/bert-base/checkpoints/model.ckpt-1724730: Not found: hdfs://namenode.linkedin.com:9000/folder/GBert/bert-base/checkpoints; No such file or directory\n")
      .append("\n")
      .append("\n")
      .append("[2020-02-05 02:56:18.081]Container exited with a non-zero exit code 1. Error file: prelaunch.err.\n")
      .append("Last 4096 bytes of prelaunch.err :\n") .append("Last 4096 bytes of stderr :\n")
      .append("ib/python3.6/site-packages/tensorflow/python/estimator/estimator.py\", line 354, in train\n")
      .append("    loss = self._train_model(input_fn, hooks, saving_listeners)\n")
      .append("  File \"/grid/b/tmp/yarn/usercache/user2/appcache/application_1576097000949_37909/container_e42_1576097000949_37909_01_000002/venv/Python/lib/python3.6/site-packages/tensorflow/python/estimator/estimator.py\", line 1207, in _train_model\n")
      .append("    return self._train_model_default(input_fn, hooks, saving_listeners)\n")
      .append("  File \"/grid/b/tmp/yarn/usercache/user2/appcache/application_1576097000949_37909/container_e42_1576097000949_37909_01_000002/venv/Python/lib/python3.6/site-packages/tensorflow/python/pywrap_tensorflow_internal.py\", line 326, in NewCheckpointReader\n")
      .append("    return CheckpointReader(compat.as_bytes(filepattern), status)\n")
      .append("  File \"/grid/b/tmp/yarn/usercache/user2/appcache/application_1576097000949_37909/container_e42_1576097000949_37909_01_000002/venv/Python/lib/python3.6/site-packages/tensorflow/python/framework/errors_impl.py\", line 528, in __exit__\n")
      .append("    c_api.TF_GetCode(self.status.status))\n")
      .append("tensorflow.python.framework.errors_impl.InvalidArgumentError: Unsuccessful TensorSliceReader constructor: Failed to get matching files on hdfs://namenode.linkedin.com:9000/folder/GBert/bert-base/checkpoints/model.ckpt-1724730: Not found: hdfs://namenode.linkedin.com:9000/folder/GBert/bert-base/checkpoints; No such file or directory\n")
      .append("\n")
      .append("\n")
      .append("</pre>\n</table>\n</body>\n</html>\n").toString();

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        logger.info("Starting FakeApplication for Testing Tony_EF");
      }
    };
    fakeApp = fakeApplication(dbConn, gs);

    //Configuration load for regex pattern etc..
    ExceptionUtils.ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());

    setUpAndStartMockSchedulerServer();
  }

  @After
  public void stop() {
    Helpers.stop(fakeApp);
    stopMockSchedulerServer();
  }

  private void setUpAndStartMockSchedulerServer() {
    _wireMockServer = new WireMockServer(MOCK_RM_PORT);
    _wireMockServer.start();
  }

  private void stopMockSchedulerServer() {
    _wireMockServer.stop();
  }

  private AnalyticJob getFakeAnalyticalJob(String appId, String jobName, boolean isSucceeded, String amContainerLogsURL, String amDiagnostic) {
    return new AnalyticJob()
        .setAppId(appId)
        .setName(jobName)
        .setSucceeded(isSucceeded)
        .setAmContainerLogsURL(amContainerLogsURL)
        .setJobDiagnostics(amDiagnostic)
        .setAppType(new ApplicationType("TonY"));
  }

  private AppResult getFakeAppResult(String id, String jobExecUrl, String flowExecUrl) {
    AppResult fakeAppResult = new AppResult();
    fakeAppResult.id = id;
    fakeAppResult.jobExecUrl = jobExecUrl;
    fakeAppResult.flowExecUrl = flowExecUrl;
    return fakeAppResult;
  }

  private void mockResponseForContainerLogs(String containerUrl, String response, int statusCode) {
    _wireMockServer.stubFor(get(urlEqualTo(containerUrl))
        .willReturn(aResponse()
            .withBody(response)
            .withStatus(statusCode)));
  }

  @Test
  public void testTonYExceptionFingerprinting()
  {
    running(testServer(TEST_SERVER_PORT, fakeApp), () -> {
      try {
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_1).getPath() + stderrContainerLogParameters,
            MOCK_RESPONSE_1, OK);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_1).getPath() + stdoutContainerLogParameters,
            "", OK);
      } catch (MalformedURLException ex) {
        logger.error("URL for test is not formed properly");
      }
      AnalyticJob fakeJob1 =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_1, TEST_JOB_NAME_1, false, TEST_AM_LOG_CONTAINER_URL_1, "diagnostic 1");
      AppResult fakeAppResult1 = getFakeAppResult(TEST_APPLICATION_ID_1, TEST_JOB_EXEC_URL_1, TEST_WORKFLOW_URL_1);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob1, fakeAppResult1);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(4, exceptionInfos.size());
      assertEquals("Job Diagnostics", exceptionInfos.get(0).getExceptionName());
      assertEquals("Job Diagnostics: \n" + fakeJob1.getJobDiagnostics(), exceptionInfos.get(0).getExceptionStackTrace());
      assertEquals("Container exited with a non-zero exit code 1. Error file: prelaunch.err.", exceptionInfos.get(1).getExceptionName());
    });
  }

  @Test
  public void testTonyExceptionFingerprintingWhenStderrLogEmpty()
  {
    running(testServer(TEST_SERVER_PORT, fakeApp), () -> {
      try {
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_2).getPath() + stderrContainerLogParameters,
            "", OK);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_2).getPath() + stdoutContainerLogParameters,
            MOCK_RESPONSE_2, OK);
      } catch (MalformedURLException ex) {
        logger.error("URL for test is not formed properly");
      }
      AnalyticJob fakeJob =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_2, TEST_JOB_NAME_2, false, TEST_AM_LOG_CONTAINER_URL_2, "Exit with status code 1.");
      AppResult fakeAppResult = getFakeAppResult(TEST_APPLICATION_ID_2, TEST_JOB_EXEC_URL_2, TEST_WORKFLOW_URL_2);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob, fakeAppResult);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(4, exceptionInfos.size());
      assertEquals("Job Diagnostics", exceptionInfos.get(0).getExceptionName());
      assertEquals("Job Diagnostics: \n" + fakeJob.getJobDiagnostics(), exceptionInfos.get(0)
          .getExceptionStackTrace());
      assertEquals("Container exited with a non-zero exit code 1. Error file: prelaunch.err.",
          exceptionInfos.get(1).getExceptionName());
      assertEquals("ERROR ApplicationMaster:983 - [2020-02-05 02:56:18.039]Exception from container-launch.",
          exceptionInfos.get(3).getExceptionName());
      assertEquals(ExceptionUtils.ConfigurationBuilder.NUMBER_OF_STACKTRACE_LINE.getValue() + 1,
          (exceptionInfos.get(3).getExceptionStackTrace().split("\n")).length);
    });
  }


  @Test
  public void testTonyExceptionFingerprintingWhenNoLogFound()
  {
    running(testServer(TEST_SERVER_PORT, fakeApp), () -> {
      try {
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_3).getPath() + stderrContainerLogParameters,
            "", NOT_FOUND);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_3).getPath() + stdoutContainerLogParameters,
            "", NOT_FOUND);
      } catch (MalformedURLException ex) {
        logger.error("URL for test is not formed properly");
      }
      AnalyticJob fakeJob =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_3, TEST_JOB_NAME_3, false, TEST_AM_LOG_CONTAINER_URL_3, "Exit with status code 1.");
      AppResult fakeAppResult = getFakeAppResult(TEST_APPLICATION_ID_3, TEST_JOB_EXEC_URL_3, TEST_WORKFLOW_URL_3);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob, fakeAppResult);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(1, exceptionInfos.size());
      assertEquals("Job Diagnostics", exceptionInfos.get(0).getExceptionName());
      assertEquals("Job Diagnostics: \n" + fakeJob.getJobDiagnostics(), exceptionInfos.get(0)
          .getExceptionStackTrace());
    });
  }
}
