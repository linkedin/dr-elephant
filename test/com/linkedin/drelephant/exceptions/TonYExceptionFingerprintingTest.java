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
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import models.AppResult;
import org.apache.commons.io.IOUtils;
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
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;
import static play.test.Helpers.*;

public class TonYExceptionFingerprintingTest {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private static FakeApplication fakeApp;
  private WireMockServer _wireMockServer;

  private String stderrContainerLogParameters = "/amstderr.log?start=0";
  private String stdoutContainerLogParameters = "/amstdout.log?start=0";

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

  private final String FAKE_RESPONSE_APP_1_STDERR_PATH = "test/resources/exception/TonY/app_1_stderr_response.html";
  private final String FAKE_RESPONSE_APP_1_STDOUT_PATH = "test/resources/exception/TonY/app_1_stdout_response.html";
  private final String FAKE_RESPONSE_APP_2_STDERR_PATH = "test/resources/exception/TonY/app_2_stderr_response.html";
  private final String FAKE_RESPONSE_APP_2_STDOUT_PATH = "test/resources/exception/TonY/app_2_stdout_response.html";
  private final String FAKE_RESPONSE_APP_STDERR_PATH = "test/resources/exception/TonY/app_stderr_response.html";

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
            getFakeResponse(FAKE_RESPONSE_APP_1_STDERR_PATH), OK);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_1).getPath() + stdoutContainerLogParameters,
            getFakeResponse(FAKE_RESPONSE_APP_1_STDOUT_PATH), OK);
      } catch (MalformedURLException ex) {
        logger.error("URL for test is not formed properly");
      }
      AnalyticJob fakeJob1 =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_1, TEST_JOB_NAME_1, false, TEST_AM_LOG_CONTAINER_URL_1,
              "diagnostic 1");
      AppResult fakeAppResult1 = getFakeAppResult(TEST_APPLICATION_ID_1, TEST_JOB_EXEC_URL_1, TEST_WORKFLOW_URL_1);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob1, fakeAppResult1);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(3, exceptionInfos.size());
      assertEquals("Job Diagnostics", exceptionInfos.get(0).getExceptionName());
      assertEquals("Job Diagnostics: \n" + fakeJob1.getJobDiagnostics(), exceptionInfos.get(0)
          .getExceptionStackTrace());
      assertEquals("Container exited with a non-zero exit code 1. Error file: prelaunch.err.",
          exceptionInfos.get(1).getExceptionName());
      assertEquals("USER_ERROR/FILE_NOT_FOUND", tonyEF.classifyException());
    });
  }

  @Test
  public void testTonyExceptionFingerprintingWhenStderrLogEmpty()
  {
    running(testServer(TEST_SERVER_PORT, fakeApp), () -> {
      try {
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_2).getPath() + stderrContainerLogParameters,
            getFakeResponse(FAKE_RESPONSE_APP_2_STDERR_PATH), OK);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_2).getPath() + stdoutContainerLogParameters,
            getFakeResponse(FAKE_RESPONSE_APP_2_STDOUT_PATH), OK);
      } catch (MalformedURLException ex) {
        logger.error("URL for test is not formed properly");
      }
      AnalyticJob fakeJob =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_2, TEST_JOB_NAME_2, false, TEST_AM_LOG_CONTAINER_URL_2,
              "Exit with status code 1.");
      AppResult fakeAppResult = getFakeAppResult(TEST_APPLICATION_ID_2, TEST_JOB_EXEC_URL_2, TEST_WORKFLOW_URL_2);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob, fakeAppResult);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(5, exceptionInfos.size());
      assertEquals("Job Diagnostics", exceptionInfos.get(0).getExceptionName());
      assertEquals("Job Diagnostics: \n" + fakeJob.getJobDiagnostics(), exceptionInfos.get(0)
          .getExceptionStackTrace());
      assertEquals("Container exited with a non-zero exit code 1. Error file: prelaunch.err.",
          exceptionInfos.get(1).getExceptionName());
      assertEquals("ERROR ApplicationMaster:983 - [2020-02-05 02:56:19.426]Container killed by the ApplicationMaster.",
          exceptionInfos.get(4).getExceptionName());
      assertEquals(ExceptionUtils.ConfigurationBuilder.NUMBER_OF_STACKTRACE_LINE.getValue() + 1,
          (exceptionInfos.get(4).getExceptionStackTrace().split("\n")).length);
      assertEquals("USER_ERROR/FILE_NOT_FOUND", tonyEF.classifyException());
    });
  }

  @Test
  public void testTonyExceptionFingerprintingWhenNoLogFound() {
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
          getFakeAnalyticalJob(TEST_APPLICATION_ID_3, TEST_JOB_NAME_3, false, TEST_AM_LOG_CONTAINER_URL_3,
              "Exit with status code 1.");
      AppResult fakeAppResult = getFakeAppResult(TEST_APPLICATION_ID_3, TEST_JOB_EXEC_URL_3, TEST_WORKFLOW_URL_3);

      TonYExceptionFingerprinting tonYExceptionFingerprintingSpy = spy(new TonYExceptionFingerprinting(fakeJob, fakeAppResult));
      List<ExceptionInfo> mockExceptionInfoListForAzkaban = new ArrayList<>();
      mockExceptionInfoListForAzkaban.add(getMockExceptionInfo("exceptionName_1",
          ExceptionInfo.ExceptionSource.SCHEDULER, "log_1", 1));
      mockExceptionInfoListForAzkaban.add(getMockExceptionInfo("exceptionName_2",
          ExceptionInfo.ExceptionSource.SCHEDULER, "log_2", 2));
      try {
        doReturn(mockExceptionInfoListForAzkaban).when(tonYExceptionFingerprintingSpy).getAzkabanExceptionInfoResults();
        tonYExceptionFingerprintingSpy.doExceptionPrinting();
        verify(tonYExceptionFingerprintingSpy, times(1)).getAzkabanExceptionInfoResults();
        List<ExceptionInfo> results = tonYExceptionFingerprintingSpy.get_exceptionInfoList();
        assertEquals(3, results.size());
        assertEquals("Job Diagnostics", results.get(0).getExceptionName());
        assertEquals("exceptionName_1", results.get(1).getExceptionName());
        assertEquals("exceptionName_2", results.get(2).getExceptionName());
        assertEquals("log_1", results.get(1).getExceptionStackTrace());
        assertEquals("log_2", results.get(2).getExceptionStackTrace());
        assertEquals("Job Diagnostics: \n" + fakeJob.getJobDiagnostics(), results.get(0).getExceptionStackTrace());
        assertEquals("USER_ERROR/UNKNOWN", tonYExceptionFingerprintingSpy.classifyException());
      } catch (Exception ex) {
        logger.info("Exception while mocking method getAzkabanExceptionInfoResults");
      }
    });
  }

  @Test
  public void testTonyExceptionFingerprintingWhenSimilarLogsArePresent()
  {
    running(testServer(TEST_SERVER_PORT, fakeApp), () -> {
      try {
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_1).getPath() + stderrContainerLogParameters,
            getFakeResponse(FAKE_RESPONSE_APP_STDERR_PATH), OK);
        mockResponseForContainerLogs(new URL(TEST_AM_LOG_CONTAINER_URL_1).getPath() + stdoutContainerLogParameters,
            "", OK);
      } catch (MalformedURLException ex) {
        logger.error("URL for test case is not formed properly");
      }
      AnalyticJob fakeJob =
          getFakeAnalyticalJob(TEST_APPLICATION_ID_1, TEST_JOB_NAME_1, false, TEST_AM_LOG_CONTAINER_URL_1,
              "Exit with status code 1.");
      AppResult fakeAppResult = getFakeAppResult(TEST_APPLICATION_ID_3, TEST_JOB_EXEC_URL_3, TEST_WORKFLOW_URL_3);
      TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(fakeJob, fakeAppResult);
      tonyEF.doExceptionPrinting();
      List<ExceptionInfo> exceptionInfos = tonyEF.get_exceptionInfoList();
      assertEquals(6, exceptionInfos.size());
      assertTrue(exceptionInfos.get(2).getExceptionStackTrace().contains("Container exited with a non-zero exit code 1."
          + " Error file: prelaunch.err.\n" + "Last 4096 bytes of prelaunch.err :\n" + "Last 4096 bytes of stderr :"));
      assertTrue(exceptionInfos.get(3).getExceptionStackTrace().contains("ERROR ApplicationMaster:983 - "
          + "[2020-02-07 06:51:28.868]Container [pid=15762,containerID=container_e42_123456789568_34567_01_000027] is "
          + "running beyond physical memory limits. Current usage: 32.0 GB of 32 GB physical memory used; 102.9 GB of "
          + "67.2 GB virtual memory used. Killing container.\n"
          + "Dump of the process-tree for container_e42_123456789568_34567_01_000027"));
      /*
        Log has two similar stackTraces and only difference between them is containerId
        Log with containerId container_e42_123456789568_34567_01_000008 will be present in result and not with containerId
        container_e42_123456789568_34567_01_000009
       */
      List<ExceptionInfo> similarLogList = exceptionInfos.stream()
          .filter(el -> el.getExceptionStackTrace().contains("container_e42_123456789568_34567_01_000009"))
          .collect(Collectors.toList());
      assertEquals(0, similarLogList.size());
    });
  }

  @Test
  public void testTonyCategorizationInfraError() {
    ExceptionInfo exceptionInfo = new ExceptionInfo(1, "__exit__\n" + "    c_api.TF_GetCode(self.status.status))\n"
        + "tensorflow.python.framework.errors_impl.UnknownError: Could not start gRPC server\n",
        "__exit__\n" + "    c_api.TF_GetCode(self.status.status))\n"
            + "tensorflow.python.framework.errors_impl.UnknownError: Could not start gRPC server\n",
        ExceptionInfo.ExceptionSource.DRIVER, 5, "fakeURL");
    TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(null, null);
    tonyEF.get_exceptionInfoList().add(exceptionInfo);
    assertEquals("TONY_INFRA_ERROR", tonyEF.classifyException());
  }

  @Test
  public void testTonyCategorizationTensorFlowError() {
    ExceptionInfo exceptionInfo = new ExceptionInfo(1, "test",
        "usERKNS1_4ArgsESB_RKNS_6TensorEbEE+0xbb)[0x7f5a30faedbb]\n"
            + "*** Error in `venv/Python/bin/python': corrupted double-linked list: 0x00007f4fc21857c0 \n",
        ExceptionInfo.ExceptionSource.DRIVER, 5, "fakeURL");
    TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(null, null);
    tonyEF.get_exceptionInfoList().add(exceptionInfo);
    assertEquals("TENSORFLOW_ERROR", tonyEF.classifyException());
    tonyEF.get_exceptionInfoList().add(new ExceptionInfo(1, "__exit__\n" + "    c_api.TF_GetCode(self.status.status))\n"
        + "tensorflow.python.framework.errors_impl.UnknownError: Could not start gRPC server\n",
        "__exit__\n" + "    c_api.TF_GetCode(self.status.status))\n"
            + "tensorflow.python.framework.errors_impl.UnknownError: Could not start gRPC server\n",
        ExceptionInfo.ExceptionSource.DRIVER, 3, "fakeURL"));
    //Since Tony infra error have higher priority.
    assertEquals("TONY_INFRA_ERROR", tonyEF.classifyException());
  }

  @Test
  public void testTonyCategorizationUserError() {
    ExceptionInfo exceptionInfo = new ExceptionInfo(1, "test",
        "keyError: field does not exist",
        ExceptionInfo.ExceptionSource.DRIVER, 5, "fakeURL");
    TonYExceptionFingerprinting tonyEF = new TonYExceptionFingerprinting(null, null);
    tonyEF.get_exceptionInfoList().add(exceptionInfo);
    assertEquals("USER_ERROR/PYTHON_ERROR", tonyEF.classifyException());
    tonyEF.get_exceptionInfoList().clear();
    tonyEF.get_exceptionInfoList().add(new ExceptionInfo(1, "test",
        "net.lingala.zip4j.exception.ZipException: Probably not a zip file or a corrupted zip file\n",
        ExceptionInfo.ExceptionSource.DRIVER, 3, "fakeURL"));
    //Since Tony infra error have higher priority.
    assertEquals("USER_ERROR/ZIP_EXCEPTION", tonyEF.classifyException());

    tonyEF.get_exceptionInfoList().clear();
    tonyEF.get_exceptionInfoList().add(new ExceptionInfo(1, "test",
        "hdfsPread: NewByteArray error:\n" + "java.lang.OutOfMemoryError: Java heap space\n",
        ExceptionInfo.ExceptionSource.DRIVER, 3, "fakeURL"));
    //Since Tony infra error have higher priority.
    assertEquals("USER_ERROR/OUT_OF_MEMORY", tonyEF.classifyException());

    tonyEF.get_exceptionInfoList().clear();
    tonyEF.get_exceptionInfoList().add(new ExceptionInfo(1, "test",
        " File \"./tensorflow-starter-kit.pyz/_bootstrap/interpreter.py\", line 12, in _exec_function\n"
            + "  File \"linkedin/tensorflowstarterkit/tony/mnist_single_node_tony.py\", line 17, in <module>\n"
            + "    exec(open(activate_this).read(), dict(__file__=activate_this))\n"
            + "FileNotFoundError: [Errno 2] No such file or directory: './venv/Python/bin/activate_this.py'\n",
        ExceptionInfo.ExceptionSource.DRIVER, 3, "fakeURL"));
    //Since Tony infra error have higher priority.
    assertEquals("USER_ERROR/FILE_NOT_FOUND", tonyEF.classifyException());


    TonYExceptionFingerprinting tonyEFNoData = new TonYExceptionFingerprinting(null, null);
    assertEquals("CANNOT CLASSIFY DATA", tonyEFNoData.classifyException());

  }


  private String getFakeResponse(String path) {
    try {
      FileInputStream inputStream = new FileInputStream(path);
      return IOUtils.toString(inputStream);
    } catch (IOException ex) {
      logger.error("Exception while parsing fake response");
      return "";
    }
  }

  private ExceptionInfo getMockExceptionInfo(String exceptionName, ExceptionInfo.ExceptionSource exceptionSource,
      String exceptionStackTrace, int exceptionWeight) {
    return new ExceptionInfo(exceptionName.hashCode(), exceptionName, exceptionStackTrace,
        exceptionSource, exceptionWeight, "");
  }
}