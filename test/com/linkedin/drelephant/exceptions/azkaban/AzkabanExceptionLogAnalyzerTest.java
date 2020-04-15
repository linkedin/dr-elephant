package com.linkedin.drelephant.exceptions.azkaban;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.clients.WorkflowClient;
import com.linkedin.drelephant.clients.azkaban.AzkabanWorkflowClient;
import com.linkedin.drelephant.exceptions.util.ExceptionInfo;
import com.linkedin.drelephant.exceptions.util.ExceptionUtils;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;
import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.*;
import static common.TestConstants.*;
import static org.mockito.Mockito.*;


public class AzkabanExceptionLogAnalyzerTest {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  WorkflowClient mockWorkflowClient = mock(AzkabanWorkflowClient.class);

  private final String MOCK_LOG_PATH_1 = "test/resources/exception/azkaban/azkabanExceptionLog_1.log.txt";

  @Before
  public void setUp() {
    ExceptionUtils.ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  @Test
  public void testAzkabanExceptionLogAnalyzer() {
    AzkabanExceptionLogAnalyzer azkabanExceptionLogAnalyzerSpy = spy(new AzkabanExceptionLogAnalyzer(
        TEST_FLOW_EXEC_ID1, TEST_JOB_EXEC_ID1));
    String jobName = getJobName(TEST_JOB_EXEC_ID1);
    String jobLog = getLog(MOCK_LOG_PATH_1);
    when(mockWorkflowClient.getAzkabanJobLog(jobName, AZKABAN_JOB_LOG_START_OFFSET.getValue()
        .toString(), AZKABAN_JOB_LOG_MAX_LENGTH.getValue().toString())).thenReturn(jobLog);
    doReturn(mockWorkflowClient).when(azkabanExceptionLogAnalyzerSpy).getWorkflowClient(Mockito.any(String.class));
    azkabanExceptionLogAnalyzerSpy.getExceptionInfoList();

    List<ExceptionInfo> result = azkabanExceptionLogAnalyzerSpy.getExceptionInfoList();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(1, result.get(0).getWeightOfException().intValue());
    Assert.assertEquals(ExceptionInfo.ExceptionSource.SCHEDULER, result.get(0).getExceptionSource());
    Assert.assertEquals("Exception in thread \"main\" java.lang.reflect.UndeclaredThrowableException\n",
        result.get(0).getExceptionName());
    Assert.assertEquals(2, result.get(1).getWeightOfException().intValue());
    Assert.assertEquals(ExceptionInfo.ExceptionSource.SCHEDULER, result.get(1).getExceptionSource());
    Assert.assertEquals("java.lang.RuntimeException: azkaban.jobExecutor.utils.process.ProcessFailureException:"
            + " Process exited with code 1\n", result.get(1).getExceptionName());
  }

  @Test
  public void testAzkabanExceptionLogAnalyzerWhenNoLogFound() {
    AzkabanExceptionLogAnalyzer azkabanExceptionLogAnalyzerSpy = spy(new AzkabanExceptionLogAnalyzer(
        TEST_FLOW_EXEC_ID1, TEST_JOB_EXEC_ID1));
    String jobName = getJobName(TEST_JOB_EXEC_ID1);
    when(mockWorkflowClient.getAzkabanJobLog(jobName, AZKABAN_JOB_LOG_START_OFFSET.getValue()
        .toString(), AZKABAN_JOB_LOG_MAX_LENGTH.getValue().toString())).thenReturn("");
    doReturn(mockWorkflowClient).when(azkabanExceptionLogAnalyzerSpy).getWorkflowClient(Mockito.any(String.class));
    azkabanExceptionLogAnalyzerSpy.getExceptionInfoList();
    List<ExceptionInfo> result = azkabanExceptionLogAnalyzerSpy.getExceptionInfoList();
    Assert.assertEquals(0, result.size());
  }

  private String getLog(String path) {
    try {
      FileInputStream inputStream = new FileInputStream(path);
      return IOUtils.toString(inputStream);
    } catch (IOException ex) {
      logger.error("Exception while reading mock logs from path " + path);
      return "";
    }
  }
}
