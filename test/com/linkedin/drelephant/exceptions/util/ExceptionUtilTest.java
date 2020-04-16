package com.linkedin.drelephant.exceptions.util;

import com.linkedin.drelephant.ElephantContext;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.drelephant.exceptions.util.ExceptionUtils.ConfigurationBuilder.*;
import static org.junit.Assert.*;


public class ExceptionUtilTest {

  private final String LOG_RESOURCE_PATH_1 = "test/resources/exception/util/testLogSimilarityResource_1.log";
  private final String LOG_RESOURCE_PATH_2 = "test/resources/exception/util/testLogSimilarityResource_2.log";
  private final String LOG_RESOURCE_PATH_3 = "test/resources/exception/util/testLogSimilarityResource_3.log";
  private final String LOG_RESOURCE_PATH_4 = "test/resources/exception/util/testLogSimilarityResource_4.log";

  @Before
  public void setup() {
    //Configuration load for regex pattern etc..
    ExceptionUtils.ConfigurationBuilder.buildConfigurations(ElephantContext.instance().getAutoTuningConf());
  }

  @Test
  public void testLogGetSimilarityPercentage() {
    String
        log_1 = getMockLogs(LOG_RESOURCE_PATH_1),
        log_2 = getMockLogs(LOG_RESOURCE_PATH_2),
        log_3 = getMockLogs(LOG_RESOURCE_PATH_3),
        log_4 = getMockLogs(LOG_RESOURCE_PATH_4);

    assertTrue(isSimilar(ExceptionUtils.getLogSimilarityPercentage(log_1, log_2)));
    assertFalse(isSimilar(ExceptionUtils.getLogSimilarityPercentage(log_1, log_3)));
    assertFalse(isSimilar(ExceptionUtils.getLogSimilarityPercentage(log_2, log_3)));
    assertFalse(isSimilar(ExceptionUtils.getLogSimilarityPercentage(log_1, log_4)));
    assertFalse(isSimilar(ExceptionUtils.getLogSimilarityPercentage(log_3, log_4)));
  }

  private String getMockLogs(String path) {
    try {
      FileInputStream inputStream = new FileInputStream(path);
      return IOUtils.toString(inputStream);
    } catch (IOException ex) {
      return "";
    }
  }

  private boolean isSimilar(int similarityPercentage) {
    return similarityPercentage > MAX_LOG_SIMILARITY_PERCENTAGE_THRESHOLD.getValue();
  }
}
