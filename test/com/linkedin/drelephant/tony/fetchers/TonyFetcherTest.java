/*
 * Copyright 2019 LinkedIn Corp.
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
package com.linkedin.drelephant.tony.fetchers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.tony.data.TonyApplicationData;
import com.linkedin.drelephant.tony.data.TonyTaskData;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.events.Metric;
import com.linkedin.tony.events.TaskFinished;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TonyFetcherTest {
  private static final String APPLICATION_ID_1 = "application_123_456";
  private static final String APPLICATION_ID_2 = "application_789_101";
  private static File _intermediateDir;
  private static File _finishedDir;
  private static String _tonyConfDir;
  private static Date _endDate;

  @BeforeClass
  public static void setup() throws IOException, ParseException {
    setupTestData();
    setupTestTonyConfDir();
  }

  private static void setupTestData() throws IOException, ParseException {
    String yearMonthDay = "2019/05/02";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    _endDate = sdf.parse(yearMonthDay);

    File tempDir = Files.createTempDir();
    _intermediateDir = new File(tempDir, "intermediate");
    _finishedDir = new File(tempDir, "finished");

    createAppHistoryDir(new File(_intermediateDir, APPLICATION_ID_1));
    createAppHistoryDir(new File(_finishedDir, yearMonthDay + Path.SEPARATOR + APPLICATION_ID_2));
  }

  private static void createAppHistoryDir(File appDir) throws IOException {
    appDir.mkdirs();

    // write fake config history file
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    File configFile = new File(appDir, Constants.TONY_FINAL_XML);
    conf.writeXml(new FileOutputStream(configFile));

    // create fake events
    Event event0 = new Event(EventType.TASK_FINISHED, new TaskFinished("worker", 0, "SUCCEEDED",
        ImmutableList.of(new Metric("my_metric", 0.0))), System.currentTimeMillis());
    Event event1 = new Event(EventType.TASK_FINISHED, new TaskFinished("worker", 1, "SUCCEEDED",
        ImmutableList.of(new Metric("my_metric", 1.0))), System.currentTimeMillis());
    Event event2 = new Event(EventType.TASK_FINISHED, new TaskFinished("ps", 0, "SUCCEEDED",
        ImmutableList.of(new Metric("my_metric", 0.0))), System.currentTimeMillis());

    // write fake events history file
    File eventFile = new File(appDir,
        APPLICATION_ID_1 + "-0-" + _endDate.getTime() + "-user1-SUCCEEDED." + Constants.HISTFILE_SUFFIX);
    DatumWriter<Event> userDatumWriter = new SpecificDatumWriter<>(Event.class);
    DataFileWriter<Event> dataFileWriter = new DataFileWriter<>(userDatumWriter);
    dataFileWriter.create(event0.getSchema(), eventFile);
    dataFileWriter.append(event0);
    dataFileWriter.append(event1);
    dataFileWriter.append(event2);
    dataFileWriter.close();
  }

  private static void setupTestTonyConfDir() throws IOException {
    Configuration testTonyConf = new Configuration(false);
    testTonyConf.set(TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE, _intermediateDir.getPath());
    testTonyConf.set(TonyConfigurationKeys.TONY_HISTORY_FINISHED, _finishedDir.getPath());

    File confDir = Files.createTempDir();
    _tonyConfDir = confDir.getPath();
    File tonySiteFile = new File(confDir, Constants.TONY_SITE_CONF);
    testTonyConf.writeXml(new FileOutputStream(tonySiteFile));
  }

  @Test
  public void testFetchDataIntermediateDir() throws Exception {
    testHelper(APPLICATION_ID_1);
  }

  @Test
  public void testFetchDataFinishedDir() throws Exception {
    testHelper(APPLICATION_ID_2);
  }

  private static void testHelper(String appId) throws Exception {
    FetcherConfigurationData configData = new FetcherConfigurationData(null, null,
        ImmutableMap.of(Constants.TONY_CONF_DIR, _tonyConfDir,
            TonyConfigurationKeys.TONY_HISTORY_FINISHED_DIR_TIMEZONE,
            TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED_DIR_TIMEZONE));
    TonyFetcher tonyFetcher = new TonyFetcher(configData);

    AnalyticJob job = new AnalyticJob();
    ApplicationType tonyAppType = new ApplicationType(Constants.APP_TYPE);
    job.setFinishTime(_endDate.getTime());
    job.setAppId(appId);
    job.setAppType(tonyAppType);
    TonyApplicationData appData = tonyFetcher.fetchData(job);

    Assert.assertEquals(appId, appData.getAppId());
    Assert.assertEquals(tonyAppType, appData.getApplicationType());
    Assert.assertEquals("bar", appData.getConf().get("foo"));
    Map<String, Map<Integer, TonyTaskData>> metricsMap = appData.getTaskMap();
    Assert.assertEquals(2, metricsMap.size());
    Assert.assertEquals(2, metricsMap.get("worker").size());
    Assert.assertEquals(1, metricsMap.get("ps").size());
  }
}