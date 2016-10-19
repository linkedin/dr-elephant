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

package com.linkedin.drelephant.mapreduce.fetchers;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfiguration;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class MapReduceFSFetcherHadoop2Test {

  private static Document document9 = null;
  private static Document document10 = null;
  private static Document document11 = null;

  @BeforeClass
  public static void before() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      ClassLoader classLoader = MapReduceFSFetcherHadoop2Test.class.getClassLoader();
      document9 = builder.parse(classLoader.getResourceAsStream(
              "configurations/fetcher/FetcherConfTest9.xml"));
      document10 = builder.parse(classLoader.getResourceAsStream(
              "configurations/fetcher/FetcherConfTest10.xml"));
      document11 = builder.parse(classLoader.getResourceAsStream(
              "configurations/fetcher/FetcherConfTest11.xml"));
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException("Test files are not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read test files ", e);
    }
  }

  @Test
  public void testFetcherDefaultConfig() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document9.getDocumentElement());
    try {
      MapReduceFSFetcherHadoop2 fetcher = new MapReduceFSFetcherHadoop2(
              fetcherConf.getFetchersConfigurationData().get(0));
      Assert.assertFalse("Sampling should be disabled in default", fetcher.isSamplingEnabled());
      Assert.assertEquals(fetcher.DEFALUT_MAX_LOG_SIZE_IN_MB, fetcher.getMaxLogSizeInMB(), 0.0001);

      List<Object> list = new ArrayList<Object>();
      int listLen = fetcher.MAX_SAMPLE_SIZE * 2;
      for (int i = 0; i < listLen; i++) {
        list.add(0);
      }
      Assert.assertEquals("Should not sample task list when sampling is disabled", listLen,
              fetcher.sampleAndGetSize("appId", list));
    } catch (IOException e) {
      Assert.assertNull("Failed to initialize FileSystem", e);
    }
  }

  @Test
  public void testFetcherConfig() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document10.getDocumentElement());
    try {
      MapReduceFSFetcherHadoop2 fetcher = new MapReduceFSFetcherHadoop2(
              fetcherConf.getFetchersConfigurationData().get(0));
      Assert.assertTrue("Failed to enable sampling", fetcher.isSamplingEnabled());
      Assert.assertEquals(200d, fetcher.getMaxLogSizeInMB(), 0.0001);

      List<Object> list = new ArrayList<Object>();
      int listLen = fetcher.MAX_SAMPLE_SIZE * 2;
      for (int i = 0; i < listLen; i++) {
        list.add(0);
      }
      Assert.assertEquals("Should sample task list when sampling is enabled", fetcher.MAX_SAMPLE_SIZE,
              fetcher.sampleAndGetSize("appId", list));
    } catch (IOException e) {
      Assert.assertNull("Failed to initialize FileSystem", e);
    }
  }

  @Test
  public void testFetcherEmptyConf() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document11.getDocumentElement());
    try {
      MapReduceFSFetcherHadoop2 fetcher = new MapReduceFSFetcherHadoop2(
              fetcherConf.getFetchersConfigurationData().get(0));
      Assert.assertFalse("Sampling should be disabled in default", fetcher.isSamplingEnabled());
      Assert.assertEquals(fetcher.DEFALUT_MAX_LOG_SIZE_IN_MB, fetcher.getMaxLogSizeInMB(), 0.0001);

      List<Object> list = new ArrayList<Object>();
      int listLen = fetcher.MAX_SAMPLE_SIZE * 2;
      for (int i = 0; i < listLen; i++) {
        list.add(0);
      }
      Assert.assertEquals("Should not sample task list when sampling is disabled", listLen,
              fetcher.sampleAndGetSize("appId", list));
    } catch (IOException e) {
      Assert.assertNull("Failed to initialize FileSystem", e);
    }
  }

  @Test
  public void testGetHistoryDir() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document9.getDocumentElement());
    try {
      MapReduceFSFetcherHadoop2 fetcher = new MapReduceFSFetcherHadoop2(
              fetcherConf.getFetchersConfigurationData().get(0));
      Calendar timestamp = Calendar.getInstance();
      timestamp.set(2016, Calendar.JULY, 30);
      AnalyticJob job = new AnalyticJob()
              .setAppId("application_1461566847127_84624")
              .setFinishTime(timestamp.getTimeInMillis());

      String expected = StringUtils.join(new String[]{fetcher.getHistoryLocation(), "2016", "07", "30", "000084", ""}, File.separator);
      Assert.assertEquals("Error history directory", expected, fetcher.getHistoryDir(job));
    } catch (IOException e) {
      Assert.assertNull("Failed to initialize FileSystem", e);
    }
  }
}
