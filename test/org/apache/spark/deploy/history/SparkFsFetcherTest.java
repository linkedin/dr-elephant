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

package org.apache.spark.deploy.history;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfiguration;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.util.HadoopUtils;
import com.linkedin.drelephant.util.HadoopUtilsTest;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import static org.junit.Assert.assertEquals;

public class SparkFsFetcherTest {

  private static Document document1 = null;
  private static Document document2 = null;
  private static Document document3 = null;
  private static Document document4 = null;

  private static final String spark = "SPARK";
  private static final String defEventLogDir = "/system/spark-history";
  private static final String confEventLogDir = "/custom/configured";
  private static final double defEventLogSize = 100;
  private static final double confEventLogSize = 50;

  @BeforeClass
  public static void runBeforeClass() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document1 = builder.parse(
              SparkFsFetcherTest.class.getClassLoader().getResourceAsStream(
                      "configurations/fetcher/FetcherConfTest5.xml"));
      document2 = builder.parse(
              SparkFsFetcherTest.class.getClassLoader().getResourceAsStream(
                      "configurations/fetcher/FetcherConfTest6.xml"));
      document3 = builder.parse(
              SparkFsFetcherTest.class.getClassLoader().getResourceAsStream(
                      "configurations/fetcher/FetcherConfTest7.xml"));
      document4 = builder.parse(
              SparkFsFetcherTest.class.getClassLoader().getResourceAsStream(
                      "configurations/fetcher/FetcherConfTest8.xml"));
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException("Test files are not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read test files ", e);
    }
  }

  private static Document parseDocument(String resourcePath) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    return builder.parse(SparkFsFetcherTest.class.getClassLoader().getResourceAsStream(resourcePath));
  }

  @Test
  public void testSparkFetcherConfig() throws Exception {
    Document document = parseDocument("configurations/fetcher/FetcherConfTest5.xml");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    FetcherConfigurationData fetcherConfData = fetcherConf.getFetchersConfigurationData().get(0);
    Class<?> fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(fetcherConfData.getClassName());
    SparkFSFetcher fetcher =
      (SparkFSFetcher) fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(fetcherConfData);

    assertEquals(50, fetcher.eventLogSizeLimitMb(), 0);
    assertEquals("/custom/configured", fetcher.eventLogDir());
  }

  @Test
  public void testSparkFetcherUnspecifiedConfig() throws Exception {
    Document document = parseDocument("configurations/fetcher/FetcherConfTest7.xml");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    FetcherConfigurationData fetcherConfData = fetcherConf.getFetchersConfigurationData().get(0);
    Class<?> fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(fetcherConfData.getClassName());
    SparkFSFetcher fetcher =
      (SparkFSFetcher) fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(fetcherConfData);

    assertEquals(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB(), fetcher.eventLogSizeLimitMb(), 0);
    assertEquals(SparkFSFetcher.DEFAULT_EVENT_LOG_DIR(), fetcher.eventLogDir());
  }

  @Test
  public void testSparkFetcherEmptyConfig() throws Exception {
    Document document = parseDocument("configurations/fetcher/FetcherConfTest6.xml");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    FetcherConfigurationData fetcherConfData = fetcherConf.getFetchersConfigurationData().get(0);
    Class<?> fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(fetcherConfData.getClassName());
    SparkFSFetcher fetcher =
      (SparkFSFetcher) fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(fetcherConfData);

    assertEquals(SparkFSFetcher.DEFAULT_EVENT_LOG_SIZE_LIMIT_MB(), fetcher.eventLogSizeLimitMb(), 0);
    assertEquals(SparkFSFetcher.DEFAULT_EVENT_LOG_DIR(), fetcher.eventLogDir());
  }

  @Test
  public void testNameNodeAddressFromHadoopConf() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document2.getDocumentElement());
    FetcherConfigurationData fetcherConfData = fetcherConf.getFetchersConfigurationData().get(0);
    Configuration conf = new Configuration();
    HadoopUtils hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
      new scala.Tuple2("sample-ha1.grid.example.com", new scala.Tuple2("sample-ha1.grid.example.com", "standby")),
      new scala.Tuple2("sample-ha2.grid.example.com", new scala.Tuple2("sample-ha2.grid.example.com", "active")));

    String nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfData, conf, hadoopUtils).get();
    assertEquals(nameNode, "sample-ha2.grid.example.com:50070");
  }

  @Test
  public void testNameNodeAddressFromFetcherConf() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document4.getDocumentElement());
    FetcherConfigurationData fetcherConfData = fetcherConf.getFetchersConfigurationData().get(0);
    Configuration conf = new Configuration();
    HadoopUtils hadoopUtils = HadoopUtilsTest.newFakeHadoopUtilsForNameNode(
      new scala.Tuple2("sample-ha1.grid.example.com", new scala.Tuple2("sample-ha1.grid.example.com", "standby")),
      new scala.Tuple2("sample-ha2.grid.example.com", new scala.Tuple2("sample-ha2.grid.example.com", "active")),
      new scala.Tuple2("sample-ha3.grid.example.com", new scala.Tuple2("sample-ha3.grid.example.com", "standby")),
      new scala.Tuple2("sample-ha4.grid.example.com", new scala.Tuple2("sample-ha4.grid.example.com", "active")));

    String nameNode = SparkFSFetcher.nameNodeAddress(fetcherConfData, conf, hadoopUtils).get();
    assertEquals(nameNode, "sample-ha4.grid.example.com:50070");
  }
}
