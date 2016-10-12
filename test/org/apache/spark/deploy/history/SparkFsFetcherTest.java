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

  /**
   * Test for verifying the configured event log directory and log size
   *
   * <params>
   *   <event_log_size_limit_in_mb>50</event_log_size_limit_in_mb>
   *   <event_log_dir>/custom/configured</event_log_dir>
   * </params>
   */
  @Test
  public void testSparkFetcherConfig() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document1.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    Class<?> fetcherClass = null;
    FetcherConfigurationData data = fetcherConf.getFetchersConfigurationData().get(0);
    try {
      fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(data.getClassName());
      Object sparkFetcherInstance = fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(data);
      if (!(sparkFetcherInstance instanceof ElephantFetcher)) {
        throw new IllegalArgumentException(
                "Class " + fetcherClass.getName() + " is not an implementation of " + ElephantFetcher.class.getName());
      }

      // Check if the configurations are picked up correctly
      assertEquals(confEventLogSize, ((SparkFSFetcher) sparkFetcherInstance).getEventLogSize(), 0);
      assertEquals(confEventLogDir, ((SparkFSFetcher) sparkFetcherInstance).getEventLogDir());

    } catch (InstantiationException e) {
      throw new RuntimeException("Could not instantiate class " + data.getClassName(), e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not find class " + data.getClassName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access constructor for class" + data.getClassName(), e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Could not invoke class " + data.getClassName(), e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find constructor for class " + data.getClassName(), e);
    }
  }

  /**
   * Test for verifying unspecified log directory and log size configs
   *
   * <params>
   * </params>
   */
  @Test
  public void testSparkFetcherUnspecifiedConfig() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document3.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    Class<?> fetcherClass = null;
    FetcherConfigurationData data = fetcherConf.getFetchersConfigurationData().get(0);
    try {
      fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(data.getClassName());
      Object sparkFetcherInstance = fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(data);
      if (!(sparkFetcherInstance instanceof ElephantFetcher)) {
        throw new IllegalArgumentException(
                "Class " + fetcherClass.getName() + " is not an implementation of " + ElephantFetcher.class.getName());
      }

      // Check if the default values are used
      assertEquals(defEventLogSize, ((SparkFSFetcher) sparkFetcherInstance).getEventLogSize(), 0);
      assertEquals(defEventLogDir, ((SparkFSFetcher) sparkFetcherInstance).getEventLogDir());

    } catch (InstantiationException e) {
      throw new RuntimeException("Could not instantiate class " + data.getClassName(), e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not find class " + data.getClassName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access constructor for class" + data.getClassName(), e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Could not invoke class " + data.getClassName(), e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find constructor for class " + data.getClassName(), e);
    }
  }

  /**
   * Test for verifying empty log directory and log size configs
   *
   * <params>
   *   <event_log_size_limit_in_mb></event_log_size_limit_in_mb>
   *   <event_log_dir>/system/spark-history</event_log_dir>
   * </params>
   */
  @Test
  public void testSparkFetcherEmptyConfig() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document2.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);

    Class<?> fetcherClass = null;
    FetcherConfigurationData data = fetcherConf.getFetchersConfigurationData().get(0);
    try {
      fetcherClass = SparkFsFetcherTest.class.getClassLoader().loadClass(data.getClassName());
      Object sparkFetcherInstance = fetcherClass.getConstructor(FetcherConfigurationData.class).newInstance(data);
      if (!(sparkFetcherInstance instanceof ElephantFetcher)) {
        throw new IllegalArgumentException(
                "Class " + fetcherClass.getName() + " is not an implementation of " + ElephantFetcher.class.getName());
      }

      // Check if the default values are used
      assertEquals(defEventLogSize, ((SparkFSFetcher) sparkFetcherInstance).getEventLogSize(), 0);
      assertEquals(defEventLogDir, ((SparkFSFetcher) sparkFetcherInstance).getEventLogDir());

    } catch (InstantiationException e) {
      throw new RuntimeException("Could not instantiate class " + data.getClassName(), e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not find class " + data.getClassName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access constructor for class" + data.getClassName(), e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Could not invoke class " + data.getClassName(), e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find constructor for class " + data.getClassName(), e);
    }
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
