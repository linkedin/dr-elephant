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

package com.linkedin.drelephant.configurations.fetcher;

import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import static org.junit.Assert.assertEquals;


public class FetcherConfigurationTest {

  private static Document document1 = null;
  private static Document document2 = null;
  private static Document document3 = null;
  private static Document document4 = null;
  private static Document document5 = null;

  private static final String spark = "SPARK";
  private static final String logDirField = "event_log_dir";
  private static final String logDirValue = "/custom/configured";
  private static final String logSizeField = "event_log_size_limit_in_mb";
  private static final String logSizeValue = "50";


  @BeforeClass
  public static void runBeforeClass() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document1 = builder.parse(
          FetcherConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/fetcher/FetcherConfTest1.xml"));
      document2 = builder.parse(
          FetcherConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/fetcher/FetcherConfTest2.xml"));
      document3 = builder.parse(
          FetcherConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/fetcher/FetcherConfTest3.xml"));
      document4 = builder.parse(
          FetcherConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/fetcher/FetcherConfTest4.xml"));
      document5 = builder.parse(
              FetcherConfigurationTest.class.getClassLoader().getResourceAsStream(
                      "configurations/fetcher/FetcherConfTest5.xml"));
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException("Test files are not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read test files ", e);
    }
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   *  Correctly configured fetcher
   */
  @Test
  public void testParseFetcherConf1() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document1.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 2);
  }

  /**
   *  No classname field
   */
  @Test
  public void testParseFetcherConf2() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'classname' in fetcher 2");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document2.getDocumentElement());
  }

  /**
   *  Empty classname field
   */
  @Test
  public void testParseFetcherConf3() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Empty tag 'classname' in fetcher 1");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document3.getDocumentElement());
  }

  /**
   *  No applicationtype tag
   */
  @Test
  public void testParseFetcherConf4() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag or invalid tag 'applicationtype' in fetcher 1"
            + " classname com.linkedin.drelephant.mapreduce.fetchers.MapReduceFetcherHadoop2");
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document4.getDocumentElement());
  }

  /**
   *  Test Spark fetcher params, Event log size and log directory
   */
  @Test
  public void testParseFetcherConf5() {
    FetcherConfiguration fetcherConf = new FetcherConfiguration(document5.getDocumentElement());
    assertEquals(fetcherConf.getFetchersConfigurationData().size(), 1);
    assertEquals(fetcherConf.getFetchersConfigurationData().get(0).getAppType().getName(), spark);
  }

}
