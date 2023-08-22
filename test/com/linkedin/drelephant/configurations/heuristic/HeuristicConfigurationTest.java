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

package com.linkedin.drelephant.configurations.heuristic;

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


public class HeuristicConfigurationTest {

  private static Document document1 = null;
  private static Document document2 = null;
  private static Document document3 = null;
  private static Document document4 = null;
  private static Document document5 = null;

  @BeforeClass
  public static void runBeforeClass() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document1 = builder.parse(HeuristicConfigurationTest.class.getClassLoader()
              .getResourceAsStream("configurations/heuristic/HeuristicConfTest1.xml"));
      document2 = builder.parse(
          HeuristicConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/heuristic/HeuristicConfTest2.xml"));
      document3 = builder.parse(
          HeuristicConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/heuristic/HeuristicConfTest3.xml"));
      document4 = builder.parse(
          HeuristicConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/heuristic/HeuristicConfTest4.xml"));
      document5 = builder.parse(
          HeuristicConfigurationTest.class.getClassLoader().getResourceAsStream(
              "configurations/heuristic/HeuristicConfTest5.xml"));
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
    HeuristicConfiguration heuristicConf = new HeuristicConfiguration(document1.getDocumentElement());
    assertEquals(heuristicConf.getHeuristicsConfigurationData().size(), 3);
  }

  /**
   * No classname tag
   */
  @Test
  public void testParseFetcherConf2() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'classname' in heuristic 1");
    HeuristicConfiguration heuristicConf = new HeuristicConfiguration(document2.getDocumentElement());
  }

  /**
   * No heuristic name tag
   */
  @Test
  public void testParseFetcherConf3() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'heuristicname' in heuristic 1 classname"
        + " com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic");
    HeuristicConfiguration heuristicConf = new HeuristicConfiguration(document3.getDocumentElement());
  }

  /**
   * No view name tag
   */
  @Test
  public void testParseFetcherConf4() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'viewname' in heuristic 1 classname"
        + " com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic");
    HeuristicConfiguration heuristicConf = new HeuristicConfiguration(document4.getDocumentElement());
  }

  /**
   * No application type tag
   */
  @Test
  public void testParseFetcherConf5() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag or invalid tag 'applicationtype' in heuristic 2 classname"
      + " com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic");
    HeuristicConfiguration heuristicConf = new HeuristicConfiguration(document5.getDocumentElement());
  }
}

