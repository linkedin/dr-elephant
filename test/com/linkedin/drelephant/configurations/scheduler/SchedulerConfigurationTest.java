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

package com.linkedin.drelephant.configurations.scheduler;

import com.linkedin.drelephant.configurations.scheduler.SchedulerConfiguration;
import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;

import java.io.IOException;
import java.util.List;

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
import static org.junit.Assert.assertTrue;


public class SchedulerConfigurationTest {

  private static Document document1;
  private static Document document2;
  private static Document document3;

  @BeforeClass
  public static void runBeforeClass() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document1 = builder.parse(SchedulerConfigurationTest.class.getClassLoader()
              .getResourceAsStream("configurations/scheduler/SchedulerConfTest1.xml"));
      document2 = builder.parse(SchedulerConfigurationTest.class.getClassLoader()
              .getResourceAsStream("configurations/scheduler/SchedulerConfTest2.xml"));
      document3 = builder.parse(SchedulerConfigurationTest.class.getClassLoader()
              .getResourceAsStream("configurations/scheduler/SchedulerConfTest3.xml"));
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
   * Correctly configured scheduler
   */
  @Test
  public void testParseSchedulerConf1() {
    SchedulerConfiguration schedulerConf = new SchedulerConfiguration(document1.getDocumentElement());
    List<SchedulerConfigurationData> schedulerConfData = schedulerConf.getSchedulerConfigurationData();
    assertEquals(schedulerConfData.size(), 2);
    for (SchedulerConfigurationData data : schedulerConfData) {
      if (data.getSchedulerName().equals("airflow")) {
        assertEquals("com.linkedin.drelephant.schedulers.AirflowScheduler", data.getClassName());
        assertEquals("http://localhost:8000", data.getParamMap().get("airflowbaseurl"));
      } else {
        assertEquals("azkaban", data.getSchedulerName());
        assertEquals("com.linkedin.drelephant.schedulers.AzkabanScheduler", data.getClassName());
      }
    }
  }

  /**
   * No classname tag
   */
  @Test
  public void testParseSchedulerConf2() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'classname' in scheduler 1");
    new SchedulerConfiguration(document2.getDocumentElement());
  }

  /**
   * No name tag
   */
  @Test
  public void testParseSchedulerConf3() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'name' in scheduler 2 classname com.linkedin.drelephant.schedulers.AzkabanScheduler");
    new SchedulerConfiguration(document3.getDocumentElement());
  }

}
