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

package com.linkedin.drelephant.configurations.jobtype;

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


public class JobTypeConfigurationTest {

  private static Document document1 = null;
  private static Document document2 = null;
  private static Document document3 = null;
  private static Document document4 = null;
  private static Document document5 = null;
  private static Document document6 = null;

  @BeforeClass
  public static void runBeforeClass() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document1 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest1.xml"));
      document2 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest2.xml"));
      document3 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest3.xml"));
      document4 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest4.xml"));
      document5 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest5.xml"));
      document6 = builder.parse(JobTypeConfigurationTest.class.getClassLoader()
          .getResourceAsStream("configurations/jobtype/JobTypeConfTest6.xml"));
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
   * Correctly configured fetcher
   */
  @Test
  public void testParseFetcherConf1() {
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document1.getDocumentElement());
    assertEquals(jobTypeConf.getAppTypeToJobTypeList().size(), 2);
  }

  /**
   * No name tag
   */
  @Test
  public void testParseFetcherConf2() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'jobtype' in jobtype 3");
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document2.getDocumentElement());
  }

  /**
   * No conf tag
   */
  @Test
  public void testParseFetcherConf3() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'conf' in jobtype Spark");
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document3.getDocumentElement());
  }

  /**
   * No applicationtype tag
   */
  @Test
  public void testParseFetcherConf4() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("No tag 'applicationtype' in jobtype Pig");
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document4.getDocumentElement());
  }

  /**
   * Wrong pattern for job type
   */
  @Test
  public void testParseFetcherConf5() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Error processing this pattern.  Pattern:[(voldemort) jobtype:Voldemort");
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document5.getDocumentElement());
  }

  /**
   * Multiple default types
   */
  @Test
  public void testParseFetcherConf6() {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Each application type should have one and only one default job type. Duplicate default"
        + " job type: jobType:Hive, for application type:mapreduce, isDefault:true, confName:hive.mapred.mode,"
        + " confValue:.*. for application type: MAPREDUCE");
    JobTypeConfiguration jobTypeConf = new JobTypeConfiguration(document6.getDocumentElement());
  }
}
