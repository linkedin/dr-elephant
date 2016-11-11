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

package com.linkedin.drelephant.util;


import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * This class tests the Utils class
 */
public class UtilsTest {

  @Test
  public void testParseJavaOptions() {
    Map<String, String> options1 = Utils.parseJavaOptions("-Dfoo=bar");
    assertEquals(1, options1.size());
    assertEquals("bar", options1.get("foo"));

    Map<String, String> options2 = Utils.parseJavaOptions(" -Dfoo=bar   -Dfoo2=bar2 -Dfoo3=bar3");
    assertEquals(3, options2.size());
    assertEquals("bar", options2.get("foo"));
    assertEquals("bar2", options2.get("foo2"));
    assertEquals("bar3", options2.get("foo3"));
  }

  @Test
  public void testParseJavaOptionsIgnoresNonStandardOptions() {
    Map<String, String> options1 = Utils.parseJavaOptions("-Dfoo=bar -XX:+UseCompressedOops -XX:MaxPermSize=512m -Dfoo2=bar2");
    assertEquals(2, options1.size());
    assertEquals("bar", options1.get("foo"));
    assertEquals("bar2", options1.get("foo2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseJavaOptionsThrowsIllegalArgumentExceptionForMissingAssignment() {
    Utils.parseJavaOptions("-Dfoo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseJavaOptionsThrowsIllegalArgumentExceptionForUnexpectedProperties() {
    Utils.parseJavaOptions("-foo");
  }

  @Test
  public void testGetParam() {
    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put("test_severity_1", "10, 50, 100, 200");
    paramMap.put("test_severity_2", "2, 4, 8");
    paramMap.put("test_param_1", "2&");
    paramMap.put("test_param_2", "2");
    paramMap.put("test_param_3", "");
    paramMap.put("test_param_4", null);

    double limits1[] = Utils.getParam(paramMap.get("test_severity_1"), 4);
    assertEquals(10d, limits1[0], 0);
    assertEquals(50d, limits1[1], 0);
    assertEquals(100d, limits1[2], 0);
    assertEquals(200d, limits1[3], 0);

    double limits2[] = Utils.getParam(paramMap.get("test_severity_2"), 4);
    assertEquals(null, limits2);

    double limits3[] = Utils.getParam(paramMap.get("test_param_1"), 1);
    assertEquals(null, limits3);

    double limits4[] = Utils.getParam(paramMap.get("test_param_2"), 1);
    assertEquals(2d, limits4[0], 0);

    double limits5[] = Utils.getParam(paramMap.get("test_param_3"), 1);
    assertEquals(null, limits5);

    double limits6[] = Utils.getParam(paramMap.get("test_param_4"), 1);
    assertEquals(null, limits6);
  }

  @Test
  public void testCommaSeparated() {
    String commaSeparated1 = Utils.commaSeparated("foo");
    assertEquals("foo", commaSeparated1);

    String commaSeparated2 = Utils.commaSeparated("foo", "bar", "");
    assertEquals("foo,bar", commaSeparated2);

    String commaSeparated3 = Utils.commaSeparated("foo", "bar", null);
    assertEquals("foo,bar", commaSeparated3);

    String commaSeparated4 = Utils.commaSeparated();
    assertEquals("", commaSeparated4);
  }

  @Test
  public void testTruncateField() {
    String truncatedField1 = Utils.truncateField("foo-bar", 7, "id");
    assertEquals("foo-bar", truncatedField1);

    String truncatedField2 = Utils.truncateField("foo-bar", 6, "id");
    assertEquals("foo...", truncatedField2);

    String truncatedField3 = Utils.truncateField("foo-bar", -1, "id");
    assertEquals("foo-bar", truncatedField3);

    String truncatedField4 = Utils.truncateField(null, 5, "id");
    assertEquals(null, truncatedField4);
  }

  @Test
  public void testParseCsKeyValue() {
    Map<String, String> properties = Utils.parseCsKeyValue("");
    assertEquals(0, properties.size());

    Map<String, String> properties1 = Utils.parseCsKeyValue("foo=bar");
    assertEquals(1, properties1.size());
    assertEquals("bar", properties1.get("foo"));

    Map<String, String> properties2 = Utils.parseCsKeyValue("foo1=bar1,foo2=bar2,foo3=bar3");
    assertEquals(3, properties2.size());
    assertEquals("bar1", properties2.get("foo1"));
    assertEquals("bar2", properties2.get("foo2"));
    assertEquals("bar3", properties2.get("foo3"));
  }

  @Test
  public void testGetNonNegativeInt() {
    Configuration conf = new Configuration();
    conf.set("foo1", "100");
    conf.set("foo2", "-100");
    conf.set("foo3", "0");
    conf.set("foo4", "0.5");
    conf.set("foo5", "9999999999999999");
    conf.set("foo6", "bar");

    int defaultValue = 50;
    assertEquals(100, Utils.getNonNegativeInt(conf, "foo1", defaultValue));
    assertEquals(0, Utils.getNonNegativeInt(conf, "foo2", defaultValue));
    assertEquals(0, Utils.getNonNegativeInt(conf, "foo3", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeInt(conf, "foo4", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeInt(conf, "foo5", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeInt(conf, "foo6", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeInt(conf, "foo7", defaultValue));
  }

  @Test
  public void testGetNonNegativeLong() {
    Configuration conf = new Configuration();

    conf.set("foo1", "100");
    conf.set("foo2", "-100");
    conf.set("foo3", "0");
    conf.set("foo4", "0.5");
    conf.set("foo5", "9999999999999999");
    conf.set("foo6", "bar");

    long defaultValue = 50;
    assertEquals(100, Utils.getNonNegativeLong(conf, "foo1", defaultValue));
    assertEquals(0, Utils.getNonNegativeLong(conf, "foo2", defaultValue));
    assertEquals(0, Utils.getNonNegativeLong(conf, "foo3", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeLong(conf, "foo4", defaultValue));
    assertEquals(9999999999999999L, Utils.getNonNegativeLong(conf, "foo5", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeLong(conf, "foo6", defaultValue));
    assertEquals(defaultValue, Utils.getNonNegativeLong(conf, "foo7", defaultValue));
  }

  @Test
  public void testFormatStringOrNull() {
    assertEquals("Hello world!", Utils.formatStringOrNull("%s %s!", "Hello", "world"));
    assertEquals(null, Utils.formatStringOrNull("%s %s!", "Hello", null));
  }
 
  @Test 
  public void testGetDurationBreakdown() {
    long []durations = {13423,432344,23423562,23,324252,1132141414141L};
    assertEquals("0:00:13", Utils.getDurationBreakdown(durations[0]));
    assertEquals("0:07:12", Utils.getDurationBreakdown(durations[1]));
    assertEquals("6:30:23", Utils.getDurationBreakdown(durations[2]));
    assertEquals("0:00:00", Utils.getDurationBreakdown(durations[3]));
    assertEquals("0:05:24", Utils.getDurationBreakdown(durations[4]));
    assertEquals("314483:43:34", Utils.getDurationBreakdown(durations[5]));
  }
  @Test
  public void testGetPercentage() {
    long []numerators = {10,20,30,40,50};
    long []denominators = {100,200,100,52,70};

    assertEquals("10.00 %", Utils.getPercentage(numerators[0],denominators[0]));
    assertEquals("10.00 %", Utils.getPercentage(numerators[1],denominators[1]));
    assertEquals("30.00 %", Utils.getPercentage(numerators[2],denominators[2]));
    assertEquals("76.92 %", Utils.getPercentage(numerators[3],denominators[3]));
    assertEquals("71.43 %", Utils.getPercentage(numerators[4],denominators[4]));
    assertEquals("NaN", Utils.getPercentage(0,0));
  }

  @Test
  public void testGetDurationInGBHours() {

    long []durations = {10000, 213234343, 23424, 635322, 213};

    assertEquals("0.003 GB Hours", Utils.getResourceInGBHours(durations[0]));
    assertEquals("57.844 GB Hours", Utils.getResourceInGBHours(durations[1]));
    assertEquals("0.006 GB Hours", Utils.getResourceInGBHours(durations[2]));
    assertEquals("0.172 GB Hours", Utils.getResourceInGBHours(durations[3]));
    assertEquals("0 GB Hours", Utils.getResourceInGBHours(durations[4]));

  }

}
