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

package com.linkedin.drelephant.math;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;


public class StatisticsTest {

  @Test
  public void testAverage1() {
    assertEquals(6, Statistics.average(new long[]{2, 4, 6, 8, 10}));
    assertEquals(0, Statistics.average(new long[] {}));
  }

  @Test
  public void testAverage2() {
    ArrayList<Long> list1 = new ArrayList<Long>();
    list1.add(2l);
    list1.add(4l);
    list1.add(6l);
    list1.add(8l);
    list1.add(10l);
    assertEquals(6, Statistics.average(list1));

    ArrayList<Long> list2 = new ArrayList<Long>();
    assertEquals(0, Statistics.average(list2));
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testMedian1() {
    ArrayList<Long> list1 = new ArrayList<Long>();
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Median of an empty list is not defined.");
    Statistics.median(list1);
  }

  @Test
  public void testMedian2() {
    ArrayList<Long> list2 = new ArrayList<Long>();
    list2.add(2l);
    list2.add(4l);
    list2.add(6l);
    list2.add(8l);
    assertEquals(5, Statistics.median(list2));

    list2.add(15l);
    assertEquals(6, Statistics.median(list2));
  }

  @Test
  public void testDescribeFactor() {
    assertEquals("", Statistics.describeFactor(0, 0, "test"));
    assertEquals("(5.00test)", Statistics.describeFactor(10, 2, "test"));
  }

  @Test
  public void testReadableTimespan() {
    assertEquals("0 sec", Statistics.readableTimespan(0));
    assertEquals("1 sec", Statistics.readableTimespan(1000));
    assertEquals("1 min", Statistics.readableTimespan(60000));
    assertEquals("1 hr", Statistics.readableTimespan(3600000));
  }

  @Test
  public void testPercentile() {
    List<Long> finishTimes = new ArrayList<Long>();
    for(int i=1;i<=10;i++) {
      finishTimes.add((long)i*10);
    }
    assertEquals(100, Statistics.percentile(finishTimes,100));
    assertEquals(50, Statistics.percentile(finishTimes,50));
    assertEquals(0, Statistics.percentile(finishTimes,0));
    assertEquals(10, Statistics.percentile(finishTimes,10));
    assertEquals(10, Statistics.percentile(finishTimes,4));

    List<Long> oddLengthValues = new ArrayList<Long>();
    oddLengthValues.add(1L);
    oddLengthValues.add(2L);
    oddLengthValues.add(3L);
    oddLengthValues.add(4L);
    oddLengthValues.add(5L);
    assertEquals(3L, Statistics.percentile(oddLengthValues, 50));

    List<Long> finishTimeSingle = new ArrayList<Long>();
    finishTimeSingle.add(10L);
    assertEquals(10,Statistics.percentile(finishTimeSingle,100));
    assertEquals(0, Statistics.percentile(finishTimeSingle,0));
    assertEquals(10,Statistics.percentile(finishTimeSingle, 10));
    assertEquals(10,Statistics.percentile(finishTimeSingle, 50));
  }
}
