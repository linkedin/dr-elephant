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

import java.util.Random;
import junit.framework.TestCase;


/**
 * This class test MemoryFormatUtils
 */
public class MemoryFormatUtilsTest extends TestCase {
  public void testBytesToString() {
    // Check integer values
    assertEquals("0 B", MemoryFormatUtils.bytesToString(0L));
    assertEquals("1 B", MemoryFormatUtils.bytesToString(1L));
    assertEquals("1 KB", MemoryFormatUtils.bytesToString(1L << 10));
    assertEquals("1 MB", MemoryFormatUtils.bytesToString(1L << 20));
    assertEquals("1 GB", MemoryFormatUtils.bytesToString(1L << 30));
    assertEquals("1,000 GB", MemoryFormatUtils.bytesToString(1000L << 30));
    assertEquals("1 TB", MemoryFormatUtils.bytesToString(1L << 40));
    assertEquals("1,024 TB", MemoryFormatUtils.bytesToString(1L << 50));

    // Check double values
    assertEquals("8.79 KB", MemoryFormatUtils.bytesToString(9000L));
    assertEquals("8.79 MB", MemoryFormatUtils.bytesToString(9000L << 10));
    assertEquals("8.79 GB", MemoryFormatUtils.bytesToString(9000L << 20));
    assertEquals("8.79 TB", MemoryFormatUtils.bytesToString(9000L << 30));
    assertEquals("87,890.62 TB", MemoryFormatUtils.bytesToString((9000L * 10000) << 30));
  }

  public void testScientificStringNumberToBytes() {
    long expectedBytes = (long ) (8.79 * (1 << 20));
    assertEquals(expectedBytes, MemoryFormatUtils.stringToBytes("8.79e0 MB"));
    assertEquals(expectedBytes, MemoryFormatUtils.stringToBytes("8.79E0 MB"));
    assertEquals(expectedBytes, MemoryFormatUtils.stringToBytes("0.879e1 MB"));
    assertEquals(expectedBytes, MemoryFormatUtils.stringToBytes("+0.879e+1 MB"));
    assertEquals(expectedBytes, MemoryFormatUtils.stringToBytes("87.9e-1 MB"));
  }

  public void testStringToBytes() {
    // Null
    assertEquals(0L, MemoryFormatUtils.stringToBytes(null));

    // Integer tests
    String[] units = new String[]{"", "K", "M", "G", "T"};
    long[] multiplers = new long[]{1L, 1L << 10, 1L << 20, 1L << 30, 1L << 40};
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < units.length; i++) {
      String unit = units[i];

      // Int values
      int[] integerNums =
          units.equals("T") ? new int[]{0, random.nextInt(1024), 2048} : new int[]{0, random.nextInt(1024)};
      for (int num : integerNums) {
        long expectedNum = num * multiplers[i];
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "b"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "b"));

        unit = unit.toLowerCase();
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "b"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "b"));
      }

      // Double values, notice that converting double values to long might lose some precision
      double[] doublleNums = new double[]{random.nextDouble(), random.nextDouble() + random.nextInt(1024), 8.79d};
      for (double num : doublleNums) {
        long expectedNum = (long) (num * multiplers[i]);
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "b"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "b"));

        unit = unit.toLowerCase();
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "b"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "B"));
        assertEquals(expectedNum, MemoryFormatUtils.stringToBytes(num + " " + unit + "b"));
      }
    }
  }

  public void testRoundOffMemoryStringToNextInteger() {
    assertEquals("157 MB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("156.1 MB"));
    assertEquals("155 MB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("155.0 MB"));
    assertEquals("0 MB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("0 MB"));
    assertEquals("156 GB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("155.1 G"));
    assertEquals("", MemoryFormatUtils.roundOffMemoryStringToNextInteger(null));
    assertEquals("500 MB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("500M"));
    assertEquals("600 GB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("600 gb"));
    assertEquals("600 GB", MemoryFormatUtils.roundOffMemoryStringToNextInteger("600 g"));
    assertEquals("", MemoryFormatUtils.roundOffMemoryStringToNextInteger(""));
  }
}
