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

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;


/**
 * This is a utils class that handles memory string parsing and formatting problem.
 */
public class MemoryFormatUtils {
  private MemoryFormatUtils() {
    // Do nothing, empty on purpose
  }

  private static class MemoryUnit {
    private final String _name;
    private final long _bytes;

    public MemoryUnit(String name, long bytes) {
      _name = name;
      _bytes = bytes;
    }

    public String getName() {
      return _name;
    }

    public long getBytes() {
      return _bytes;
    }

    @Override
    public String toString() {
      return _name;
    }
  }

  // Units must be in a descent order
  private static final MemoryUnit[] UNITS =
      new MemoryUnit[]{new MemoryUnit("TB", 1L << 40), new MemoryUnit("GB", 1L << 30), new MemoryUnit("MB",
          1L << 20), new MemoryUnit("KB", 1L << 10), new MemoryUnit("B", 1L)};

  private static final DecimalFormat FORMATTER = new DecimalFormat("#,##0.##");
  private static final Pattern REGEX_MATCHER =
      Pattern.compile("([-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?)\\s*((?:[T|G|M|K])?B?)?", Pattern.CASE_INSENSITIVE);

  /**
   * Given a memory value in bytes, convert it to a string with the unit that round to a >0 integer part.
   *
   * @param value The memory value in long bytes
   * @return The formatted string, null if
   */
  public static String bytesToString(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Invalid memory size: " + value);
    }
    for (int i = 0; i < UNITS.length; i++) {
      long bytes = UNITS[i].getBytes();
      if (value >= bytes) {
        double numResult = bytes > 1 ? (double) value / (double) bytes : (double) value;
        return FORMATTER.format(numResult) + " " + UNITS[i].getName();
      }
    }
    return value + " " + UNITS[UNITS.length - 1].getName();
  }

  /**
   * Convert a formatted string into a long value in bytes.
   * This method handles
   *
   * @param formattedString The string to convert
   * @return The bytes value
   */
  public static long stringToBytes(String formattedString) {
    if (formattedString == null || formattedString.isEmpty()) {
      return 0L;
    }

    //handling if the string has , for eg. 1,000MB
    formattedString = formattedString.replace(",", "");

    Matcher matcher = REGEX_MATCHER.matcher(formattedString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "The formatted string [" + formattedString + "] does not match with the regex /" + REGEX_MATCHER.toString()
              + "/");
    }
    if (matcher.groupCount() != 1 && matcher.groupCount() != 2) {
      throw new IllegalArgumentException();
    }

    double numPart = Double.parseDouble(matcher.group(1));
    if (numPart < 0) {
      throw new IllegalArgumentException("The number part of the memory cannot be less than zero: [" + numPart + "].");
    }
    String unitPart = matcher.groupCount() == 2 ? matcher.group(2).toUpperCase() : "";
    if (!unitPart.endsWith("B")) {
      unitPart += "B";
    }
    for (int i = 0; i < UNITS.length; i++) {
      if (unitPart.equals(UNITS[i].getName())) {
        return (long) (numPart * UNITS[i].getBytes());
      }
    }
    throw new IllegalArgumentException("The formatted string [" + formattedString + "] 's unit part [" + unitPart
        + "] does not match any unit. The supported units are (case-insensitive, and also the 'B' is ignorable): ["
        + StringUtils.join(UNITS) + "].");
  }

  /**
   * Given a memory value in string format, it rounds off the double value to next integer.
   * @param formattedString
   * @return : formatted String with int value to next integer.
   */
  public static String roundOffMemoryStringToNextInteger(String formattedString) {
    if (formattedString == null || formattedString.isEmpty()) {
      return "";
    }

    //handling if the string has , for eg. 1,000MB
    formattedString = formattedString.replace(",", "");

    Matcher matcher = REGEX_MATCHER.matcher(formattedString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "The formatted string [" + formattedString + "] does not match with the regex /" + REGEX_MATCHER.toString()
              + "/");
    }
    if (matcher.groupCount() != 1 && matcher.groupCount() != 2) {
      throw new IllegalArgumentException();
    }

    double numPart = Double.parseDouble(matcher.group(1));
    if (numPart < 0) {
      throw new IllegalArgumentException("The number part of the memory cannot be less than zero: [" + numPart + "].");
    }

    int numPartInt = (int) Math.ceil(numPart);

    String unitPart = matcher.groupCount() == 2 ? matcher.group(2).toUpperCase() : "";
    if (!unitPart.endsWith("B")) {
      unitPart += "B";
    }
    return (numPartInt + " " + unitPart);
  }
}
