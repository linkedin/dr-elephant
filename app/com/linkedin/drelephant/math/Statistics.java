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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;


/**
 * This class includes all the statistical operations
 */
public final class Statistics {

  public static final long SECOND_IN_MS = 1000L;
  public static final long MINUTE_IN_MS = 60L * SECOND_IN_MS;
  public static final long HOUR_IN_MS = 60L * MINUTE_IN_MS;

  public static long MINUTE = 60L;
  public static long HOUR = 60*MINUTE;

  private Statistics() {
  }

  /**
   * Check if the array has deviating elements.
   * <p/>
   * Deviating elements are found by comparing each individual value against the average.
   *
   * @param values the array of values to check
   * @param buffer the amount to ignore as a buffer for smaller valued lists
   * @param factor the amount of allowed deviation is calculated from average * factor
   * @return the index of the deviating value, or -1 if
   */
  public static int[] deviates(long[] values, long buffer, double factor) {
    if (values == null || values.length == 0) {
      return new int[0];
    }

    long avg = average(values);

    //Find deviated elements

    long minimumDiff = Math.max(buffer, (long) (avg * factor));
    List<Integer> deviatedElements = new ArrayList<Integer>();

    for (int i = 0; i < values.length; i++) {
      long diff = values[i] - avg;
      if (diff > minimumDiff) {
        deviatedElements.add(i);
      }
    }

    int[] result = new int[deviatedElements.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = deviatedElements.get(i);
    }

    return result;
  }

  /**
   * The percentile method returns the least value from the given list which has at least given percentile.
   * @param values The list of values to find the percentile from
   * @param percentile The percentile
   * @return The least value from the list with at least the given percentile
   */
  public static long percentile(List<Long> values, int percentile) {

    if (values.size() == 0) {
      throw new IllegalArgumentException("Percentile of empty list is not defined.");
    }

    if (percentile > 100 || percentile < 0) {
      throw new IllegalArgumentException("Percentile has to be between 0-100");
    }

    if (percentile == 0) {
      return 0;
    }

    Collections.sort(values);

    // Use Nearest Rank method.
    // https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method
    int position = (int) Math.ceil(values.size() * percentile / 100.0);

    // should never happen.
    if (position == 0) {
      return values.get(position);
    }

    // position is always one greater than index. Return value at the proper index
    return values.get(position - 1);
  }


  public static long[][] findTwoGroups(long[] values) {
    return findTwoGroupsRecursive(values, average(values), 2);
  }

  public static long[][] findTwoGroupsRecursive(long[] values, long middle, int levels) {
    if (levels > 0) {
      long[][] result = twoMeans(values, middle);
      long newMiddle = average(result[1]) - average(result[0]);
      return findTwoGroupsRecursive(values, newMiddle, levels - 1);
    }
    return twoMeans(values, middle);
  }

  private static long[][] twoMeans(long[] values, long middle) {
    List<Long> smaller = new ArrayList<Long>();
    List<Long> larger = new ArrayList<Long>();
    for (int i = 0; i < values.length; i++) {
      if (values[i] < middle) {
        smaller.add(values[i]);
      } else {
        larger.add(values[i]);
      }
    }

    long[][] result = new long[2][];
    result[0] = toArray(smaller);
    result[1] = toArray(larger);

    return result;
  }

  private static long[] toArray(List<Long> input) {
    long[] result = new long[input.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = input.get(i);
    }
    return result;
  }

  /**
   * Compute average for the given array of long
   *
   * @param values the values
   * @return The average(values)
   */
  public static long average(long[] values) {
    //Find average
    double sum = 0d;
    for (long value : values) {
      sum += value;
    }
    return (long) (sum / (double) values.length);
  }

  /**
   * Compute average for a List of long values
   *
   * @param values the values
   * @return The average(values)
   */
  public static long average(List<Long> values) {
    //Find average
    double sum = 0d;
    for (long value : values) {
      sum += value;
    }
    return (long) (sum / (double) values.size());
  }

  /**
   * Find the median of the given list
   *
   * @param values The values
   * @return The median(values)
   */
  public static long median(List<Long> values) {
    if (values.size() == 0) {
      throw new IllegalArgumentException("Median of an empty list is not defined.");
    }
    Collections.sort(values);
    int middle = values.size() / 2;
    if (values.size() % 2 == 0) {
      return (values.get(middle - 1) + values.get(middle)) / 2;
    } else {
      return values.get(middle);
    }
  }

  /**
   * Compute ratio and display it with a suffix.
   *
   * Example: Average sort time	 (0.14x)
   *
   * @param value The value to be compared
   * @param compare The value compared against
   * @param suffix The suffix string
   * @return The ratio followed by suffix
   */
  public static String describeFactor(long value, long compare, String suffix) {
    double factor = (double) value / (double) compare;
    if (Double.isNaN(factor)) {
      return "";
    }
    return "(" + String.format("%.2f", factor) + suffix + ")";
  }

  /**
   * Convert milliseconds to readable value
   *
   * @param milliseconds The number of milliseconds
   * @return A String of readable time
   */
  public static String readableTimespan(long milliseconds) {
    if (milliseconds == 0) {
      return "0 sec";
    }

    long seconds = milliseconds / 1000;
    long minutes = seconds / 60;
    long hours = minutes / 60;
    minutes %= 60;
    seconds %= 60;
    StringBuilder sb = new StringBuilder();
    if (hours > 0) {
      sb.append(hours).append(" hr ");
    }
    if (minutes > 0) {
      sb.append(minutes).append(" min ");
    }
    if (seconds > 0) {
      sb.append(seconds).append(" sec ");
    }
    return sb.toString().trim();
  }

  public static <T> T[] createSample(Class<T> clazz, T[] objects, int size) {
    //Skip this process if number of items already smaller than sample size
    if (objects.length <= size) {
      return objects;
    }

    @SuppressWarnings("unchecked")
    T[] result = (T[]) Array.newInstance(clazz, size);

    //Shuffle a clone copy
    T[] clone = objects.clone();
    Collections.shuffle(Arrays.asList(clone));

    //Take the first n items
    System.arraycopy(clone, 0, result, 0, size);

    return result;
  }

  /**
   * Create a random sample within the original array
   */
  public static <T> void shuffleArraySample(T[] array, int sampleSize) {
    if (array.length <= sampleSize) {
      return;
    }

    T temp;
    int index;
    Random random = new Random();

    for (int i = 0; i < sampleSize; i++) {
      index = random.nextInt(array.length - i) + i;
      temp = array[index];
      array[index] = array[i];
      array[i] = temp;
    }
  }
}
