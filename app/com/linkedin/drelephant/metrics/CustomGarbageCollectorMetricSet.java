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

package com.linkedin.drelephant.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * This class adds couple of custom guages apart from the ones
 * implemented in <code>com.codahale.metrics.jvm.GarbageCollectorMetricSet</code>.
 *
 * <br><br>
 * The following custom guages are added.
 * <ul>jvmUptime - The time since the JVM was started.</ul>
 * <ul>gc2UptimeRatio - The ratio of GC collection times to JVM uptime. Collection
 * times for both young gen and perm gen are counted.</ul>
 */
public class CustomGarbageCollectorMetricSet implements MetricSet {
  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

  private final List<GarbageCollectorMXBean> garbageCollectors;

  /**
   * Creates a new set of gauges for all discoverable garbage collectors.
   */
  public CustomGarbageCollectorMetricSet() {
    this(ManagementFactory.getGarbageCollectorMXBeans());
  }

  /**
   * Creates a new set of gauges for the given collection of garbage collectors.
   *
   * @param garbageCollectors    the garbage collectors
   */
  public CustomGarbageCollectorMetricSet(Collection<GarbageCollectorMXBean> garbageCollectors) {
    this.garbageCollectors = new ArrayList<GarbageCollectorMXBean>(garbageCollectors);
  }

  /**
   * @return Returns a map of defined gauges.
   */
  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<String, Metric>();

    long cumulativeGCTime = 0L;

    for (final GarbageCollectorMXBean gc : garbageCollectors) {
      final String name = WHITESPACE.matcher(gc.getName()).replaceAll("-");

      gauges.put(name(name, "count"), new Gauge<Long>() {
        @Override
        public Long getValue() {
          return gc.getCollectionCount();
        }
      });

      gauges.put(name(name, "time"), new Gauge<Long>() {
        @Override
        public Long getValue() {
          return gc.getCollectionTime();
        }
      });

      cumulativeGCTime += gc.getCollectionTime();
    }

    final long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
    final Double gc2UptimeRatio = (double)cumulativeGCTime / uptime;

    gauges.put("jvmUptime", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return uptime;
      }
    });

    gauges.put("gc2UptimeRatio", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return gc2UptimeRatio;
      }
    });

    return Collections.unmodifiableMap(gauges);
  }
}
