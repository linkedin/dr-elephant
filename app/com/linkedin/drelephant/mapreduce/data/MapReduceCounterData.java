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

package com.linkedin.drelephant.mapreduce.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * This class manages all the MapReduce Counters
 */
public class MapReduceCounterData {

  // This is a map of group to all the counters in the group and their values.
  private final Map<String, Map<String, Long>> _pubCounters;

  public String toString() {
    return _pubCounters.toString();
  }

  public MapReduceCounterData() {
    _pubCounters = new HashMap<String, Map<String, Long>>(8);
  }

  /**
   * @return the value of the counter, 0 if not present.
   * This method is only used for job heuristics
   * Due to h1 & h2 counter group incompatibility, we iterate every counter group (4 by default)
   * to find a matching counter name, otherwise we have to hardcode the h1&h2 version of counter group
   * and try twice with two names for each counter in this method.
   * This approach is less efficient, but cleaner.
   */
  public long get(CounterName counterName) {
    // For each counter group, try to match the counter name
    for(Map<String, Long> counterGrp : _pubCounters.values()) {
      if(counterGrp.containsKey(counterName._name)) {
        return counterGrp.get(counterName._name);
      }
    }
    return 0;
  }

  public void set(CounterName counterName, long value) {
    set(counterName.getGroupName(), counterName.getName(), value);
  }

  /**
   * Set the value of a counter that we may want to publish later
   *
   * @param groupName
   * @param counterName
   * @param value
   */
  public void set(String groupName, String counterName, long value) {
    Map<String, Long> counterMap = _pubCounters.get(groupName);
    if (counterMap == null) {
      counterMap = new HashMap<String, Long>(4);
      _pubCounters.put(groupName, counterMap);
    }
    counterMap.put(counterName, value);
  }

  public Set<String> getGroupNames() {
    Set<String> groupNames = _pubCounters.keySet();
    return Collections.unmodifiableSet(groupNames);
  }

  /**
   * Get the values of all counters in a group
   * @param groupName
   * @return A map containing all the values of counters in a group.
   */
  public Map<String, Long> getAllCountersInGroup(String groupName) {
    Map<String, Long> counterMap = _pubCounters.get(groupName);
    if (counterMap == null) {
      counterMap = new HashMap<String, Long>(1);
    }
    return counterMap;
  }

  public static enum GroupName {
    FileInput,
    FileSystemCounters,
    MapReduce,
    FileOutput;
  }

  public static enum CounterName {
    BYTES_READ(GroupName.FileInput, "BYTES_READ", "Bytes Read"),
    BYTES_WRITTEN(GroupName.FileOutput, "BYTES_WRITTEN", "Bytes Written"),

    FILE_BYTES_READ(GroupName.FileSystemCounters, "FILE_BYTES_READ", "FILE_BYTES_READ"),
    FILE_BYTES_WRITTEN(GroupName.FileSystemCounters, "FILE_BYTES_WRITTEN", "FILE_BYTES_WRITTEN"),
    HDFS_BYTES_READ(GroupName.FileSystemCounters, "HDFS_BYTES_READ", "HDFS_BYTES_READ"),
    HDFS_BYTES_WRITTEN(GroupName.FileSystemCounters, "HDFS_BYTES_WRITTEN", "HDFS_BYTES_WRITTEN"),
    S3_BYTES_READ(GroupName.FileSystemCounters, "S3_BYTES_READ", "S3_BYTES_READ"),
    S3_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3_BYTES_WRITTEN", "S3_BYTES_WRITTEN"),
    S3N_BYTES_READ(GroupName.FileSystemCounters, "S3N_BYTES_READ", "S3N_BYTES_READ"),
    S3N_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3N_BYTES_WRITTEN", "S3N_BYTES_WRITTEN"),
    S3A_BYTES_READ(GroupName.FileSystemCounters, "S3A_BYTES_READ", "S3A_BYTES_READ"),
    S3A_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3A_BYTES_WRITTEN", "S3A_BYTES_WRITTEN"),

    MAP_INPUT_RECORDS(GroupName.MapReduce, "MAP_INPUT_RECORDS", "Map input records"),
    MAP_OUTPUT_RECORDS(GroupName.MapReduce, "MAP_OUTPUT_RECORDS", "Map output records"),
    MAP_OUTPUT_BYTES(GroupName.MapReduce, "MAP_OUTPUT_BYTES", "Map output bytes"),
    MAP_OUTPUT_MATERIALIZED_BYTES(GroupName.MapReduce, "MAP_OUTPUT_MATERIALIZED_BYTES", "Map output materialized bytes"),
    SPLIT_RAW_BYTES(GroupName.MapReduce, "SPLIT_RAW_BYTES", "SPLIT_RAW_BYTES"),

    REDUCE_INPUT_GROUPS(GroupName.MapReduce, "REDUCE_INPUT_GROUPS", "Reduce input groups"),
    REDUCE_SHUFFLE_BYTES(GroupName.MapReduce, "REDUCE_SHUFFLE_BYTES", "Reduce shuffle bytes"),
    REDUCE_OUTPUT_RECORDS(GroupName.MapReduce, "REDUCE_OUTPUT_RECORDS", "Reduce output records"),
    REDUCE_INPUT_RECORDS(GroupName.MapReduce, "REDUCE_INPUT_RECORDS", "Reduce input records"),

    COMBINE_INPUT_RECORDS(GroupName.MapReduce, "COMBINE_INPUT_RECORDS", "Combine input records"),
    COMBINE_OUTPUT_RECORDS(GroupName.MapReduce, "COMBINE_OUTPUT_RECORDS", "Combine output records"),
    SPILLED_RECORDS(GroupName.MapReduce, "SPILLED_RECORDS", "Spilled Records"),

    CPU_MILLISECONDS(GroupName.MapReduce, "CPU_MILLISECONDS", "CPU time spent (ms)"),
    GC_MILLISECONDS(GroupName.MapReduce, "GC_TIME_MILLIS", "GC time elapsed (ms)"),
    COMMITTED_HEAP_BYTES(GroupName.MapReduce, "COMMITTED_HEAP_BYTES", "Total committed heap usage (bytes)"),
    PHYSICAL_MEMORY_BYTES(GroupName.MapReduce, "PHYSICAL_MEMORY_BYTES", "Physical memory (bytes) snapshot"),
    VIRTUAL_MEMORY_BYTES(GroupName.MapReduce, "VIRTUAL_MEMORY_BYTES", "Virtual memory (bytes) snapshot");

    GroupName _group;
    String _name;
    String _displayName;

    CounterName(GroupName group, String name, String displayName) {
      this._group = group;
      this._name = name;
      this._displayName = displayName;
    }

    static Map<String, CounterName> _counterDisplayNameMap;
    static Map<String, CounterName> _counterNameMap;
    static {
      _counterDisplayNameMap = new HashMap<String, CounterName>();
      _counterNameMap = new HashMap<String, CounterName>();
      for (CounterName cn : CounterName.values()) {
        _counterDisplayNameMap.put(cn._displayName, cn);
        _counterNameMap.put(cn._name, cn);
      }
    }

    public static CounterName getCounterFromName(String name) {
      if (_counterNameMap.containsKey(name)) {
        return _counterNameMap.get(name);
      }
      return null;
    }

    public static CounterName getCounterFromDisplayName(String displayName) {
      if (_counterDisplayNameMap.containsKey(displayName)) {
        return _counterDisplayNameMap.get(displayName);
      }
      return null;
    }

    public String getName() {
      return _name;
    }

    public String getDisplayName() {
      return _displayName;
    }

    public String getGroupName() {
      return _group.name();
    }
  }
}
