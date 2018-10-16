package com.linkedin.drelephant.mapreduce.heuristics;

public class CommonConstantsHeuristic {

  public static final String MAPPER_SPEED = "Mapper Speed";
  public static final String TOTAL_INPUT_SIZE_IN_MB = "Total input size in MB";

  public enum UtilizedParameterKeys {
    AVG_PHYSICAL_MEMORY("Avg Physical Memory (MB)"),
    MAX_PHYSICAL_MEMORY("Max Physical Memory (MB)"),
    MIN_PHYSICAL_MEMORY("Min Physical Memory (MB)"),
    AVG_VIRTUAL_MEMORY("Avg Virtual Memory (MB)"),
    MAX_VIRTUAL_MEMORY("Max Virtual Memory (MB)"),
    MIN_VIRTUAL_MEMORY("Min Virtual Memory (MB)"),
    AVG_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Avg Total Committed Heap Usage Memory (MB)"),
    MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Max Total Committed Heap Usage Memory (MB)"),
    MIN_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Min Total Committed Heap Usage Memory (MB)");
    private String value;

    UtilizedParameterKeys(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  public enum ParameterKeys {
    MAPPER_MEMORY_HADOOP_CONF("mapreduce.map.memory.mb"),
    MAPPER_HEAP_HADOOP_CONF("mapreduce.map.java.opts"),
    SORT_BUFFER_HADOOP_CONF("mapreduce.task.io.sort.mb"),
    SORT_FACTOR_HADOOP_CONF("mapreduce.task.io.sort.factor"),
    SORT_SPILL_HADOOP_CONF("mapreduce.map.sort.spill.percent"),
    REDUCER_MEMORY_HADOOP_CONF("mapreduce.reduce.memory.mb"),
    REDUCER_HEAP_HADOOP_CONF("mapreduce.reduce.java.opts"),
    SPLIT_SIZE_HADOOP_CONF("mapreduce.input.fileinputformat.split.maxsize"),
    CHILD_HEAP_SIZE_HADOOP_CONF("mapred.child.java.opts"),
    PIG_SPLIT_SIZE_HADOOP_CONF("pig.maxCombinedSplitSize"),
    MAPPER_MEMORY_HEURISTICS_CONF("Mapper Memory"),
    MAPPER_HEAP_HEURISTICS_CONF("Mapper Heap"),
    REDUCER_MEMORY_HEURISTICS_CONF("Reducer Memory"),
    REDUCER_HEAP_HEURISTICS_CONF("Reducer heap"),
    SORT_BUFFER_HEURISTICS_CONF("Sort Buffer"),
    SORT_FACTOR_HEURISTICS_CONF("Sort Factor"),
    SORT_SPILL_HEURISTICS_CONF("Sort Spill"),
    SPLIT_SIZE_HEURISTICS_CONF("Split Size"),
    PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF("Pig Max Split Size");
    private String value;
    ParameterKeys(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
