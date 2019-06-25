package com.linkedin.drelephant.mapreduce.heuristics;

public class CommonConstantsHeuristic {

  public static final String MAPPER_SPEED = "Mapper Speed";
  public static final String MAPPER_SPILL = "Mapper Spill";
  public static final String MAPPER_MEMORY = "Mapper Memory";
  public static final String MAPPER_GC = "Mapper GC";
  public static final String MAPPER_TIME = "Mapper Time";
  public static final String REDUCER_MEMORY = "Reducer Memory";
  public static final String REDUCER_TIME = "Reducer Time";
  public static final String MAPREDUCE_CONFIGURATION = "MapReduceConfiguration";
  public static final String TOTAL_INPUT_SIZE_IN_MB = "Total input size in MB";
  public static final String AVG_INPUT_SIZE_IN_BYTES = "Avg Input Size (Bytes)";
  public static final String MAPPER_OUTPUT_RECORD_SPILL_RATIO = "Ratio of spilled records to output records";

  public enum MRJobTaskType{MAP, REDUCE};

  public enum UtilizedParameterKeys {
    AVG_PHYSICAL_MEMORY("Avg Physical Memory (MB)"),
    MAX_PHYSICAL_MEMORY("Max Physical Memory (MB)"),
    MIN_PHYSICAL_MEMORY("Min Physical Memory (MB)"),
    AVG_VIRTUAL_MEMORY("Avg Virtual Memory (MB)"),
    MAX_VIRTUAL_MEMORY("Max Virtual Memory (MB)"),
    MIN_VIRTUAL_MEMORY("Min Virtual Memory (MB)"),
    AVG_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Avg Total Committed Heap Usage Memory (MB)"),
    MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Max Total Committed Heap Usage Memory (MB)"),
    MIN_TOTAL_COMMITTED_HEAP_USAGE_MEMORY("Min Total Committed Heap Usage Memory (MB)"),
    AVERAGE_TASK_INPUT_SIZE("Average task input size"),
    AVERAGE_TASK_RUNTIME("Average task runtime"),
    NUMBER_OF_TASK("Number of tasks"),
    RATIO_OF_SPILLED_RECORDS_TO_OUTPUT_RECORDS("Ratio of spilled records to output records"),
    SORT_BUFFER("Sort Buffer"),
    SORT_SPILL("Sort Spill");
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
    NUMBER_OF_REDUCER_CONF("mapreduce.job.reduces"),
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
