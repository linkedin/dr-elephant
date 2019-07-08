package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic;
import com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic;
import java.util.List;


/**
 * Constants describe different TuningType , Algorithm Type
 * Execution Engine and managers .
 * These are  used in AutoTuningFlow .
 */
public class Constant {
  public enum TuningType {HBT,OBT}
  public enum AlgorithmType {PSO,PSO_IPSO,HBT}
  public enum ExecutionEngineTypes{MR,SPARK}
  public enum TypeofManagers{AbstractBaselineManager,AbstractFitnessManager,AbstractJobStatusManager,AbstractParameterGenerateManager}



  public static final String JVM_MAX_HEAP_MEMORY_REGEX = ".*-Xmx([\\d]+)([mMgG]).*";
  public static final double YARN_VMEM_TO_PMEM_RATIO = 2.1;
  public static final int MB_IN_ONE_GB = 1024;
  public static final int SORT_BUFFER_CUSHION = 769;
  public static final int DEFAULT_CONTAINER_HEAP_MEMORY = 1536;
  public static final int OPTIMAL_MAPPER_SPEED_BYTES_PER_SECOND = 10;
  public static final double HEAP_MEMORY_TO_PHYSICAL_MEMORY_RATIO = 0.75D;
  public static final double SORT_SPILL_PERCENTAGE_THRESHOLD = 0.85D;
  public static final double MAPPER_MEMORY_SPILL_THRESHOLD_1 = 2.7D;
  public static final double MAPPER_MEMORY_SPILL_THRESHOLD_2 = 2.2D;
  public static final double MEMORY_TO_SORT_BUFFER_RATIO = 1.6;
  public static final double SPILL_PERCENTAGE_STEP_SIZE = 0.05d;

  public static String MAPPER_TIME_HEURISTIC_NAME;
  public static String MAPPER_SPEED_HEURISTIC_NAME;
  public static String MAPPER_MEMORY_HEURISTIC_NAME;
  public static String REDUCER_MEMORY_HEURISTIC_NAME;
  public static String MAPPER_SPILL_HEURISTIC_NAME;
  public static String REDUCER_TIME_HEURISTIC_NAME;

  public static String MAPPER_MAX_USED_PHYSICAL_MEMORY = "mapper.max.utilised.physical.memory";
  public static String MAPPER_MAX_USED_HEAP_MEMORY = "mapper.max.utilised.heap.memory";
  public static String MAPPER_MAX_USED_VIRTUAL_MEMORY = "mapper.max.utilised.virtual.memory";
  public static String REDUCER_MAX_USED_PHYSICAL_MEMORY = "reducer.max.utilised.physical.memory";
  public static String REDUCER_MAX_USED_HEAP_MEMORY = "reducer.max.utilised.heap.memory";
  public static String REDUCER_MAX_USED_VIRTUAL_MEMORY = "reducer.max.utilised.virtual.memory";
  public static String MAPPER_MAX_RECORD_SPILL_RATIO = "mapper.max.spill.ratio";
  public static String MAX_AVG_INPUT_SIZE_IN_BYTES = "max.average.input.size.in.bytes";

  static {
    List<Heuristic> heuristics =
        ElephantContext.instance().getHeuristicsForApplicationType(new ApplicationType("mapreduce"));
    for (Heuristic heuristic : heuristics) {
      if (heuristic.getHeuristicConfData().getClassName().equals(MapperTimeHeuristic.class.getCanonicalName())) {
        MAPPER_TIME_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperSpeedHeuristic.class.getCanonicalName())) {
        MAPPER_SPEED_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperMemoryHeuristic.class.getCanonicalName())) {
        MAPPER_MEMORY_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(MapperSpillHeuristic.class.getCanonicalName())) {
        MAPPER_SPILL_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(ReducerTimeHeuristic.class.getCanonicalName())) {
        REDUCER_TIME_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      } else if (heuristic.getHeuristicConfData()
          .getClassName()
          .equals(ReducerMemoryHeuristic.class.getCanonicalName())) {
        REDUCER_MEMORY_HEURISTIC_NAME = heuristic.getHeuristicConfData().getHeuristicName();
      }
    }
  }
  public enum HeuristicsToTuneByPigHbt {
    MAPPER_MEMORY(MAPPER_MEMORY_HEURISTIC_NAME),
    MAPPER_TIME(MAPPER_TIME_HEURISTIC_NAME),
    MAPPER_SPILL(MAPPER_SPILL_HEURISTIC_NAME),
    MAPPER_SPEED(MAPPER_SPEED_HEURISTIC_NAME),
    REDUCER_MEMORY(REDUCER_MEMORY_HEURISTIC_NAME),
    REDUCER_TIME(REDUCER_TIME_HEURISTIC_NAME);
    private String value;

    HeuristicsToTuneByPigHbt(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static boolean isMember(String heuristicName) {
      for (HeuristicsToTuneByPigHbt heuristicsToTune : HeuristicsToTuneByPigHbt.values()) {
        if (heuristicsToTune.value.equals(heuristicName)) {
          return true;
        }
      }
      return false;
    }
  }
}
