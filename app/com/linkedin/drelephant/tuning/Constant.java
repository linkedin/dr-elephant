package com.linkedin.drelephant.tuning;

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

  public enum HeuristicsToTuneByPigHbt {
    MAPPER_MEMORY("Mapper Memory"),
    MAPPER_TIME("Mapper Time"),
    MAPPER_SPILL("Mapper Spill"),
    MAPPER_SPEED("Mapper Speed"),
    REDUCER_MEMORY("Reducer Memory"),
    REDUCER_TIME("Reducer Time");
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
