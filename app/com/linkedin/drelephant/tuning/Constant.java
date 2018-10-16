package com.linkedin.drelephant.tuning;

import java.util.HashMap;
import java.util.Map;


/**
 * Constants describe different TuningType , Algorithm Type
 * Execution Engine and managers .
 * These are  used in AutoTuningFlow .
 */
public class Constant {
  public enum TuningType {HBT,OBT}
  public enum AlgotihmType{PSO,PSO_IPSO,HBT}
  public enum ExecutionEngineTypes{MR,SPARK}
  public enum TypeofManagers{AbstractBaselineManager,AbstractFitnessManager,AbstractJobStatusManager,AbstractParameterGenerateManager}

}
