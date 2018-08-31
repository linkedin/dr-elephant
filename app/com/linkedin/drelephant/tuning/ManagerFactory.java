package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.TuningTypeManagerHBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBT;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.FitnessManagerOBTAlgoPSO;
import com.linkedin.drelephant.tuning.obt.OptimizationAlgoFactory;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoIPSO;
import com.linkedin.drelephant.tuning.obt.TuningTypeManagerOBTAlgoPSO;
import org.apache.log4j.Logger;



public class ManagerFactory {
  private static final Logger logger = Logger.getLogger(ManagerFactory.class);

  public static Manager getManager(String tuningType, String algorithmType, String executionEngineTypes,
      String typeOfManagers) {

    if(typeOfManagers.equals(Constant.TypeofManagers.AbstractJobStatusManager.name())){
    logger.info("Manager Type Azkaban Job Status Manager");
      return new AzkabanJobStatusManager();
    }

    else if (tuningType.equals(Constant.TuningType.HBT.name())) {
      return getMangersForHBT(algorithmType,executionEngineTypes,typeOfManagers);
    }
    else if (tuningType.equals(Constant.TuningType.OBT.name())) {
      return getManagersForOBT(algorithmType,executionEngineTypes,typeOfManagers);
    }

    return null;
  }
  private static Manager getMangersForHBT(String algorithmType, String executionEngineTypes,
      String typeOfManagers){
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractBaselineManager.name())) {
      logger.info("Manager Type Base line Manager HBT");
      return new BaselineManagerHBT();
    }
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractFitnessManager.name())) {
      logger.info("Manager Type Fitness Manager HBT");
      return new FitnessManagerHBT();
    }
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractTuningTypeManager.name())
        && executionEngineTypes.equals(Constant.ExecutionEngineTypes.MR.name())) {
      logger.info("Manager Type TuningType Manager HBT MR");
      return new TuningTypeManagerHBT(new MRExecutionEngine());
    }
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractTuningTypeManager.name())
        && executionEngineTypes.equals(Constant.ExecutionEngineTypes.SPARK.name())) {
      logger.info("Manager Type TuningType Manager HBT Spark");
      return new TuningTypeManagerHBT(new SparkExecutionEngine());
    }
    return null;
  }

  private static Manager getManagersForOBT(String algorithmType, String executionEngineTypes,
      String typeOfManagers ){
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractBaselineManager.name())) {
      logger.info("Manager Type Base line Manager OBT");
      return new BaselineManagerOBT();
    }
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractFitnessManager.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO.name())) {
      logger.info("Manager Type Fitness Manager OBT PSO");
      return new FitnessManagerOBTAlgoPSO();
    }
    if (typeOfManagers.equals(Constant.TypeofManagers.AbstractFitnessManager.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO_IPSO.name())) {
      logger.info("Manager Type Fitness Manager OBT IPSO");
      return new FitnessManagerOBTAlgoIPSO();
    }
    if(typeOfManagers.equals(Constant.TypeofManagers.AbstractTuningTypeManager.name())){
      return getManagerForOBTForTuningType(executionEngineTypes,algorithmType);
    }

    return null;
  }

  private static Manager getManagerForOBTForTuningType(String executionEngineTypes,String algorithmType){
    if (executionEngineTypes.equals(Constant.ExecutionEngineTypes.MR.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO_IPSO.name())) {
      logger.info("Manager Type TuningType Manager OBT IPSO MR");
      return new TuningTypeManagerOBTAlgoIPSO(new MRExecutionEngine());
    }
    if (executionEngineTypes.equals(Constant.ExecutionEngineTypes.MR.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO.name())) {
      logger.info("Manager Type TuningType Manager OBT PSO MR");
      return new TuningTypeManagerOBTAlgoPSO(new MRExecutionEngine());
    }
    if ( executionEngineTypes.equals(Constant.ExecutionEngineTypes.SPARK.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO_IPSO.name())) {
      logger.info("Manager Type TuningType Manager OBT IPSO Spark");
      return new TuningTypeManagerOBTAlgoIPSO(new SparkExecutionEngine());
    }
    if (executionEngineTypes.equals(Constant.ExecutionEngineTypes.SPARK.name()) && algorithmType.equals(
        Constant.AlgotihmType.PSO.name())) {
      logger.info("Manager Type TuningType Manager OBT PSO Spark");
      return new TuningTypeManagerOBTAlgoPSO(new SparkExecutionEngine());
    }
    return null;
  }
}
