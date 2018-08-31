package com.linkedin.drelephant.tuning.obt;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.engine.SparkExecutionEngine;
import models.TuningAlgorithm;
import org.apache.log4j.Logger;


public class OptimizationAlgoFactory {
  private static final Logger logger = Logger.getLogger(OptimizationAlgoFactory.class);

  public static TuningTypeManagerOBT getOptimizationAlogrithm(TuningAlgorithm tuningAlgorithm) {
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.PIG.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO_IPSO MR");
      return new TuningTypeManagerOBTAlgoIPSO(new MRExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.SPARK.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO_IPSO SPARK");
      return new TuningTypeManagerOBTAlgoIPSO(new SparkExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.PIG.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO PIG");
      return new TuningTypeManagerOBTAlgoPSO(new MRExecutionEngine());
    }
    if (tuningAlgorithm.optimizationAlgo.name().equals(TuningAlgorithm.OptimizationAlgo.PSO.name())
        && tuningAlgorithm.jobType.name().equals(TuningAlgorithm.JobType.SPARK.name())) {
      logger.info("OPTIMIZATION ALGORITHM PSO SPARK");
      return new TuningTypeManagerOBTAlgoPSO(new MRExecutionEngine());
    }
    logger.info("OPTIMIZATION ALGORITHM PSO");
    return null;
  }
}
