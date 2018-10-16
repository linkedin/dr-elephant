package com.linkedin.drelephant.tuning.obt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.AbstractBaselineManager;
import com.linkedin.drelephant.util.Utils;
import java.util.List;
import models.JobExecution;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import org.apache.log4j.Logger;


public class BaselineManagerOBT extends AbstractBaselineManager {
  private final Logger logger = Logger.getLogger(getClass());

  public BaselineManagerOBT() {
    NUM_JOBS_FOR_BASELINE_DEFAULT = 30;
    _numJobsForBaseline =
        Utils.getNonNegativeInt(configuration, super.BASELINE_EXECUTION_COUNT, NUM_JOBS_FOR_BASELINE_DEFAULT);
  }

  @Override
  protected List<TuningJobDefinition> detectJobsForBaseLineComputation() {
    logger.debug("Fetching jobs for which baseline metrics need to be computed");
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.averageResourceUsage, null)
        .or(Expr.eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.PSO.name()),
            Expr.eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
                TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name()))
        .findList();
    if(tuningJobDefinitions!=null){
      logger.debug("Total jobs for Baseline Computation in OBT");
    }
    return tuningJobDefinitions;
  }

  @Override
  public String getManagerName() {
    return "BaselineManagerOBT";
  }
}
