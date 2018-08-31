package com.linkedin.drelephant.tuning.hbt;

import com.avaje.ebean.Expr;
import com.linkedin.drelephant.tuning.AbstractBaselineManager;
import com.linkedin.drelephant.util.Utils;
import java.util.List;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import org.apache.log4j.Logger;


public class BaselineManagerHBT extends AbstractBaselineManager {
  private final Logger logger = Logger.getLogger(getClass());
  public BaselineManagerHBT() {
    NUM_JOBS_FOR_BASELINE_DEFAULT = 20;
    _numJobsForBaseline =
        Utils.getNonNegativeInt(configuration, super.BASELINE_EXECUTION_COUNT, NUM_JOBS_FOR_BASELINE_DEFAULT);
  }

  @Override
  protected List<TuningJobDefinition> detectJobsForBaseLineComputation() {

    logger.info("Fetching jobs for HBT which baseline metrics need to be computed");
    List<TuningJobDefinition> tuningJobDefinitions = TuningJobDefinition.find.where()
        .eq(TuningJobDefinition.TABLE.averageResourceUsage, null)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.optimizationAlgo,
            TuningAlgorithm.OptimizationAlgo.HBT.name())
        .findList();
    if(tuningJobDefinitions!=null){
      logger.info("Total jobs for Baseline Computation in HBT " +tuningJobDefinitions.size());
    }
    return tuningJobDefinitions;
  }

  @Override
  public String getManagerName() {
    return "BaselineManagerHBT";
  }
}
