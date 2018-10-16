package com.linkedin.drelephant.tuning;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


/**
 *  This is the Flow , which create four pipeline and each pipeline executes in seperate thread.
 *  Four pipeline are
 *  BaseLineManager : This will have two manager BaselineManagerOBT, BaselineManagerHBT
 *  JobStatusManagerPipeline: This will have one manager , AzkabanJobStatusManager
 *  FitnessManager : This will have three managers FitnessManagerHBT,FitnessManagerOBTAlgoIPSO,FitnessManagerOBTPSO
 *  TuningTypeManager : This will have Combinations of TuningType , AlgorithmType and execution engine. for e.g
 *  TuningTypeManagerOBTAlgoIPSO for Map Reduce and Spark ...
 */
public class AutoTuningFlow {
  List<List<Manager>> pipelines = null;
  private static final Logger logger = Logger.getLogger(AutoTuningFlow.class);

  public AutoTuningFlow() {
    pipelines = new ArrayList<List<Manager>>();
    createBaseLineManagersPipeline();
    createJobStatusManagersPipeline();
    createFitnessManagersPipeline();
    createTuningTypeManagersPipeline();
  }

  public List<List<Manager>> getPipeline() {
    return pipelines;
  }

  public void executeFlow() throws InterruptedException {
    for (final List<Manager> pipelineType : this.pipelines) {
      Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
            for (Manager manager : pipelineType) {
              logger.info(" Starting Manager  " + manager.getManagerName());
              Boolean execute = manager.execute();
             logger.info(" Ending Manager " + execute + " " + manager.getManagerName());
          }
        }
      });
      t1.start();
      t1.join();
    }
  }

  private void createBaseLineManagersPipeline() {
    List<Manager> baselineManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {

      baselineManagers.add(
          ManagerFactory.getManager(tuningType.name(), null, null, AbstractBaselineManager.class.getSimpleName()));
    }
    this.pipelines.add(baselineManagers);
  }

  private void createJobStatusManagersPipeline() {
    List<Manager> jobStatusManagers = new ArrayList<Manager>();
    jobStatusManagers.add(ManagerFactory.getManager(null, null, null, AbstractJobStatusManager.class.getSimpleName()));
    //jobStatusManagers.add(new JobStatusManagerOBT());
    this.pipelines.add(jobStatusManagers);
  }

  private void createFitnessManagersPipeline() {
    List<Manager> fitnessManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {
      for (Constant.AlgotihmType algotihmType : Constant.AlgotihmType.values()) {
        Manager manager = ManagerFactory.getManager(tuningType.name(), algotihmType.name(), null,
            AbstractFitnessManager.class.getSimpleName());
        if (manager != null) {
          logger.info("Loading " + manager.getManagerName());
          fitnessManagers.add(manager);
        }
      }
    }

    this.pipelines.add(fitnessManagers);
  }

  private void createTuningTypeManagersPipeline() {
    List<Manager> algorithmManagers = new ArrayList<Manager>();
    for (Constant.TuningType tuningType : Constant.TuningType.values()) {
      for (Constant.AlgotihmType algotihmType : Constant.AlgotihmType.values()) {
        for (Constant.ExecutionEngineTypes executionEngineTypes : Constant.ExecutionEngineTypes.values()) {
          Manager manager =
              ManagerFactory.getManager(tuningType.name(), algotihmType.name(), executionEngineTypes.name(),
                  AbstractParameterGenerateManager.class.getSimpleName());
          if (manager != null) {
            logger.info("Loading " + manager.getManagerName());
            algorithmManagers.add(manager);
          }
        }
      }
    }
    this.pipelines.add(algorithmManagers);
  }


}
