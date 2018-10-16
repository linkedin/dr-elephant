/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant;

import com.linkedin.drelephant.tuning.AutoTuningFlow;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.util.Utils;

import controllers.AutoTuningMetricsController;


/**
 *This class is the AutoTuner Daemon class which runs following thing in order.
 * - BaselineComputeUtil: Baseline computation for new jobs which are auto tuning enabled
 * - JobCompleteDetector: Detect if the current execution of the jobs is completed and update the status in DB
 * - APIFitnessComputeUtil: Compute the recently succeeded jobs fitness
 * - ParamGenerator : Generate the next set of parameters for suggestion
 */
public class AutoTuner implements Runnable {

  public static final long ONE_MIN = 60 * 1000;
  private static final Logger logger = Logger.getLogger(AutoTuner.class);
  private static final long DEFAULT_METRICS_COMPUTATION_INTERVAL = ONE_MIN / 5;
  public static final String AUTO_TUNING_DAEMON_WAIT_INTERVAL = "autotuning.daemon.wait.interval.ms";


  /**
   *  It will start auto tuning thread .
   */
  public void run() {
    logger.info("Starting Auto Tuning thread");
    HDFSContext.load();
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Long interval =
        Utils.getNonNegativeLong(configuration, AUTO_TUNING_DAEMON_WAIT_INTERVAL, DEFAULT_METRICS_COMPUTATION_INTERVAL);
    try {
      AutoTuningMetricsController.init();
      AutoTuningFlow autoTuningFlow= new AutoTuningFlow();
      while (!Thread.currentThread().isInterrupted()) {
        try {
          autoTuningFlow.executeFlow();
        } catch (Exception e) {
          logger.error("Error in auto tuner thread ", e);
        }
        Thread.sleep(interval);
      }
    } catch (Exception e) {
      logger.error("Error in auto tuner thread ", e);
    }
    logger.info("Auto tuning thread shutting down");
  }
}
