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

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.tunin.AzkabanJobCompleteDetector;
import com.linkedin.drelephant.tunin.BaselineComputeUtil;
import com.linkedin.drelephant.tunin.FitnessComputeUtil;
import com.linkedin.drelephant.tunin.JobCompleteDetector;
import com.linkedin.drelephant.tunin.PSOParamGenerator;
import com.linkedin.drelephant.tunin.ParamGenerator;
import com.linkedin.drelephant.util.Utils;


/**
 *This class is the AutoTuner Daemon class which runs following thing in order.
 * - BaselineComputeUtil: Baseline computation for new jobs which are auto tuning enabled
 * - JobCompleteDetector: Detect if the current execution of the jobs is completed and update the status in DB
 * - APIFitnessComputeUtil: Compute the recently succeeded jobs fitness
 * - ParamGenerator : Generate the next set of parameters for suggestion
 */
public class AutoTuner implements Runnable {

  private static final Logger logger = Logger.getLogger(AutoTuner.class);
  private static final long METRICS_COMPUTATION_INTERVAL = 60 * 1000 / 5;

  public static final String AUTO_TUNING_ENABLED = "autotuning.enabled";
  public static final String AUTO_TUNING_DAEMON_WAIT_INTERVAL = "autotuning.daemon.wait.interval.ms";

  public void run() {

    HDFSContext.load();
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Boolean autoTuningEnabled = configuration.getBoolean(AUTO_TUNING_ENABLED, false);

    if (autoTuningEnabled) {
      Long interval =
          Utils.getNonNegativeLong(configuration, AUTO_TUNING_DAEMON_WAIT_INTERVAL, METRICS_COMPUTATION_INTERVAL);

      try {
        try {
          BaselineComputeUtil baselineComputeUtil = new BaselineComputeUtil();
          JobCompleteDetector jobCompleteDetector = new AzkabanJobCompleteDetector();
          FitnessComputeUtil fitnessComputeUtil = new FitnessComputeUtil();
          ParamGenerator paramGenerator = new PSOParamGenerator();

          while (!Thread.currentThread().isInterrupted()) {

            baselineComputeUtil.computeBaseline();
            jobCompleteDetector.updateCompletedExecutions();
            fitnessComputeUtil.updateFitness();
            paramGenerator.getParams();

            Thread.sleep(interval);
          }

        } catch (Exception e) {
          logger.error("Error in auto tuner thread ", e);
        }

      } catch (Exception e) {
        logger.error("Error in auto tuner thread ", e);
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e1) {
        }
      }
    }
  }
}
