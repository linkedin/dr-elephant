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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.HDFSContext;


/**
 * The main class which starts Dr. Elephant
 */
public class DrElephant extends Thread {
  public static final String AUTO_TUNING_ENABLED = "autotuning.enabled";
  private static final Logger logger = Logger.getLogger(DrElephant.class);

  private ElephantRunner _elephant;
  private AutoTuner _autoTuner;
  private Thread _autoTunerThread;

  private Boolean autoTuningEnabled;

  public DrElephant() throws IOException {
    HDFSContext.load();
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    autoTuningEnabled = configuration.getBoolean(AUTO_TUNING_ENABLED, false);
    logger.debug("Auto Tuning Configuration: " + configuration.toString());
    _elephant = new ElephantRunner();
    if (autoTuningEnabled) {
      _autoTuner = new AutoTuner();
      _autoTunerThread = new Thread(_autoTuner, "Auto Tuner Thread");
    }
  }

  @Override
  public void run() {
    if (_autoTunerThread != null) {
      logger.debug("Starting auto tuner thread ");
      _autoTunerThread.start();
    }
    _elephant.run();
  }

  public void kill() {
    if (_elephant != null) {
      _elephant.kill();
    }
    if (_autoTunerThread != null) {
      _autoTunerThread.interrupt();
    }
  }
}
