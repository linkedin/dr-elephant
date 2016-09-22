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

package com.linkedin.drelephant.analysis;

import java.util.Properties;


/**
 * This interface indicates that a class is holding the information of a Hadoop application
 */
public interface HadoopApplicationData {

  /**
   * Returns the unique id to identify an application run.
   *
   * @return the id
   */
  public String getAppId();

  /**
   * Returns the configuration of an application.
   *
   * @return A java Properties that contains the application configuration
   */
  public Properties getConf();

  /**
   * Returns the application type this data is for
   *
   * @return the application type
   */
  public ApplicationType getApplicationType();

  /**
   * Indicate if the data holder is actually empty (nothing is set up).
   *
   * @return true if the data holder is empty else false
   */
  public boolean isEmpty();

  /**
   * Returns the status of the app.
   *
   * @return status of the app
   */
  public String getStatus();

  /**
   * Returns the start time of the app.
   *
   * @return start time of the app
   */
  public long getStartTime();

  /**
   * Returns the finish time of the app.
   *
   * @return finish time of the app
   */
  public long getFinishTime();
}
