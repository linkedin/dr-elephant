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

package com.linkedin.drelephant.spark.legacydata;

import com.linkedin.drelephant.analysis.ApplicationType;
import java.util.Properties;


/**
 * This is a pseudo local implementation of SparkApplicationData interface, supposed to be used for test purpose.
 */
public class MockSparkApplicationData implements SparkApplicationData {
  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("SPARK");

  private final SparkGeneralData _sparkGeneralData;
  private final SparkEnvironmentData _sparkEnvironmentData;
  private final SparkExecutorData _sparkExecutorData;
  private final SparkJobProgressData _sparkJobProgressData;
  private final SparkStorageData _sparkStorageData;

  public MockSparkApplicationData() {
    _sparkGeneralData = new SparkGeneralData();
    _sparkEnvironmentData = new SparkEnvironmentData();
    _sparkExecutorData = new SparkExecutorData();
    _sparkJobProgressData = new SparkJobProgressData();
    _sparkStorageData = new SparkStorageData();
  }

  @Override
  public boolean isThrottled() {
    return false;
  }

  @Override
  public SparkGeneralData getGeneralData() {
    return _sparkGeneralData;
  }

  @Override
  public SparkEnvironmentData getEnvironmentData() {
    return _sparkEnvironmentData;
  }

  @Override
  public SparkExecutorData getExecutorData() {
    return _sparkExecutorData;
  }

  @Override
  public SparkJobProgressData getJobProgressData() {
    return _sparkJobProgressData;
  }

  @Override
  public SparkStorageData getStorageData() {
    return _sparkStorageData;
  }

  @Override
  public Properties getConf() {
    return getEnvironmentData().getSparkProperties();
  }

  @Override
  public String getAppId() {
    return getGeneralData().getApplicationId();
  }

  @Override
  public ApplicationType getApplicationType() {
    return APPLICATION_TYPE;
  }

  @Override
  public boolean isEmpty() {
    return getExecutorData().getExecutors().isEmpty();
  }
}
