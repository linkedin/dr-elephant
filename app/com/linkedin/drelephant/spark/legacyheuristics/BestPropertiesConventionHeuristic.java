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

package com.linkedin.drelephant.spark.legacyheuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.spark.legacydata.SparkEnvironmentData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import com.linkedin.drelephant.util.Utils;
import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * This heuristic rule check some of the most commonly set spark properties and make sure the user is following
 * a best convention of them.
 */
public class BestPropertiesConventionHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(BestPropertiesConventionHeuristic.class);

  public static final String SPARK_SERIALIZER = "spark.serializer";
  public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
  public static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
  public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";

  // Severity parameters.
  private static final String NUM_CORE_SEVERITY = "num_core_severity";
  private static final String DRIVER_MEM_SEVERITY = "driver_memory_severity_in_gb";

  // Default value of parameters
  private double[] numCoreLimit= {2d};                   // Spark Executor Cores
  private double[] driverMemLimits = {4d, 4d, 8d, 8d};   // Spark Driver Memory

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confNumCoreLimit = Utils.getParam(paramMap.get(NUM_CORE_SEVERITY), numCoreLimit.length);
    if (confNumCoreLimit != null) {
      numCoreLimit = confNumCoreLimit;
    }
    logger.info(heuristicName + " will use " + NUM_CORE_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(numCoreLimit));

    double[] confDriverMemLimits = Utils.getParam(paramMap.get(DRIVER_MEM_SEVERITY), driverMemLimits.length);
    if (confDriverMemLimits != null) {
      driverMemLimits = confDriverMemLimits;
    }
    logger.info(heuristicName + " will use " + DRIVER_MEM_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(driverMemLimits));
    for (int i = 0; i < driverMemLimits.length; i++) {
      driverMemLimits[i] = (double) MemoryFormatUtils.stringToBytes(Double.toString(driverMemLimits[i]) + "G");
    }
  }

  public BestPropertiesConventionHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkEnvironmentData env = data.getEnvironmentData();
    String sparkSerializer = env.getSparkProperty(SPARK_SERIALIZER);
    String sparkDriverMemory = env.getSparkProperty(SPARK_DRIVER_MEMORY);
    String sparkShuffleManager = env.getSparkProperty(SPARK_SHUFFLE_MANAGER);
    String sparkExecutorCores = env.getSparkProperty(SPARK_EXECUTOR_CORES);
    int coreNum = sparkExecutorCores == null ? 1 : Integer.parseInt(sparkExecutorCores);

    Severity kryoSeverity =
        binarySeverity("org.apache.spark.serializer.KryoSerializer", sparkSerializer, true, Severity.MODERATE);
    Severity driverMemSeverity = getDriverMemorySeverity(MemoryFormatUtils.stringToBytes(sparkDriverMemory));
    Severity sortSeverity = binarySeverity("sort", sparkShuffleManager, true, Severity.MODERATE);
    Severity executorCoreSeverity = getCoreNumSeverity(coreNum);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), Severity.max(kryoSeverity, driverMemSeverity, sortSeverity,
        executorCoreSeverity), 0);

    result.addResultDetail(SPARK_SERIALIZER, propertyToString(sparkSerializer));
    result.addResultDetail(SPARK_DRIVER_MEMORY, propertyToString(sparkDriverMemory));
    result.addResultDetail(SPARK_SHUFFLE_MANAGER, propertyToString(sparkShuffleManager));
    result.addResultDetail(SPARK_EXECUTOR_CORES, propertyToString(sparkExecutorCores));

    return result;
  }

  private Severity getCoreNumSeverity(int cores) {
    if (cores > numCoreLimit[0]) {
      return Severity.CRITICAL;
    } else {
      return Severity.NONE;
    }
  }

  private Severity getDriverMemorySeverity(long mem) {
    return Severity.getSeverityAscending(
        mem, driverMemLimits[0], driverMemLimits[1], driverMemLimits[2], driverMemLimits[3]);
  }

  private static Severity binarySeverity(String expectedValue, String actualValue, boolean ignoreNull,
      Severity severity) {
    if (actualValue == null) {
      if (ignoreNull) {
        return Severity.NONE;
      } else {
        return severity;
      }
    }

    if (actualValue.equals(expectedValue)) {
      return Severity.NONE;
    } else {
      return severity;
    }
  }

  private static String propertyToString(String val) {
    return val == null ? "Not presented. Using default" : val;
  }
}
