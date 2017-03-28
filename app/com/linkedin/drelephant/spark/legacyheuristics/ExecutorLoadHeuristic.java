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
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData;
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import com.linkedin.drelephant.util.Utils;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.spark.legacydata.SparkExecutorData.EXECUTOR_DRIVER_NAME;


/**
 * This heuristic rule observes load details of executors
 */
public class ExecutorLoadHeuristic implements Heuristic<SparkApplicationData> {
  private static final Logger logger = Logger.getLogger(ExecutorLoadHeuristic.class);
  private static final long MEMORY_OBSERVATION_THRESHOLD = MemoryFormatUtils.stringToBytes("1 MB");

  // Severity parameters.
  private static final String LOOSER_METRIC_DEV_SEVERITY = "looser_metric_deviation_severity";
  private static final String METRIC_DEV_SEVERITY = "metric_deviation_severity";

  // Default value of parameters
  private double[] looserMetDevLimits = {0.8d, 1d, 1.2d, 1.4d};  // Max deviation from avg.
  private double[] metDevLimits = {0.4d, 0.6d, 0.8d, 1.0d};

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confLooserMetDevLimits = Utils.getParam(paramMap.get(LOOSER_METRIC_DEV_SEVERITY),
        looserMetDevLimits.length);
    if (confLooserMetDevLimits != null) {
      looserMetDevLimits = confLooserMetDevLimits;
    }
    logger.info(heuristicName + " will use " + LOOSER_METRIC_DEV_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(looserMetDevLimits));

    double[] confMetDevLimits = Utils.getParam(paramMap.get(METRIC_DEV_SEVERITY), metDevLimits.length);
    if (confMetDevLimits != null) {
      metDevLimits = confMetDevLimits;
    }
    logger.info(heuristicName + " will use " + METRIC_DEV_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(metDevLimits));
  }

  public ExecutorLoadHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  private class ValueObserver {
    private final long[] _values;
    private Long _min;
    private Long _max;
    private Long _avg;

    public ValueObserver(int size) {
      _values = new long[size];
    }

    public void set(int index, long value) {
      _values[index] = value;
      if (_min == null) {
        _min = value;
      } else {
        _min = Math.min(value, _min);
      }

      if (_max == null) {
        _max = value;
      } else {
        _max = Math.max(value, _max);
      }
    }

    public long getMin() {
      return _min == null ? 0L : _min;
    }

    public long getMax() {
      return _max == null ? 0L : _max;
    }

    public long getAvg() {
      if (_avg == null) {
        if (_values == null) {
          return 0L;
        }
        _avg = Statistics.average(_values);
      }
      return _avg;
    }

    /**
     * Max(|max-avg|, |min-avg|) / avg
     *
     * @return
     */
    public double getDeviationFactor() {
      long avg = getAvg();
      if (avg == 0) {
        return 0d;
      }
      long diff = Math.max(getMax() - avg, avg - getMin());
      return diff * 1.0d / avg;
    }
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkExecutorData executorData = data.getExecutorData();
    Set<String> executors = executorData.getExecutors();

    int numNonDriverExe = executors.size();
    if (executors.contains(EXECUTOR_DRIVER_NAME)) {
      numNonDriverExe -= 1;
    }
    ValueObserver peakMems = new ValueObserver(numNonDriverExe);
    ValueObserver durations = new ValueObserver(numNonDriverExe);
    ValueObserver inputBytes = new ValueObserver(numNonDriverExe);
    ValueObserver outputBytes = new ValueObserver(numNonDriverExe);

    int i = 0;
    for (String exeId : executors) {
      if (!exeId.equals(EXECUTOR_DRIVER_NAME)) {
        SparkExecutorData.ExecutorInfo info = executorData.getExecutorInfo(exeId);

        /* Ignore the memory variation and consider it as 0 if it is too small
         * The deviation of memory usage in KB level is too fluctuating to track.
         */
        if (info.memUsed < MEMORY_OBSERVATION_THRESHOLD) {
          peakMems.set(i, 0L);
        } else {
          peakMems.set(i, info.memUsed);
        }

        durations.set(i, info.duration);
        inputBytes.set(i, info.inputBytes);
        outputBytes.set(i, info.outputBytes);
        i += 1;
      }
    }

    Severity severity = Severity.max(getLooserMetricDeviationSeverity(peakMems), getMetricDeviationSeverity(durations),
        getMetricDeviationSeverity(inputBytes), getLooserMetricDeviationSeverity(outputBytes));

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, 0);

    result.addResultDetail("Average peak storage memory",
        String.format("%s (%s~%s)", MemoryFormatUtils.bytesToString(peakMems.getAvg()),
            MemoryFormatUtils.bytesToString(peakMems.getMin()), MemoryFormatUtils.bytesToString(peakMems.getMax())));
    result.addResultDetail("Average runtime",
        String.format("%s (%s~%s)", Statistics.readableTimespan(durations.getAvg()),
            Statistics.readableTimespan(durations.getMin()), Statistics.readableTimespan(durations.getMax())));
    result.addResultDetail("Average input size",
        String.format("%s (%s~%s)", MemoryFormatUtils.bytesToString(inputBytes.getAvg()),
            MemoryFormatUtils.bytesToString(inputBytes.getMin()), MemoryFormatUtils.bytesToString(inputBytes.getMax())));
    result.addResultDetail("Average output size",
        String.format("%s (%s~%s)", MemoryFormatUtils.bytesToString(outputBytes.getAvg()),
            MemoryFormatUtils.bytesToString(outputBytes.getMin()), MemoryFormatUtils.bytesToString(outputBytes.getMax())));

    return result;
  }

  /**
   * Some metrics by nature could deviate a bit more than other metrics. This method basically allows some metrics
   * to have looser severity thresholds.
   *
   * @param ob
   * @return the corresponding severity
   */
  private Severity getLooserMetricDeviationSeverity(ValueObserver ob) {
    double diffFactor = ob.getDeviationFactor();
    return Severity.getSeverityAscending(
        diffFactor, looserMetDevLimits[0], looserMetDevLimits[1], looserMetDevLimits[2], looserMetDevLimits[3]);
  }

  private Severity getMetricDeviationSeverity(ValueObserver ob) {
    double diffFactor = ob.getDeviationFactor();
    return Severity.getSeverityAscending(
        diffFactor, metDevLimits[0], metDevLimits[1], metDevLimits[2], metDevLimits[3]);
  }

}
