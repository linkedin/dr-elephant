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

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;

import org.apache.log4j.Logger;


/*
Heuristics to collect memory data/counter values  about application previous exeution.
 */
public class ConfigurationHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ConfigurationHeuristic.class);

  private HeuristicConfigurationData _heuristicConfData;

  public ConfigurationHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    if (!data.getSucceeded()) {
      return null;
    }
    String mapperMemory = data.getConf().getProperty(MAPPER_MEMORY_HADOOP_CONF.getValue());
    String mapperHeap = data.getConf().getProperty(MAPPER_HEAP_HADOOP_CONF.getValue());
    if (mapperHeap == null) {
      mapperHeap = data.getConf().getProperty(CHILD_HEAP_SIZE_HADOOP_CONF.getValue());
    }
    String sortBuffer = data.getConf().getProperty(SORT_BUFFER_HADOOP_CONF.getValue());
    String sortFactor = data.getConf().getProperty(SORT_FACTOR_HADOOP_CONF.getValue());
    String sortSplill = data.getConf().getProperty(SORT_SPILL_HADOOP_CONF.getValue());
    String reducerMemory = data.getConf().getProperty(REDUCER_MEMORY_HADOOP_CONF.getValue());
    String reducerHeap = data.getConf().getProperty(REDUCER_HEAP_HADOOP_CONF.getValue());
    if (reducerHeap == null) {
      reducerHeap = data.getConf().getProperty(CHILD_HEAP_SIZE_HADOOP_CONF.getValue());
    }
    String splitSize = data.getConf().getProperty(SPLIT_SIZE_HADOOP_CONF.getValue());
    String pigSplitSize = data.getConf().getProperty(PIG_SPLIT_SIZE_HADOOP_CONF.getValue());
    HeuristicResult result =
        new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), Severity.LOW, 0);

    result.addResultDetail(MAPPER_MEMORY_HEURISTICS_CONF.getValue(), mapperMemory);
    result.addResultDetail(MAPPER_HEAP_HEURISTICS_CONF.getValue(), mapperHeap.replaceAll("\\s+", "\n"));
    result.addResultDetail(REDUCER_MEMORY_HEURISTICS_CONF.getValue(), reducerMemory);
    result.addResultDetail(REDUCER_HEAP_HEURISTICS_CONF.getValue(), reducerHeap.replaceAll("\\s+", "\n"));
    result.addResultDetail(SORT_BUFFER_HEURISTICS_CONF.getValue(), sortBuffer);
    result.addResultDetail(SORT_FACTOR_HEURISTICS_CONF.getValue(), sortFactor);
    result.addResultDetail(SORT_SPILL_HEURISTICS_CONF.getValue(), sortSplill);
    if (splitSize != null) {
      result.addResultDetail(SPLIT_SIZE_HEURISTICS_CONF.getValue(), splitSize);
    }
    if (pigSplitSize != null) {
      result.addResultDetail(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue(), pigSplitSize);
    }

    return result;
  }
}
