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

import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;

import java.util.Arrays;



/**
 * This Heuristic analyses the skewness in the reducer input data
 */
public class ReducerSkewHeuristic extends GenericSkewHeuristic {

  public ReducerSkewHeuristic(HeuristicConfigurationData heuristicConfData) {
    super(Arrays.asList(MapReduceCounterData.CounterName.REDUCE_SHUFFLE_BYTES), heuristicConfData);
  }

  @Override
  protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
    return data.getReducerData();
  }
}
