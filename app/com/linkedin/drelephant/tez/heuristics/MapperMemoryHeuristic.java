/*
 * Copyright 2017 Electronic Arts Inc.
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
 *
 */
package com.linkedin.drelephant.tez.heuristics;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;

import com.linkedin.drelephant.tez.data.TezApplicationData;

import com.linkedin.drelephant.tez.data.TezTaskData;

import org.apache.log4j.Logger;

/**
 * Analyzes mapper memory allocation and requirements
 */
public class MapperMemoryHeuristic extends GenericMemoryHeuristic {

  private static final Logger logger = Logger.getLogger(MapperMemoryHeuristic.class);

  public static final String MAPPER_MEMORY_CONF = "mapreduce.map.memory.mb";

  public MapperMemoryHeuristic(HeuristicConfigurationData __heuristicConfData){
    super(MAPPER_MEMORY_CONF, __heuristicConfData);
  }

  @Override
  protected TezTaskData[] getTasks(TezApplicationData data) {
    return data.getMapTaskData();
  }

}