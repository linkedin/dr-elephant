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

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import java.util.Map;
import org.apache.log4j.Logger;

import java.util.ArrayList;


import java.util.*;

import com.linkedin.drelephant.util.Utils;

/**
 * Analyses garbage collection efficiency
 */
public abstract class GenericHelloWorld implements Heuristic<MapReduceApplicationData> {

  private static final Logger logger = Logger.getLogger(GenericHelloWorld.class);
  private HeuristicConfigurationData _heuristicConfData;
  
    
    private static final String DEVIATION_SEVERITY = "standard_deviation_severity";
    private double[] deviationLimits = {5, 10, 15, 20};

  protected GenericHelloWorld(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
  }
  
  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);
  
  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {


    if(!data.getSucceeded()) {
      return null;
    }


    Severity severity;
    severity = Severity.NONE;
    
    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
                                                 _heuristicConfData.getHeuristicName(),
                                                 severity,
                                                 Utils.getHeuristicScore(severity, 0));

    result.addResultDetail("Hello World!", "Hello World!");


    return result;
  }
}




/*
    MapReduceTaskData[] mapTasks = data.getMapperData();
    MapReduceTaskData[] reduceTasks = data.getReducerData();

    List<Long> mapInputObjects = new ArrayList<Long>();
    List<Long> mapOutputObjects = new ArrayList<Long>();

    for (MapReduceTaskData task : mapTasks) {      // In-Out mapper
      if (task.isSampled()) {
        mapInputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT));    //nombre de In de chaque tache
        mapOutputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT));    //nombre de Out de chaque tache
      }
    }

    List<Long> ReduceInputObjects = new ArrayList<Long>();
    List<Long> ReduceOutputObjects = new ArrayList<Long>();

    for (MapReduceTaskData task : reduceTasks) {    // In-Out reducer
      if (task.isSampled()) {
        ReduceInputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT));    //nombre de In de chaque tache
        ReduceOutputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT));    //nombre de Out de chaque tache
      }
    }

    long in = Utils.sum(mapInputObjects) + Utils.sum(ReduceInputObjects);
    long out = Utils.sum(mapOutputObjects) + Utils.sum(ReduceOutputObjects);

    String str = "";
    for (long value : mapInputObjects) {
      str += value + ", ";
    }

    String str2 = "";
    for (long value : mapOutputObjects) {
      str2 += value + ", ";
    }

    String str3 = "";
    for (long value : ReduceInputObjects) {
      str3 += value + ", ";
    }

    String str4 = "";
    for (long value : ReduceOutputObjects) {
      str4 += value + ", ";
    }

    

    result.addResultDetail("mapInputObjects", str);
    result.addResultDetail("mapOutputObjects", str2);
    result.addResultDetail("ReduceInputObjects", str3);
    result.addResultDetail("ReduceOutputObjects", str4);
    
    result.addResultDetail("Total in = ", data.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT) + "");
    result.addResultDetail("Total out = ", data.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT) + "");
    result.addResultDetail("in|out", data.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT) + "|" +data.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT));
    
                    
*/

