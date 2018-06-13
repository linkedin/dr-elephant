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

import com.google.common.base.Strings;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import com.linkedin.drelephant.util.Utils;
import org.apache.log4j.Logger;

import org.apache.commons.io.FileUtils;

import java.util.*;

/**
 * Analyzes mapper memory allocation and requirements
 */
public abstract class GenericMemoryHeuristic implements Heuristic<TezApplicationData> {

  private static final Logger logger = Logger.getLogger(GenericMemoryHeuristic.class);

  //Severity Parameters
  private static final String MEM_RATIO_SEVERITY = "memory_ratio_severity";
  private static final String DEFAULT_MAPPER_CONTAINER_SIZE = "2048";
  private static final String CONTAINER_MEM_DEFAULT_MB = "container_memory_default_mb";
  private String _containerMemConf;

  //Default Value of parameters

  private double [] memoryRatioLimits = {0.6d, 0.5d, 0.4d, 0.3d}; //Ratio of successful tasks

  private HeuristicConfigurationData _heuristicConfData;

  private String getContainerMemDefaultMBytes() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    if (paramMap.containsKey(CONTAINER_MEM_DEFAULT_MB)) {
      String strValue = paramMap.get(CONTAINER_MEM_DEFAULT_MB);
      try {
        return strValue;
      }
      catch (NumberFormatException e) {
        logger.warn(CONTAINER_MEM_DEFAULT_MB + ": expected number [" + strValue + "]");
      }
    }
    return DEFAULT_MAPPER_CONTAINER_SIZE;
  }

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confSuccessRatioLimits = Utils.getParam(paramMap.get(MEM_RATIO_SEVERITY), memoryRatioLimits.length);
    if (confSuccessRatioLimits != null) {
      memoryRatioLimits = confSuccessRatioLimits;
    }
    logger.info(heuristicName + " will use " + MEM_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(memoryRatioLimits));


  }

  public GenericMemoryHeuristic(String containerMemConf, HeuristicConfigurationData heuristicConfData) {
    this._containerMemConf = containerMemConf;
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  protected abstract TezTaskData[] getTasks(TezApplicationData data);

  public HeuristicResult apply(TezApplicationData data) {
    if(!data.getSucceeded()) {
      return null;
    }
    TezTaskData[] tasks = getTasks(data);


    List<Long> totalPhysicalMemory = new LinkedList<Long>();
    List<Long> totalVirtualMemory = new LinkedList<Long>();
    List<Long> runTime = new LinkedList<Long>();

    for (TezTaskData task : tasks) {

      if (task.isSampled()) {
        totalPhysicalMemory.add(task.getCounters().get(TezCounterData.CounterName.PHYSICAL_MEMORY_BYTES));
        totalVirtualMemory.add(task.getCounters().get(TezCounterData.CounterName.VIRTUAL_MEMORY_BYTES));
        runTime.add(task.getTotalRunTimeMs());
      }


    }

    long averagePMem = Statistics.average(totalPhysicalMemory);
    long averageVMem = Statistics.average(totalVirtualMemory);
    long maxPMem;
    long minPMem;
    try{
      maxPMem = Collections.max(totalPhysicalMemory);
      minPMem = Collections.min(totalPhysicalMemory);

    }
    catch(Exception exception){
      maxPMem = 0;
      minPMem = 0;
    }
    long averageRunTime = Statistics.average(runTime);

    String containerSizeStr;

    if(!Strings.isNullOrEmpty(data.getConf().getProperty(_containerMemConf))){
      containerSizeStr = data.getConf().getProperty(_containerMemConf);
    }
    else {
      containerSizeStr = getContainerMemDefaultMBytes();
    }

    long containerSize = Long.valueOf(containerSizeStr) * FileUtils.ONE_MB;

    double averageMemMb = (double)((averagePMem) /FileUtils.ONE_MB) ;

    double ratio = averageMemMb / ((double)(containerSize / FileUtils.ONE_MB));

    Severity severity ;

    if(tasks.length == 0){
      severity = Severity.NONE;
    }
    else{
      severity = getMemoryRatioSeverity(ratio);
    }

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

    result.addResultDetail("Number of tasks", Integer.toString(tasks.length));
    result.addResultDetail("Maximum Physical Memory (MB)",
        tasks.length == 0 ? "0" : Long.toString(maxPMem/FileUtils.ONE_MB));
    result.addResultDetail("Minimum Physical memory (MB)",
        tasks.length == 0 ? "0" : Long.toString(minPMem/FileUtils.ONE_MB));
    result.addResultDetail("Average Physical Memory (MB)",
        tasks.length == 0 ? "0" : Long.toString(averagePMem/FileUtils.ONE_MB));
    result.addResultDetail("Average Virtual Memory (MB)",
        tasks.length == 0 ? "0" : Long.toString(averageVMem/FileUtils.ONE_MB));
    result.addResultDetail("Average Task RunTime",
        tasks.length == 0 ? "0" : Statistics.readableTimespan(averageRunTime));
    result.addResultDetail("Requested Container Memory (MB)",
        (tasks.length == 0 || containerSize == 0 || containerSize == -1) ? "0" : String.valueOf(containerSize / FileUtils.ONE_MB));


    return result;

  }

  private Severity getMemoryRatioSeverity(double ratio) {
    return Severity.getSeverityDescending(
        ratio, memoryRatioLimits[0], memoryRatioLimits[1], memoryRatioLimits[2], memoryRatioLimits[3]);
  }




}