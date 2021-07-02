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

package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningParameter;
import org.apache.log4j.Logger;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.TuningParameterConstraint;
import org.apache.commons.io.FileUtils;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.*;
import static java.lang.Double.*;
import static java.lang.Math.max;
import static java.lang.Math.min;


/*
This Implements IPSO in AutoTuning Framework

 */
public class IPSOManager implements AutoTuningOptimizeManager {
  private static final Logger logger = Logger.getLogger(IPSOManager.class);
  private Map<String, Map<String, Double>> usageDataGlobal = null;
  private String functionTypes[] = {"map", "reduce"};

  enum UsageCounterSchema {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

  @Override
  public void intializePrerequisite(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet) {
    logger.info(" Intialize Prerequisite ");
    setDefaultParameterValues(tuningAlgorithm, jobSuggestedParamSet);
  }

  private void setDefaultParameterValues(TuningAlgorithm tuningAlgorithm, JobSuggestedParamSet jobSuggestedParamSet) {
    List<TuningParameter> tuningParameters =
        TuningParameter.find.where().eq(TuningParameter.TABLE.tuningAlgorithm, tuningAlgorithm).findList();
    for (TuningParameter tuningParameter : tuningParameters) {
      TuningParameterConstraint tuningParameterConstraint = new TuningParameterConstraint();
      tuningParameterConstraint.jobDefinition = jobSuggestedParamSet.jobDefinition;
      tuningParameterConstraint.tuningParameter = tuningParameter;
      tuningParameterConstraint.lowerBound = tuningParameter.minValue;
      tuningParameterConstraint.upperBound = tuningParameter.maxValue;
      tuningParameterConstraint.constraintType = TuningParameterConstraint.ConstraintType.BOUNDARY;
      tuningParameterConstraint.save();
    }
  }

  @Override
  public Map<String, Map<String, Double>> extractParameterInformation(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information");
    usageDataGlobal = new HashMap<String, Map<String, Double>>();
    intialize();
    for (AppResult appResult : appResults) {
      Map<String, Map<String, Double>> usageDataApplicationlocal = collectUsageDataPerApplication(appResult);
      for (String functionType : usageDataApplicationlocal.keySet()) {
        Map<String, Double> usageDataForFunctionGlobal = usageDataGlobal.get(functionType);
        Map<String, Double> usageDataForFunctionlocal = usageDataApplicationlocal.get(functionType);
        for (String usageName : usageDataForFunctionlocal.keySet()) {
          usageDataForFunctionGlobal.put(usageName,
              max(usageDataForFunctionGlobal.get(usageName), usageDataForFunctionlocal.get(usageName)));
        }
      }
    }
    logger.debug("Usage Values Global ");
    printInformation(usageDataGlobal);
    return usageDataGlobal;
  }

  private void intialize() {
    for (String function : functionTypes) {
      Map<String, Double> usageData = new HashMap<String, Double>();
      for (UtilizedParameterKeys value : UtilizedParameterKeys.values()) {
        usageData.put(value.getValue(), 0.0);
      }
      usageDataGlobal.put(function, usageData);
    }
  }

  private Map<String, Map<String, Double>> collectUsageDataPerApplication(AppResult appResult) {
    Map<String, Map<String, Double>> usageData = new HashMap<String, Map<String, Double>>();
    if (appResult.yarnAppHeuristicResults != null) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {

        if (appHeuristicResult.heuristicName.equals("Mapper Memory")) {
          Map<String, Double> counterData = new HashMap<String, Double>();
          collectUsageDataPerApplicationForFunction(appHeuristicResult, counterData);
          usageData.put("map", counterData);
        }
        if (appHeuristicResult.heuristicName.equals("Reducer Memory")) {
          Map<String, Double> counterData = new HashMap<String, Double>();
          collectUsageDataPerApplicationForFunction(appHeuristicResult, counterData);
          usageData.put("reduce", counterData);
        }
      }
    }
    logger.debug("Usage Values local   " + appResult.jobExecUrl);
    printInformation(usageData);
    return usageData;
  }

  private void printInformation(Map<String, Map<String, Double>> information) {
    for (String functionType : information.keySet()) {
      logger.debug("function Type    " + functionType);
      Map<String, Double> usage = information.get(functionType);
      for (String data : usage.keySet()) {
        logger.debug(data + " " + usage.get(data));
      }
    }
  }

  private void collectUsageDataPerApplicationForFunction(AppHeuristicResult appHeuristicResult,
      Map<String, Double> counterData) {
    if (appHeuristicResult.yarnAppHeuristicResultDetails != null) {
      for (AppHeuristicResultDetails appHeuristicResultDetails : appHeuristicResult.yarnAppHeuristicResultDetails) {
        for (UtilizedParameterKeys value : UtilizedParameterKeys.values()) {
          if (appHeuristicResultDetails.name.equals(value.getValue())) {
            counterData.put(value.getValue(), appHeuristicResultDetails.value == null ? 0
                : ((double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value)));
          }
        }
      }
    }
  }

  @Override
  public void parameterOptimizer(Integer jobID) {
    logger.info(" IPSO Optimizer");
    List<TuningParameterConstraint> parameterConstraints = TuningParameterConstraint.find.where().
        eq("job_definition_id", jobID).findList();
    for (String function : functionTypes) {
      logger.debug(" Optimizing Parameter Space  " + function);
      if (usageDataGlobal.get(function).get(UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue()) > 0.0) {
        List<Double> usageStats = extractUsageParameter(function);
        Map<String, TuningParameterConstraint> memoryConstraints =
            filterMemoryConstraint(parameterConstraints, function);
        memoryParameterIPSO(function, memoryConstraints, usageStats);
      }
    }
  }

  private Map<String, TuningParameterConstraint> filterMemoryConstraint(
      List<TuningParameterConstraint> parameterConstraints, String functionType) {
    Map<String, TuningParameterConstraint> memoryConstraints = new HashMap<String, TuningParameterConstraint>();
    for (TuningParameterConstraint parameterConstraint : parameterConstraints) {
      if (functionType.equals("map")) {
        if (parameterConstraint.tuningParameter.paramName.equals(ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
      if (functionType.equals("reduce")) {
        if (parameterConstraint.tuningParameter.paramName.equals(ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
    }
    return memoryConstraints;
  }

  private List<Double> extractUsageParameter(String functionType) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    usedPhysicalMemoryMB =
        usageDataGlobal.get(functionType).get(UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue());
    usedVirtualMemoryMB = usageDataGlobal.get(functionType).get(UtilizedParameterKeys.MAX_VIRTUAL_MEMORY.getValue());
    usedHeapMemoryMB =
        usageDataGlobal.get(functionType).get(UtilizedParameterKeys.MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    logger.debug(" Usage Stats " + functionType);
    logger.debug(" Physical Memory Usage MB " + usedPhysicalMemoryMB);
    logger.debug(" Virtual Memory Usage MB " + usedVirtualMemoryMB / 2.1);
    logger.debug(" Heap Usage MB " + usedHeapMemoryMB);
    List<Double> usageStats = new ArrayList<Double>();
    usageStats.add(usedPhysicalMemoryMB);
    usageStats.add(usedVirtualMemoryMB);
    usageStats.add(usedHeapMemoryMB);
    return usageStats;
  }

  private void memoryParameterIPSO(String trigger, Map<String, TuningParameterConstraint> constraints,
      List<Double> usageStats) {
    logger.info(" IPSO for " + trigger);
    Double usagePhysicalMemory = usageStats.get(UsageCounterSchema.USED_PHYSICAL_MEMORY.ordinal());
    Double usageVirtualMemory = usageStats.get(UsageCounterSchema.USED_VIRTUAL_MEMORY.ordinal());
    Double usageHeapMemory = usageStats.get(UsageCounterSchema.USED_HEAP_MEMORY.ordinal());
    Double memoryMB =
        applyContainerSizeFormula(constraints.get("CONTAINER_MEMORY"), usagePhysicalMemory, usageVirtualMemory);
    applyHeapSizeFormula(constraints.get("CONTAINER_HEAP"), usageHeapMemory, memoryMB);
  }

  private Double applyContainerSizeFormula(TuningParameterConstraint containerConstraint, Double usagePhysicalMemory,
      Double usageVirtualMemory) {
    Double memoryMB = max(usagePhysicalMemory, usageVirtualMemory / (2.1));
    Double containerSizeLower = getContainerSize(memoryMB);
    Double containerSizeUpper = getContainerSize(1.2 * memoryMB);
    logger.debug(" Previous Lower Bound  Memory  " + containerConstraint.lowerBound);
    logger.debug(" Previous Upper Bound  Memory " + containerConstraint.upperBound);
    logger.debug(" Current Lower Bound  Memory  " + containerSizeLower);
    logger.debug(" Current Upper Bound  Memory " + containerSizeUpper);
    containerConstraint.lowerBound = containerSizeLower;
    containerConstraint.upperBound = containerSizeUpper;
    containerConstraint.save();
    return memoryMB;
  }

  private void applyHeapSizeFormula(TuningParameterConstraint containerHeapSizeConstraint, Double usageHeapMemory,
      Double memoryMB) {
    Double heapSizeLowerBound = min(0.75 * memoryMB, usageHeapMemory);
    Double heapSizeUpperBound = heapSizeLowerBound * 1.2;
    logger.debug(" Previous Lower Bound  XMX  " + containerHeapSizeConstraint.lowerBound);
    logger.debug(" Previous Upper Bound  XMX " + containerHeapSizeConstraint.upperBound);
    logger.debug(" Current Lower Bound  XMX  " + heapSizeLowerBound);
    logger.debug(" Current Upper Bound  XMX " + heapSizeUpperBound);
    containerHeapSizeConstraint.lowerBound = heapSizeLowerBound;
    containerHeapSizeConstraint.upperBound = heapSizeUpperBound;
    containerHeapSizeConstraint.save();
  }

  private Double getContainerSize(Double memory) {
    return Math.ceil(memory / 1024.0) * 1024;
  }

  @Override
  public void applyIntelligenceOnParameter(List<TuningParameter> tuningParameterList, JobDefinition job) {
    logger.info(" Apply Intelligence");
    List<TuningParameterConstraint> tuningParameterConstraintList = new ArrayList<TuningParameterConstraint>();
    try {
      tuningParameterConstraintList = TuningParameterConstraint.find.where()
          .eq("job_definition_id", job.id)
          .eq(TuningParameterConstraint.TABLE.constraintType, TuningParameterConstraint.ConstraintType.BOUNDARY)
          .findList();
    } catch (NullPointerException e) {
      logger.info("No boundary constraints found for job: " + job.jobName);
    }

    Map<Integer, Integer> paramConstrainIndexMap = new HashMap<Integer, Integer>();
    int i = 0;
    for (TuningParameterConstraint tuningParameterConstraint : tuningParameterConstraintList) {
      paramConstrainIndexMap.put(tuningParameterConstraint.tuningParameter.id, i);
      i += 1;
    }

    for (TuningParameter tuningParameter : tuningParameterList) {
      if (paramConstrainIndexMap.containsKey(tuningParameter.id)) {
        int index = paramConstrainIndexMap.get(tuningParameter.id);
        tuningParameter.minValue = tuningParameterConstraintList.get(index).lowerBound;
        tuningParameter.maxValue = tuningParameterConstraintList.get(index).upperBound;
      }
    }
  }

  @Override
  public int numberOfConstraintsViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    logger.info(" Constraint Violeted ");
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double mrReduceMemory = null;
    Double mrMapXMX = null;
    Double mrReduceXMX = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;

    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      if (jobSuggestedParamValue.tuningParameter.paramName.equals(ParameterKeys.SORT_BUFFER_HADOOP_CONF.getValue())) {
        mrSortMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
        mrMapMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
        mrReduceMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
        mrMapXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
        mrReduceXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          ParameterKeys.PIG_SPLIT_SIZE_HADOOP_CONF.getValue())) {
        pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
      }
    }

    if (mrSortMemory != null && mrMapMemory != null) {
      if (mrSortMemory > 0.6 * mrMapMemory) {
        logger.debug("Sort Memory " + mrSortMemory);
        logger.debug("Mapper Memory " + mrMapMemory);
        logger.debug("Constraint violated: Sort memory > 60% of map memory");
        violations++;
      }
      if (mrMapMemory - mrSortMemory < 768) {
        logger.debug("Sort Memory " + mrSortMemory);
        logger.debug("Mapper Memory " + mrMapMemory);
        logger.debug("Constraint violated: Map memory - sort memory < 768 mb");
        violations++;
      }
    }
    if (mrMapXMX != null && mrMapMemory != null && mrMapXMX > 0.80 * mrMapMemory) {
      logger.debug("Mapper Heap Max " + mrMapXMX);
      logger.debug("Mapper Memory " + mrMapMemory);
      logger.debug("Constraint violated:  Mapper  XMX > 0.8*mrMapMemory");
      violations++;
    }
    if (mrReduceMemory != null && mrReduceXMX != null && mrReduceXMX > 0.80 * mrReduceMemory) {
      logger.debug("Reducer Heap Max " + mrMapXMX);
      logger.debug("Reducer Memory " + mrMapMemory);
      logger.debug("Constraint violated:  Reducer  XMX > 0.8*mrReducerMemory");
      violations++;
    }

    if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
      logger.debug("Constraint violated: Pig max combined split size > 1.8 * map memory");
      violations++;
    }
    return violations;
  }

  @Override
  /*
  Since IPSO collects information from previous iterations , it better to minimize the independendent iterations(as compare to PSO).
  Swarm size of 2 makes two independendent iterations and next set of iterations will
  depend on the constraints values set by previous set. This will help parameters to converge faster.
   */

  public int getSwarmSize() {
    return 2;
  }
}

