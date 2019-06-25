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

package com.linkedin.drelephant.tuning.hbt;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tuning.TuningHelper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;
import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.UtilizedParameterKeys.*;
import static com.linkedin.drelephant.tuning.Constant.*;
import static java.lang.Math.*;


public class PigHbtParameterRecommender {
  private List<AppResult> appResultList;
  @Getter
  private Map<String, Double> jobSuggestedParameters = new HashMap<>();
  @Getter
  private Map<String, Double> latestAppliedParams = new HashMap<>();
  public Map<String, Double> essentialCounters = new HashMap<>();

  private final Logger logger = Logger.getLogger(getClass());

  public PigHbtParameterRecommender(List<AppResult> appResultList) {
    this.appResultList = appResultList;
  }

  /**
   * Method to extract all the important parameters applied in the latest execution
   * @throws IllegalArgumentException if the List of AppResult is null or have to AppResult
   */
  @VisibleForTesting
  void loadLatestAppliedParameters() {
    if (appResultList != null && appResultList.size() > 0) {
      AppResult appResult = appResultList.get(0);
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult != null && appHeuristicResult.heuristicName.equals(MAPREDUCE_CONFIGURATION)) {
          for (AppHeuristicResultDetails heuristicResultDetail : appHeuristicResult.yarnAppHeuristicResultDetails) {
            if (heuristicResultDetail.name.equals(MAPPER_MEMORY_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(MAPPER_MEMORY_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(REDUCER_MEMORY_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(REDUCER_MEMORY_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(SORT_BUFFER_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(SORT_BUFFER_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(SORT_FACTOR_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(SORT_FACTOR_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(PIG_MAX_SPLIT_SIZE_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(SORT_SPILL_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(SORT_SPILL_HADOOP_CONF.getValue(),
                  Double.parseDouble(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(MAPPER_HEAP_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(MAPPER_HEAP_HADOOP_CONF.getValue(), getHeapMemory(heuristicResultDetail.value));
            } else if (heuristicResultDetail.name.equals(REDUCER_HEAP_HEURISTICS_CONF.getValue())) {
              latestAppliedParams.put(REDUCER_HEAP_HADOOP_CONF.getValue(), getHeapMemory(heuristicResultDetail.value));
            }
          }
        }
      }
    } else {
      throw new IllegalArgumentException("No App Results found while TuneIn was suggesting new parameters");
    }
  }

  /**
   * This method is responsible for suggesting the parameters to fix failed heuristics
   * in the next execution or if there is no failed heuristics then try to optimize Resource usage
   * @return Map with suggested parameter's name as key and its value
   */
  public Map<String, Double> suggestParameters() {
    loadLatestAppliedParameters();
    List<String> failedHeuristicNameList = getFailedHeuristics();
    suggestToFixFailedHeuristics(failedHeuristicNameList);
    if (failedHeuristicNameList.size() == 0) {
      logger.info("No failed heuristics, so will try to optimize memory parameters");
      suggestMemoryParam(MRJobTaskType.MAP);
      suggestMemoryParam(MRJobTaskType.REDUCE);
    }
    addRequiredPreviousSuggestedParameters();
    modifyMapperMemory();
    return jobSuggestedParameters;
  }
  /**
   * Method to suggest parameters accordingly for each failed heuristic
   * @param failedHeuristicNameList list with names of heuristics which are failed
   */
  private void suggestToFixFailedHeuristics(List<String> failedHeuristicNameList) {
    for (String failedHeuristicName : failedHeuristicNameList) {
      switch (failedHeuristicName) {
        case MAPPER_MEMORY :
          suggestMemoryParam(MRJobTaskType.MAP);
          break;
        case REDUCER_MEMORY :
          suggestMemoryParam(MRJobTaskType.REDUCE);
          break;
        case MAPPER_SPILL :
          suggestParametersForMemorySpill();
          break;
        case MAPPER_TIME:
        case MAPPER_SPEED:
          suggestSplitSize();
      }
    }
  }

  /**
   * Method to add previous applied parameters values for Parameters
   * which are not suggested this time
   */
  private void addRequiredPreviousSuggestedParameters() {
    for (String key : latestAppliedParams.keySet()) {
      if (!jobSuggestedParameters.containsKey(key)) {
        jobSuggestedParameters.put(key, latestAppliedParams.get(key));
      }
    }
  }

  /**
   * Method to suggest Memory for container for provided @taskType
   * @param taskType Task type which can be either Map or Reduce
   */
  @VisibleForTesting
  void suggestMemoryParam(MRJobTaskType taskType) {
    String memoryHeuristicName;
    if (taskType.equals(MRJobTaskType.MAP)) {
      memoryHeuristicName = MAPPER_MEMORY_HEURISTICS_CONF.getValue();
    } else {
      memoryHeuristicName = REDUCER_MEMORY_HEURISTICS_CONF.getValue();
    }
    double maxUsedPhysicalMapperMemoryByJob =
        getHeuristicDetailsMaxValueForJob(memoryHeuristicName, MAX_PHYSICAL_MEMORY.getValue());
    double maxUsedHeapMemory =
        getHeuristicDetailsMaxValueForJob(memoryHeuristicName, MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
    double maxUsedVirtualMemory = getHeuristicDetailsMaxValueForJob(memoryHeuristicName, MAX_VIRTUAL_MEMORY.getValue());
    double recommendedMemory = max(maxUsedPhysicalMapperMemoryByJob, (maxUsedVirtualMemory / YARN_VMEM_TO_PMEM_RATIO));
    double containerSize = TuningHelper.getContainerSize(recommendedMemory);
    double recommendedHeapMemory = TuningHelper.getHeapSize(min(HEAP_MEMORY_TO_PHYSICAL_MEMORY_RATIO * containerSize,
        maxUsedHeapMemory));
    setSuggestedMemoryParameter(taskType, containerSize, recommendedHeapMemory);
  }

  /**
   * Method to set the memory parameter's value for given task type
   * @param taskType Task type which can be either Map or Reduce
   * @param suggestedContainerMemory container memory suggested for the task type for next execution
   * @param suggestedHeapMemory heap memory suggested for the task type for next execution
   */
  private void setSuggestedMemoryParameter(MRJobTaskType taskType, double suggestedContainerMemory,
      double suggestedHeapMemory) {
    if (taskType.equals(MRJobTaskType.MAP)) {
      setSuggestedMemoryParametersForMapper(suggestedContainerMemory, suggestedHeapMemory);
    } else if (taskType.equals(MRJobTaskType.REDUCE)) {
      setSuggestedMemoryParametersForReducer(suggestedContainerMemory, suggestedHeapMemory);
    }
  }

  /**
   * Method to set the memory parameter's value Map task
   * @param suggestedContainerMemory container memory suggested for the Map task next execution
   * @param suggestedHeapMemory heap memory suggested for the Map task next execution
   */
  private void setSuggestedMemoryParametersForMapper(double suggestedContainerMemory, double suggestedHeapMemory) {
    jobSuggestedParameters.put(MAPPER_MEMORY_HADOOP_CONF.getValue(), suggestedContainerMemory);
    jobSuggestedParameters.put(MAPPER_HEAP_HADOOP_CONF.getValue(), suggestedHeapMemory);
  }

  /**
   * Method to set the memory parameter's value Reduce task
   * @param suggestedContainerMemory container memory suggested for the Reduce task next execution
   * @param suggestedHeapMemory heap memory suggested for the Reduce task next execution
   */
  private void setSuggestedMemoryParametersForReducer(double suggestedContainerMemory, double suggestedHeapMemory) {
    jobSuggestedParameters.put(REDUCER_MEMORY_HADOOP_CONF.getValue(), suggestedContainerMemory);
    jobSuggestedParameters.put(REDUCER_HEAP_HADOOP_CONF.getValue(), suggestedHeapMemory);
  }

  /**
   * Method to find the maximum value for give Heuristic Detail among the Job
   * @param heuristicName heuristic name in which the detail will be present
   * @param heuristicDetailName heuristic detail name for which max  value is required
   * @return max value for heuristicDetailName
   */
  private Double getHeuristicDetailsMaxValueForJob(String heuristicName, String heuristicDetailName) {
    List<AppHeuristicResult> appHeuristicResultList = new ArrayList<>();
    for (AppResult appResult : appResultList) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult.heuristicName.equals(heuristicName)) {
          appHeuristicResultList.add(appHeuristicResult);
        }
      }
    }
    return getHeuristicDetailsMaxValue(appHeuristicResultList, heuristicDetailName);
  }

  /**
   * Method to find the maximum value for give Heuristic Detail among the all the applications for the Job
   * @param appHeuristicResultList List containing AppResult for all the application for a Job
   * @param heuristicDetailName heuristic detail name for which max  value is required
   * @return max value for heuristicDetailName
   */
  private Double getHeuristicDetailsMaxValue(List<AppHeuristicResult> appHeuristicResultList,
      String heuristicDetailName) {
    double maxHeuristicDetailValueForJob = 0D;
    if (appHeuristicResultList != null) {
      for (AppHeuristicResult appHeuristicResult : appHeuristicResultList) {
        for (AppHeuristicResultDetails appHeuristicResultDetail : appHeuristicResult.yarnAppHeuristicResultDetails) {
          if (appHeuristicResultDetail.name.equals(heuristicDetailName)) {
            double maxHeuristicDetailValueForApplication = Double.parseDouble(appHeuristicResultDetail.value);
            maxHeuristicDetailValueForJob =
                Math.max(maxHeuristicDetailValueForJob, maxHeuristicDetailValueForApplication);
          }
        }
      }
      return maxHeuristicDetailValueForJob;
    } else {
      throw new IllegalArgumentException("No heuristic analysis result found for any application");
    }
  }

  /**
   *  Method to suggest parameters for suggesting Spilt size which will be used to fix
   *  Mapper Speed and/or Mapper Time heuristic
   */
  @VisibleForTesting
  void suggestSplitSize() {
    long maxAvgInputSizeInBytes;
    int maxAvgRuntimeInSeconds;
    maxAvgInputSizeInBytes = getHeuristicDetailsMaxValueForJob(MAPPER_TIME, AVG_INPUT_SIZE_IN_BYTES).longValue();
    maxAvgRuntimeInSeconds = getMaxAvgTaskRuntime();
    if (maxAvgRuntimeInSeconds == 0) {
      logger.error("Maximum Avg Runtime was 0 so not suggesting split size");
      return;
    }
    Double mapperHeapMemory = getMapperHeapMemory();
    long mapperHeapMemoryInBytes = mapperHeapMemory.longValue() * FileUtils.ONE_MB;
    long suggestedSplitSizeInBytes =
        (maxAvgInputSizeInBytes * OPTIMAL_MAPPER_SPEED_BYTES_PER_SECOND * 60) / (maxAvgRuntimeInSeconds);
    suggestedSplitSizeInBytes =
        min(suggestedSplitSizeInBytes, mapperHeapMemoryInBytes);
    logger.debug("Split size suggested  " + suggestedSplitSizeInBytes);
    jobSuggestedParameters.put(PIG_SPLIT_SIZE_HADOOP_CONF.getValue(), suggestedSplitSizeInBytes * 1.0);
    jobSuggestedParameters.put(SPLIT_SIZE_HADOOP_CONF.getValue(), suggestedSplitSizeInBytes * 1.0);
  }

  /**
   *  Method to get Mapper's heap memory, if it is suggested then return the suggested value
   *  else return the last applied heap memory
   */
  private double getMapperHeapMemory() {
    return jobSuggestedParameters.containsKey(MAPPER_HEAP_HADOOP_CONF.getValue()) ? jobSuggestedParameters.get(
        MAPPER_HEAP_HADOOP_CONF.getValue()) : latestAppliedParams.get(MAPPER_HEAP_HADOOP_CONF.getValue());
  }

  /**
   *  Method to get maximum value for Avg Task Runtime
   */
  private int getMaxAvgTaskRuntime() {
    int maxAvgRuntimeInSeconds = 0;
    for (AppResult appResult : appResultList) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (appHeuristicResult.heuristicName.equals(MAPPER_TIME)) {
          for (AppHeuristicResultDetails appHeuristicResultDetail : appHeuristicResult.yarnAppHeuristicResultDetails) {
            if (appHeuristicResultDetail.name.equals(AVERAGE_TASK_RUNTIME.getValue())) {
              try {
                maxAvgRuntimeInSeconds =
                    Math.max(maxAvgRuntimeInSeconds, Statistics.getTimeInSeconds(appHeuristicResultDetail.value));
              } catch (NumberFormatException numberFormatException) {
                logger.error("Error getting runtime in seconds for duration " + appHeuristicResultDetail.value,
                    numberFormatException);
              }
            }
          }
        }
      }
    }
    return maxAvgRuntimeInSeconds;
  }

  /**
   * Method to suggest parameters for Mapper Spill heuristic
   */
  @VisibleForTesting
  void suggestParametersForMemorySpill() {
    int suggestedBufferSize;
    double suggestedSpillPercentage = 0.0f;
    double mapperSpillRatio = getHeuristicDetailsMaxValueForJob(MAPPER_SPILL, MAPPER_OUTPUT_RECORD_SPILL_RATIO);
    int previousAppliedSortBufferValue = latestAppliedParams.get(SORT_BUFFER_HADOOP_CONF.getValue()).intValue();
    double previousAppliedSpillPercentage = latestAppliedParams.get(SORT_SPILL_HADOOP_CONF.getValue());
    if (mapperSpillRatio > MAPPER_MEMORY_SPILL_THRESHOLD_2) {
      if (mapperSpillRatio > MAPPER_MEMORY_SPILL_THRESHOLD_1) {
        if (previousAppliedSpillPercentage <= SORT_SPILL_PERCENTAGE_THRESHOLD) {
          suggestedSpillPercentage = previousAppliedSpillPercentage + SPILL_PERCENTAGE_STEP_SIZE;
          suggestedBufferSize = (int) (previousAppliedSortBufferValue * 1.2);
        } else {
          suggestedBufferSize = (int) (previousAppliedSortBufferValue * 1.3);
        }
      } else {
        if (previousAppliedSpillPercentage <= SORT_SPILL_PERCENTAGE_THRESHOLD) {
          suggestedSpillPercentage = previousAppliedSpillPercentage + SPILL_PERCENTAGE_STEP_SIZE;
          suggestedBufferSize = (int) (previousAppliedSortBufferValue * 1.1);
        } else {
          suggestedBufferSize = (int) (previousAppliedSortBufferValue * 1.2);
        }
      }
      jobSuggestedParameters.put(SORT_BUFFER_HADOOP_CONF.getValue(), suggestedBufferSize * 1.0);
      if (suggestedSpillPercentage > 0) {
        jobSuggestedParameters.put(SORT_SPILL_HADOOP_CONF.getValue(), suggestedSpillPercentage);
      }
    }
  }

  /**
   * Method to suggest or modified suggested Mapper Memory in accordance to
   * currently suggested parameters (sort buffer, spill percentage) for Mapper Spill heuristic
   */
  private void modifyMapperMemory() {
    double currentMapperMemory =
        jobSuggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue()) == null ? latestAppliedParams.get(
            MAPPER_MEMORY_HADOOP_CONF.getValue()) : jobSuggestedParameters.get(MAPPER_MEMORY_HADOOP_CONF.getValue());
    Double sortBuffer = jobSuggestedParameters.get(SORT_BUFFER_HADOOP_CONF.getValue());
    Double minPhysicalMemoryRequired = max(sortBuffer + SORT_BUFFER_CUSHION, sortBuffer * MEMORY_TO_SORT_BUFFER_RATIO);
    if (minPhysicalMemoryRequired > currentMapperMemory) {
      logger.info("Modifying memory to adjust according to Sort Buffer size");
      currentMapperMemory = minPhysicalMemoryRequired;
      jobSuggestedParameters.put(MAPPER_MEMORY_HADOOP_CONF.getValue(),
          TuningHelper.getContainerSize(currentMapperMemory));
      Double heapMemory = jobSuggestedParameters.get(MAPPER_HEAP_HADOOP_CONF.getValue());
      heapMemory = (heapMemory == null) ? (HEAP_MEMORY_TO_PHYSICAL_MEMORY_RATIO * currentMapperMemory)
          : min(HEAP_MEMORY_TO_PHYSICAL_MEMORY_RATIO * currentMapperMemory, heapMemory);
      jobSuggestedParameters.put(MAPPER_HEAP_HADOOP_CONF.getValue(), heapMemory);
    }
  }

  /**
   * Method to get all the heuristics failed for any MR application in Job
   * @return List of Heuristic names which failed
   */
  @VisibleForTesting
  List<String> getFailedHeuristics() {
    List<String> failedHeuristicNameList = new ArrayList<>();
    for (AppResult appResult : appResultList) {
      for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
        if (HeuristicsToTuneByPigHbt.isMember(appHeuristicResult.heuristicName) && !failedHeuristicNameList.contains(
            appHeuristicResult.heuristicName)
            && appHeuristicResult.severity.getValue() > Severity.MODERATE.getValue()) {
          logger.info("Failed heuristic " + appHeuristicResult.heuristicName);
          failedHeuristicNameList.add(appHeuristicResult.heuristicName);
        }
      }
    }
    return failedHeuristicNameList;
  }

  /**
   * Method to extract the heap memory from the max allowed JVM heap memory conf value
   * @param mrHeapMemoryConfigValue max allowed heap Memory for JVM e.g. -Xmx200m
   * @return Extracted heap size i.e. 200 for above example
   */
  double getHeapMemory(String mrHeapMemoryConfigValue) {
    Pattern pattern = Pattern.compile(JVM_MAX_HEAP_MEMORY_REGEX);
    Matcher matcher = pattern.matcher(mrHeapMemoryConfigValue);
    double maxHeapSize;
    if (matcher.find()) {
      int memoryValue = Integer.parseInt(matcher.group(1));
      maxHeapSize = matcher.group(2).toLowerCase().equals("g") ? memoryValue * MB_IN_ONE_GB : memoryValue;
    } else {
      logger.warn("Couldn't find JVM max heap pattern in config value " + mrHeapMemoryConfigValue);
      maxHeapSize = DEFAULT_CONTAINER_HEAP_MEMORY;
    }
    return maxHeapSize;
  }
}
