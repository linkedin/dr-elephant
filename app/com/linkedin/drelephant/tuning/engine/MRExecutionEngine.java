package com.linkedin.drelephant.tuning.engine;

import com.avaje.ebean.Expr;
import com.avaje.ebean.ExpressionList;
import com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic;
import com.linkedin.drelephant.tuning.ExecutionEngine;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningParameter;
import models.TuningParameterConstraint;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import static java.lang.Math.*;


public class MRExecutionEngine implements ExecutionEngine {
  private final Logger logger = Logger.getLogger(getClass());

  enum UsageCounterSchema {USED_PHYSICAL_MEMORY, USED_VIRTUAL_MEMORY, USED_HEAP_MEMORY}

  private String functionTypes[] = {"map", "reduce"};

  @Override
  public void computeValuesOfDerivedConfigurationParameters(List<TuningParameter> derivedParameterList,
      List<JobSuggestedParamValue> jobSuggestedParamValues) {
    Map<String, Double> jobSuggestedParamValueMap = new HashMap<String, Double>();
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValues) {
      jobSuggestedParamValueMap.put(jobSuggestedParamValue.tuningParameter.paramName,
          jobSuggestedParamValue.paramValue);
    }

    for (TuningParameter derivedParameter : derivedParameterList) {
      logger.info("Computing value of derived param: " + derivedParameter.paramName);
      Double paramValue = null;
      if (derivedParameter.paramName.equals("mapreduce.reduce.java.opts")) {
        String parentParamName = "mapreduce.reduce.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.map.java.opts")) {
        String parentParamName = "mapreduce.map.memory.mb";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = 0.75 * jobSuggestedParamValueMap.get(parentParamName);
        }
      } else if (derivedParameter.paramName.equals("mapreduce.input.fileinputformat.split.maxsize")) {
        String parentParamName = "pig.maxCombinedSplitSize";
        if (jobSuggestedParamValueMap.containsKey(parentParamName)) {
          paramValue = jobSuggestedParamValueMap.get(parentParamName);
        }
      }

      if (paramValue != null) {
        JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
        jobSuggestedParamValue.paramValue = paramValue;
        jobSuggestedParamValue.tuningParameter = derivedParameter;
        jobSuggestedParamValues.add(jobSuggestedParamValue);
      }
    }
  }

  /**
   * Check if the parameters violated constraints
   * Constraint 1: sort.mb > 60% of map.memory: To avoid heap memory failure
   * Constraint 2: map.memory - sort.mb < 768: To avoid heap memory failure
   * Constraint 3: pig.maxCombinedSplitSize > 1.8*mapreduce.map.memory.mb
   * @param jobSuggestedParamValueList List of suggested param values
   * @return true if the constraint is violated, false otherwise
   */

  @Override
  public Boolean isParamConstraintViolatedIPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    logger.info(" Constraint Violeted ");
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double mrReduceMemory = null;
    Double mrMapXMX = null;
    Double mrReduceXMX = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;

    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.SORT_BUFFER_HADOOP_CONF.getValue())) {
        mrSortMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
        mrMapMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
        mrReduceMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
        mrMapXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
        mrReduceXMX = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.PIG_SPLIT_SIZE_HADOOP_CONF.getValue())) {
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
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }

  public Boolean isParamConstraintViolatedPSO(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    Double mrSortMemory = null;
    Double mrMapMemory = null;
    Double pigMaxCombinedSplitSize = null;
    Integer violations = 0;
    for (JobSuggestedParamValue jobSuggestedParamValue : jobSuggestedParamValueList) {
      if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.task.io.sort.mb")) {
        mrSortMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("mapreduce.map.memory.mb")) {
        mrMapMemory = jobSuggestedParamValue.paramValue;
      } else if (jobSuggestedParamValue.tuningParameter.paramName.equals("pig.maxCombinedSplitSize")) {
        pigMaxCombinedSplitSize = jobSuggestedParamValue.paramValue / FileUtils.ONE_MB;
      }
    }
    if (mrSortMemory != null && mrMapMemory != null) {
      if (mrSortMemory > 0.6 * mrMapMemory) {
        logger.info("Constraint violated: Sort memory > 60% of map memory");
        violations++;
      }
      if (mrMapMemory - mrSortMemory < 768) {
        logger.info("Constraint violated: Map memory - sort memory < 768 mb");
        violations++;
      }
    }

    if (pigMaxCombinedSplitSize != null && mrMapMemory != null && (pigMaxCombinedSplitSize > 1.8 * mrMapMemory)) {
      logger.info("Constraint violated: Pig max combined split size > 1.8 * map memory");
      violations++;
    }
    if (violations == 0) {
      return false;
    } else {
      logger.info("Number of constraint(s) violated: " + violations);
      return true;
    }
  }

  @Override
  public ExpressionList<JobSuggestedParamSet> getPendingJobs() {
    return JobSuggestedParamSet.find.select("*")
        .fetch(JobSuggestedParamSet.TABLE.jobDefinition, "*")
        .where()
        .or(Expr.or(Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.CREATED),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.SENT)),
            Expr.eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.EXECUTED))
        .eq(JobSuggestedParamSet.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.PIG.name())
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 0);
  }

  @Override
  public ExpressionList<TuningJobDefinition> getTuningJobDefinitionsForParameterSuggestion() {
    return TuningJobDefinition.find.select("*")
        .fetch(TuningJobDefinition.TABLE.job, "*")
        .where()
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1)
        .eq(TuningJobDefinition.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.jobType,
            TuningAlgorithm.JobType.PIG.name());
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
        for (CommonConstantsHeuristic.UtilizedParameterKeys value : CommonConstantsHeuristic.UtilizedParameterKeys.values()) {
          if (appHeuristicResultDetails.name.equals(value.getValue())) {
            counterData.put(value.getValue(), appHeuristicResultDetails.value == null ? 0
                : ((double) MemoryFormatUtils.stringToBytes(appHeuristicResultDetails.value)));
          }
        }
      }
    }
  }

  private List<Double> extractUsageParameter(String functionType, Map<String, Map<String, Double>> usageDataGlobal) {
    Double usedPhysicalMemoryMB = 0.0, usedVirtualMemoryMB = 0.0, usedHeapMemoryMB = 0.0;
    usedPhysicalMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue());
    usedVirtualMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_VIRTUAL_MEMORY.getValue());
    usedHeapMemoryMB = usageDataGlobal.get(functionType)
        .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_TOTAL_COMMITTED_HEAP_USAGE_MEMORY.getValue());
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

  private Map<String, TuningParameterConstraint> filterMemoryConstraint(
      List<TuningParameterConstraint> parameterConstraints, String functionType) {
    Map<String, TuningParameterConstraint> memoryConstraints = new HashMap<String, TuningParameterConstraint>();
    for (TuningParameterConstraint parameterConstraint : parameterConstraints) {
      if (functionType.equals("map")) {
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
      if (functionType.equals("reduce")) {
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_MEMORY", parameterConstraint);
        }
        if (parameterConstraint.tuningParameter.paramName.equals(
            CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue())) {
          memoryConstraints.put("CONTAINER_HEAP", parameterConstraint);
        }
      }
    }
    return memoryConstraints;
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

  @Override
  public void parameterOptimizerIPSO(List<AppResult> results, JobExecution jobExecution) {
    Map<String, Map<String, Double>> previousUsedMetrics = extractParameterInformationIPSO(results);
    List<TuningParameterConstraint> parameterConstraints = TuningParameterConstraint.find.where().
        eq("job_definition_id", jobExecution.job.id).findList();
    for (String function : functionTypes) {
      logger.debug(" Optimizing Parameter Space  " + function);
      if (previousUsedMetrics.get(function)
          .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue()) > 0.0) {
        List<Double> usageStats = extractUsageParameter(function, previousUsedMetrics);
        Map<String, TuningParameterConstraint> memoryConstraints =
            filterMemoryConstraint(parameterConstraints, function);
        memoryParameterIPSO(function, memoryConstraints, usageStats);
      }
    }
  }

  @Override
  public String parameterGenerationsHBT(List<AppResult> results, List<TuningParameter> tuningParameters) {
    Map<String, Map<String, Double>> previousUsedMetrics = extractParameterInformationIPSO(results);
    StringBuffer idParameters = new StringBuffer();
    for (TuningParameter tuningParameter : tuningParameters) {
      if (tuningParameter.paramName.equals(
          CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue())) {
        idParameters.append(tuningParameter.id)
            .append("\t")
            .append(previousUsedMetrics.get("map")
                .get(CommonConstantsHeuristic.UtilizedParameterKeys.MAX_PHYSICAL_MEMORY.getValue()));
        idParameters.append("\n");
        break;
      }
    }
    logger.info(" New Suggested Parameter " + idParameters);
    return idParameters.toString();
  }

  @Override
  public Boolean isParamConstraintViolatedHBT(List<JobSuggestedParamValue> jobSuggestedParamValueList) {
    return false;
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

  private Map<String, Map<String, Double>> extractParameterInformationIPSO(List<AppResult> appResults) {
    logger.info(" Extract Parameter Information for MR IPSO");
    Map<String, Map<String, Double>> usageDataGlobal = new HashMap<String, Map<String, Double>>();
    intialize(usageDataGlobal);
    for (AppResult appResult : appResults) {
      Map<String, Map<String, Double>> usageDataApplicationlocal = collectUsageDataPerApplicationIPSO(appResult);
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

  private void intialize(Map<String, Map<String, Double>> usageDataGlobal) {
    for (String function : functionTypes) {
      Map<String, Double> usageData = new HashMap<String, Double>();
      for (CommonConstantsHeuristic.UtilizedParameterKeys value : CommonConstantsHeuristic.UtilizedParameterKeys.values()) {
        usageData.put(value.getValue(), 0.0);
      }
      usageDataGlobal.put(function, usageData);
    }
  }

  private Map<String, Map<String, Double>> collectUsageDataPerApplicationIPSO(AppResult appResult) {
    Map<String, Map<String, Double>> usageData = null;
    usageData = new HashMap<String, Map<String, Double>>();
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
}


