package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.MRApplicationData;
import com.linkedin.drelephant.tuning.hbt.MRJob;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.TuningParameter;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;



public class ParameterGenerateManagerTestRunner implements Runnable {
  private Map<String, Double> appliedParameter = null;
  private List<MRApplicationData> mrApplicationDatas = null;
  private Map<String, Double> suggestedParameter = null;

  private void populateTestData() {
    try {
      initMRHBT();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    try {
      testGenerateParamSet();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    testMemoryAndNumberOfTaskRecommendations();
    testNumberOfReducerTaskAndMapperSpillRecommendations();
    testNoHeuristicFailRecommendations();
    testParseMaxHeapSizeInMB();
    testTimeInMinutes();
  }

  private void testGenerateParamSet() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ParameterGenerateManagerHBT parameterGenerateManagerHBT = new ParameterGenerateManagerHBT(new MRExecutionEngine());
    List<TuningParameter> tuningParameters = null;
    JobDefinition job = JobDefinition.find.select("*")
                        .where()
                        .eq(JobDefinition.TABLE.id, 100003)
                        .setMaxRows(1)
                        .findUnique();

    Method generateParamSet = ParameterGenerateManagerHBT.class.getDeclaredMethod("generateParamSet", List.class, JobDefinition.class);
    generateParamSet.setAccessible(true);
    String output = (String) generateParamSet.invoke(parameterGenerateManagerHBT, tuningParameters, job);

    assertTrue("Generated Param sets are " + output, output.equals(""));

  }

  private void testMemoryAndNumberOfTaskRecommendations() {
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://sample:8443/executor?execid=5416293")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://sample:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0")
        .findList();
    processData(results);
    assertTrue(" Number of MapReduce Application ", results.size() == 2);
    assertTrue(" Mapper Memory " +appliedParameter.get("Mapper Memory"), appliedParameter.get("Mapper Memory").equals(2048.0));
    assertTrue(" Reducer Memory "+appliedParameter.get("Reducer Memory"), appliedParameter.get("Reducer Memory").equals(2048.0));
    assertTrue(" Sort Buffer "+appliedParameter.get("Sort Buffer").intValue(), appliedParameter.get("Sort Buffer").intValue()==100);
    assertTrue(" Sort Spill "+appliedParameter.get("Sort Spill"), appliedParameter.get("Sort Spill")==0.80);
    assertTrue(" Split Size "+appliedParameter.get("Split Size").intValue(), appliedParameter.get("Split Size").intValue()==536870912);

    testApplicationRecommendedMemoryParameter(mrApplicationDatas);
    testApplicationRecommendedNumberoFTasks(mrApplicationDatas);
    testJobRecommendedMemoryParameter(suggestedParameter);
  }

  private void testNumberOfReducerTaskAndMapperSpillRecommendations() {
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://sample:8443/executor?execid=1200332")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://sample:8443/executor?execid=1200332&job=untitled&attempt=0")
        .findList();
    assertTrue(" Number of MapReduce Application " + results.size(), results.size() == 1);
    processData(results);
    testApplicationRecommendedReducerTaskMapperSpill(mrApplicationDatas);
    testJobRecommendedReducerTaskAndMapperSpill(suggestedParameter);
  }

  private void testNoHeuristicFailRecommendations() {
    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, "https://sample:8443/executor?execid=1342802")
        .eq(AppResult.TABLE.JOB_EXEC_ID,
            "https://sample:8443/executor?execid=1342802&job=fetl-dupe_fetlDupeLog&attempt=0")
        .findList();
    assertTrue(" Number of MapReduce Application " + results.size(), results.size() == 1);
    processData(results);
    testApplicationRecommendedForRU(mrApplicationDatas);
    testJobRecommendedForRU(suggestedParameter);
  }

  private void processData(List<AppResult> results) {
    MRJob mrJob = new MRJob(results);
    appliedParameter = mrJob.getAppliedParameter();
    mrJob.analyzeAllApplications();
    mrApplicationDatas = mrJob.getApplicationAnalyzedData();
    mrJob.processJobForParameter();
    suggestedParameter = mrJob.getJobSuggestedParameter();
  }

  private void testApplicationRecommendedReducerTaskMapperSpill(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1540411174627_3924329"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      /*assertTrue("Reducer Number of tasks) " + usedParameter.get("Reducer Number of tasks"),
          usedParameter.get("Reducer Number of tasks") == 51);*/
      /*assertTrue(
          "Reducer Average task runtime " + Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0,
          Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0 == 176.83);*/
     // assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 102);

      assertTrue("Ratio of spilled records to output records " + usedParameter.get(
          "Ratio of spilled records to output records"),
          Math.round(usedParameter.get("Ratio of spilled records to output records") * 100) / 100.0 == 3.13);
      assertTrue("Sort Buffer " + usedParameter.get("Sort Buffer"), usedParameter.get("Sort Buffer") == 100);
      assertTrue("Sort Spill " + usedParameter.get("Sort Spill"),
          Math.round(usedParameter.get("Sort Spill") * 100) / 100.0 == 0.80);
      assertTrue("Suggested Sort Buffer " + suggestedParameter.get("mapreduce.task.io.sort.mb"),
          suggestedParameter.get("mapreduce.task.io.sort.mb") == 120);
      assertTrue("Suggested Sort Spill " + suggestedParameter.get("mapreduce.map.sort.spill.percent"),
          Math.round(suggestedParameter.get("mapreduce.map.sort.spill.percent") * 100) / 100.0 == 0.85);
    }
  }

  private void testJobRecommendedReducerTaskAndMapperSpill(Map<String, Double> suggestedParameter) {
    assertTrue(" Total  Parameter Suggested "+suggestedParameter.keySet().size(), suggestedParameter.keySet().size() == 8);
    //assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 102);
    assertTrue(" Suggested Sort Buffer  ", suggestedParameter.get("mapreduce.task.io.sort.mb") == 120);
    assertTrue(" Suggested Sort Spill ",
        Math.round(suggestedParameter.get("mapreduce.map.sort.spill.percent") * 100) / 100.0 == 0.85);
  }

  private void testApplicationRecommendedMemoryParameter(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")
          || mrApplicationData.getApplicationID().equals("application_1458194917883_1453362"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")) {
        assertTrue("Mapper Max Virtual Memory (MB) ", usedParameter.get("Mapper Max Virtual Memory (MB)") == 1426.0);
        assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 595.0);
        assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 427.0);
        assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 1024.0);
        assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 1350.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453362")) {
        assertTrue("Mapper Max Virtual Memory (MB) ", usedParameter.get("Mapper Max Virtual Memory (MB)") == 2200.0);
        assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 595.0);
        assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
        assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);

        assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 2100.0);
        assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 497.0);
        assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
            usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 300.0);
        assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
            suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
        assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
      }
    }
  }

  private void testJobRecommendedMemoryParameter(Map<String, Double> suggestedParameter) {
    assertTrue(" Total  Parameter Suggested "+suggestedParameter.keySet().size(), suggestedParameter.keySet().size() == 8);
    assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
    assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 600.0);
    assertTrue(" Reducer Memory Suggested ", suggestedParameter.get("mapreduce.reduce.memory.mb") == 1024.0);
    assertTrue(" Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 600.0);
    assertTrue(" Split Size Recommneded " +suggestedParameter.get("mapreduce.input.fileinputformat.split.maxsize"),
        suggestedParameter.get("mapreduce.input.fileinputformat.split.maxsize") == 536870912);
    assertTrue(" Split Size Recommneded " +suggestedParameter.get("pig.maxCombinedSplitSize"),
        suggestedParameter.get("pig.maxCombinedSplitSize") == 536870912);
   // assertTrue(" Number of Reducer ", suggestedParameter.get("mapreduce.job.reduces") == 370);
  }

  private void testApplicationRecommendedNumberoFTasks(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")
          || mrApplicationData.getApplicationID().equals("application_1458194917883_1453362"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453361")) {
        assertTrue("Mapper Average task input size) " + usedParameter.get("Mapper Average task input size"),
            usedParameter.get("Mapper Average task input size") == 80740352);
        assertTrue(
            "Mapper Average task runtime " + Math.round(usedParameter.get("Mapper Average task runtime") * 100) / 100.0,
            Math.round(usedParameter.get("Mapper Average task runtime") * 100) / 100.0 == 0.47);
        assertTrue(" Split Size Recommneded ", suggestedParameter.get("mapreduce.input.fileinputformat.split.maxsize") == 161480704);
      }
      if (mrApplicationData.getApplicationID().equals("application_1458194917883_1453362")) {
        /*assertTrue("Reducer Number of tasks) " + usedParameter.get("Reducer Number of tasks"),
            usedParameter.get("Reducer Number of tasks") == 741.0);*/
       /* assertTrue("Reducer Average task runtime "
                + Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0,
            Math.round(usedParameter.get("Reducer Average task runtime") * 100) / 100.0 == 0.12);*/
       // assertTrue(" Number of Reducer recommended ", suggestedParameter.get("mapreduce.job.reduces") == 370);
      }
    }
  }

  private void testApplicationRecommendedForRU(List<MRApplicationData> mrApplicationDatas) {
    for (MRApplicationData mrApplicationData : mrApplicationDatas) {
      assertTrue(" Application IDs ", mrApplicationData.getApplicationID().equals("application_1540411174627_1086799"));
      Map<String, Double> suggestedParameter = mrApplicationData.getSuggestedParameter();
      Map<String, Double> usedParameter = mrApplicationData.getCounterValues();
      assertTrue("Mapper Max Virtual Memory (MB) " + usedParameter.get("Mapper Max Virtual Memory (MB)"),
          usedParameter.get("Mapper Max Virtual Memory (MB)") == 2330);
      assertTrue("Mapper Max Physical Memory (MB)", usedParameter.get("Mapper Max Physical Memory (MB)") == 325);
      assertTrue("Mapper Max Total Committed Heap Usage Memory (MB)",
          usedParameter.get("Mapper Max Total Committed Heap Usage Memory (MB)") == 619);
      assertTrue(" Mapper Memory Recommended ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
      assertTrue(" Mapper Memory Heap Recommended " + suggestedParameter.get("mapreduce.map.java.opts"),
          suggestedParameter.get("mapreduce.map.java.opts") == 619);

      assertTrue("Reducer Max Virtual Memory (MB)", usedParameter.get("Reducer Max Virtual Memory (MB)") == 2335);
      assertTrue("Reducer Max Physical Memory (MB)", usedParameter.get("Reducer Max Physical Memory (MB)") == 425);
      assertTrue("Reducer Max Total Committed Heap Usage Memory (MB)",
          usedParameter.get("Reducer Max Total Committed Heap Usage Memory (MB)") == 619);
      assertTrue(" Reducer Memory Recommended " + suggestedParameter.get("mapreduce.reduce.memory.mb"),
          suggestedParameter.get("mapreduce.reduce.memory.mb") == 2048.0);
      assertTrue("Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 619);
    }
  }
  private void testJobRecommendedForRU(Map<String, Double> suggestedParameter) {

    assertTrue(" Total  Parameter Suggested "+suggestedParameter.keySet().size(), suggestedParameter.keySet().size() == 8);
    assertTrue(" Mapper Memory Suggested ", suggestedParameter.get("mapreduce.map.memory.mb") == 2048.0);
    assertTrue(" Mapper Memory Heap Recommended ", suggestedParameter.get("mapreduce.map.java.opts") == 619.0);
    assertTrue(" Reducer Memory Suggested ", suggestedParameter.get("mapreduce.reduce.memory.mb") == 2048.0);
    assertTrue(" Reducer Memory Heap Recommended ", suggestedParameter.get("mapreduce.reduce.java.opts") == 619);

  }

  private void testParseMaxHeapSizeInMB(){
    assertTrue(" Heap Size ", TuningHelper.parseMaxHeapSizeInMB(
        "-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize="
            + "256m -XX:ParallelGCThreads=5 -Xms512m -Djava.net.preferIPv4Stack=true -Xmx1536g") == 1536 * 1024.0);
    assertTrue(" Heap Size ", TuningHelper.parseMaxHeapSizeInMB(
        "-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize="
            + "256m -XX:ParallelGCThreads=5 -Xms512m -Xmx1536m -Djava.net.preferIPv4Stack=true") == 1536);

    assertTrue(" Heap Size ", TuningHelper.parseMaxHeapSizeInMB(
        "-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize="
            + "256m -XX:ParallelGCThreads=5 -Xms512m -Djava.net.preferIPv4Stack=true") == 1536);

    assertTrue(" Heap Size ", TuningHelper.parseMaxHeapSizeInMB(
        "-Xmx700m") == 700);

  }

  private void testTimeInMinutes(){
    assertTrue(" time in minutes", TuningHelper.getTimeInMinute("1 hr 1 min") == 61);
    assertTrue(" time in minutes ", TuningHelper.getTimeInMinute("2 hr ") == 120);
    assertTrue(" time in minutes "+TuningHelper.getTimeInMinute("1 hr 1 min 30 sec "), TuningHelper.getTimeInMinute("1 hr 1 min 30 sec ") == 61.5);
    assertTrue(" time in minutes "+TuningHelper.getTimeInMinute("1 hr 30 sec "), TuningHelper.getTimeInMinute("1 hr 30 sec ") == 60.5);
  }
}