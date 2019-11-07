package com.linkedin.drelephant.tuning.hbt;

import java.util.List;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;


public class FitnessManagerHBTTest implements Runnable {

  private void populateTestData() {
    try {
      initParamGenerater();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testCalculateAndUpdateFitness();
  }

  private void testCalculateAndUpdateFitness() {
    FitnessManagerHBT fitnessManagerHBT = new FitnessManagerHBT();

    JobExecution jobExecution = JobExecution.find.select("*")
        .where()
        .eq(JobExecution.TABLE.id,"1541")
        .findUnique();

    List<AppResult> appResults = AppResult.find.select("*")
        .where()
        .eq(JobExecution.TABLE.jobExecId, "https://elephant.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0")
        .findList();

    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, "100003")
        .findUnique();

    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.id, "1137")
        .findUnique();
    jobSuggestedParamSet.fitness = 123D;
    jobSuggestedParamSet.update();

    TuningJobExecutionParamSet tuningJobExecutionParamSet = TuningJobExecutionParamSet.find.select("*")
        .where()
        .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.id, "1541")
        .findUnique();
    tuningJobExecutionParamSet.isRetried = true;
    tuningJobExecutionParamSet.update();

    fitnessManagerHBT.calculateAndUpdateFitness(jobExecution, appResults, tuningJobDefinition,
        jobSuggestedParamSet, true);

    assertTrue("Fitness : " + jobSuggestedParamSet.fitness, jobSuggestedParamSet.fitness!=10000);
    assertTrue("InputSizeInBytes : " + jobExecution.autoTuningFault, !jobExecution.autoTuningFault);

    tuningJobExecutionParamSet = TuningJobExecutionParamSet.find.select("*")
        .where()
        .eq(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.id, "1542")
        .findUnique();
    tuningJobExecutionParamSet.isRetried = true;
    tuningJobExecutionParamSet.update();

    fitnessManagerHBT.calculateAndUpdateFitness(jobExecution, appResults, tuningJobDefinition,
        jobSuggestedParamSet, true);

    assertTrue("Fitness : " + jobSuggestedParamSet.fitness, jobSuggestedParamSet.fitness==10000);
    assertTrue("ExecutionTime : " + jobExecution.executionTime, jobExecution.executionTime==0D);
    assertTrue("InputSizeInBytes : " + jobExecution.inputSizeInBytes, jobExecution.inputSizeInBytes==1);
    assertTrue("InputSizeInBytes : " + jobExecution.autoTuningFault, jobExecution.autoTuningFault);

  }

}
