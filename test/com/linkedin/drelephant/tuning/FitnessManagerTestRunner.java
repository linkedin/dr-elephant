package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;


public class FitnessManagerTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initFitnessComputation();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testFitnessManagerHBT();
  }

  public void testFitnessManagerHBT() {
    AbstractFitnessManager fitnessManager = new FitnessManagerHBT();
    List<TuningJobExecutionParamSet> jobsForFitnessComputation = fitnessManager.detectJobsForFitnessComputation();
    assertTrue(" Jobs for fitness computation HBT " + jobsForFitnessComputation.size(),
        jobsForFitnessComputation.size() == 1);


    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.id, 1137)
        .findUnique();

    assertTrue("Job Suggested Param Set Before " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.EXECUTED.name()));

    assertTrue("Fitness Calculated " ,fitnessManager.calculateFitness(jobsForFitnessComputation));

    JobSuggestedParamSet jobSuggestedParamSet1 = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.id, 1137)
        .findUnique();


    assertTrue("Job Suggested Param Set After " + jobSuggestedParamSet1.paramSetState.name(),
        jobSuggestedParamSet1.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED.name()));


    testTuningDisabled(jobsForFitnessComputation,fitnessManager);
    testFitnessDelay(jobsForFitnessComputation,fitnessManager);

  }

  private void testTuningDisabled(List<TuningJobExecutionParamSet> jobsForFitnessComputation,AbstractFitnessManager fitnessManager ){
    JobDefinition jobDefinition = jobsForFitnessComputation.get(0).jobSuggestedParamSet.jobDefinition;
    assertTrue(" Number of executions reach to threshold " ,!fitnessManager.reachToNumberOfThresholdIterations(jobsForFitnessComputation,jobDefinition));
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*").where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, 100003)
        .eq(TuningJobDefinition.TABLE.tuningEnabled, 1).findUnique();

    tuningJobDefinition.numberOfIterations=1;
    tuningJobDefinition.save();
    assertTrue(" Number of executions reach to threshold " ,fitnessManager.reachToNumberOfThresholdIterations(jobsForFitnessComputation,jobDefinition));
    assertTrue(" Tuning Enabled ", tuningJobDefinition.tuningEnabled);
    fitnessManager.disableTuning(jobDefinition, "User Specified Iterations reached");
    TuningJobDefinition tuningJobDefinition1 = TuningJobDefinition.find.select("*").where()
        .eq(TuningJobDefinition.TABLE.job + "." + JobDefinition.TABLE.id, 100003).findUnique();
    assertTrue(" Tuning Disabled reason ", tuningJobDefinition1.tuningDisabledReason.equals("User Specified Iterations reached"));
    assertTrue(" Tuning Disabled reason "+tuningJobDefinition1.tuningEnabled, !tuningJobDefinition1.tuningEnabled);
  }

  private void testFitnessDelay(List<TuningJobExecutionParamSet> jobsForFitnessComputation,AbstractFitnessManager fitnessManager){
    TuningJobExecutionParamSet tuningJobExecutionParamSet = jobsForFitnessComputation.get(0);
    tuningJobExecutionParamSet.jobExecution.updatedTs.setTime(System.currentTimeMillis());
    List<TuningJobExecutionParamSet> completedJobExecutionParamSet = new ArrayList<TuningJobExecutionParamSet>();
    fitnessManager.getCompletedExecution(jobsForFitnessComputation, completedJobExecutionParamSet);
    assertTrue("Current Time in millis: " + System.currentTimeMillis() + ", Job execution last updated time "
        + tuningJobExecutionParamSet.jobExecution.updatedTs.getTime() + " FitnessComputeInterval "
        + fitnessManager.fitnessComputeWaitInterval, completedJobExecutionParamSet.size() == 0);
  }

}
