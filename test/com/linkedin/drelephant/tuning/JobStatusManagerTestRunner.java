package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.Schduler.AzkabanJobStatusManager;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import java.util.List;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;


public class JobStatusManagerTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initDBAzkabanJobStatus();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testJobStatusAzkaban();
  }

  private void testJobStatusAzkaban() {
    AbstractJobStatusManager jobStatusManager = new AzkabanJobStatusManager();
    List<TuningJobExecutionParamSet> tuningJobExecutionParamSets = jobStatusManager.detectJobsExecutionInProgress();
    /*List<TuningJobExecutionParamSet> tuningJobExecutionParamSets =
        TuningJobExecutionParamSet.find.select("*").fetch(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet, "*").findList();*/
    assertTrue(" Number of Jobs in progress " + tuningJobExecutionParamSets.size(),
        tuningJobExecutionParamSets.size() == 1);

    boolean calculateCompletedJobExDone = jobStatusManager.analyzeCompletedJobsExecution(tuningJobExecutionParamSets);

    assertTrue(" Analyize completed job execution ", !calculateCompletedJobExDone);

    boolean updateDatabase = jobStatusManager.updateDataBase(tuningJobExecutionParamSets);


    assertTrue(" Update database done  ", updateDatabase);

    JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*").findUnique();
    assertTrue(" Job Suggested param set " + jobSuggestedParamSet.paramSetState.name(),
        jobSuggestedParamSet.paramSetState.name().equals(JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED.name()));

    JobExecution jobExecution = JobExecution.find.select("*").findUnique();
    assertTrue(" Job Execution Status  " , jobExecution.executionState.name().equals(JobExecution.ExecutionState.IN_PROGRESS.name()));

    boolean updateMetrics = jobStatusManager.updateMetrics(tuningJobExecutionParamSets);

    assertTrue(" Update Metrics   " , updateMetrics);

  }
}
