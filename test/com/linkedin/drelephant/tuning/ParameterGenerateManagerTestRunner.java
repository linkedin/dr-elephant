package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.FitnessManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import java.util.List;
import models.JobSuggestedParamSet;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;
import static common.DBTestUtil.*;


public class ParameterGenerateManagerTestRunner implements Runnable {

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
    testParamGeneraterHBT();
  }

  public void testParamGeneraterHBT() {
    AbstractParameterGenerateManager parameterGenerateManager =
        new ParameterGenerateManagerHBT(new MRExecutionEngine());
    List<JobTuningInfo> jobTuningInfos = parameterGenerateManager.detectJobsForParameterGeneration();
    assertTrue(" Job Needed for Param Generation " + jobTuningInfos.size(), jobTuningInfos.size() == 1);
    assertTrue(" Job State " + jobTuningInfos.get(0).getTunerState().replaceAll("\\{\\}", ""),
        jobTuningInfos.get(0).getTunerState().replaceAll("\\{\\}", "").length() == 0);

    List<JobTuningInfo> updatedJobTuningInfoList = parameterGenerateManager.generateParameters(jobTuningInfos);
    assertTrue(" Job State After param generation " + updatedJobTuningInfoList.get(0).getTunerState(),
        updatedJobTuningInfoList.get(0).getTunerState().replaceAll("\\{\\}", "").length() > 0);

    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.paramSetState,JobSuggestedParamSet.ParamSetStatus.CREATED)
        .findList();

    assertTrue(" Parameter Created  "+jobSuggestedParamSets.size(),jobSuggestedParamSets.size()==0);

    boolean isDataBaseUpdated = parameterGenerateManager.updateDatabase(updatedJobTuningInfoList);

    assertTrue(" Updated database with param generation "+isDataBaseUpdated,isDataBaseUpdated);

    List<JobSuggestedParamSet> jobSuggestedParamSets1 = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.paramSetState,JobSuggestedParamSet.ParamSetStatus.CREATED)
        .findList();


    assertTrue(" Parameter Created  "+jobSuggestedParamSets1.size(),jobSuggestedParamSets1.size()==1);


  }
}
