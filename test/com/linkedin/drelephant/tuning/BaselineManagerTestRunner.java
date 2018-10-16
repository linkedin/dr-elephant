package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.engine.MRExecutionEngine;
import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.hbt.ParameterGenerateManagerHBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import com.linkedin.drelephant.tuning.obt.ParameterGenerateManagerOBTAlgoPSOIPSOImpl;
import java.util.List;
import models.TuningJobDefinition;

import static common.DBTestUtil.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class BaselineManagerTestRunner implements Runnable{

  private void populateTestData() {
    try {
      initDBBaseline();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  @Override
  public void run() {
    populateTestData();
    testBaselineManagerOBT();
    testBaselineManagerHBT();
  }

  private void testBaselineManagerOBT(){
    AbstractBaselineManager abstractBaselineManager =  new BaselineManagerOBT();
    List<TuningJobDefinition> tuningJobDefinitions = abstractBaselineManager.detectJobsForBaseLineComputation();
    assertTrue(" Base line detection jobs for OBT "+tuningJobDefinitions.size(), tuningJobDefinitions.size()==1);
    assertTrue (" Calculating Base line ",abstractBaselineManager.calculateBaseLine(tuningJobDefinitions));
    for(TuningJobDefinition tuningJobDefinition : tuningJobDefinitions){
      assertTrue(" Average Resource usage is not null "+tuningJobDefinition.averageResourceUsage!=null);
    }
    abstractBaselineManager.updateDataBase(tuningJobDefinitions);
    tuningJobDefinitions = abstractBaselineManager.detectJobsForBaseLineComputation();
    assertTrue ("Base line Done . No jobs for Baseline  ",(tuningJobDefinitions ==null ||tuningJobDefinitions.size()==0));
  }

  private void testBaselineManagerHBT(){
    AbstractBaselineManager abstractBaselineManager =  new BaselineManagerHBT();
    List<TuningJobDefinition> tuningJobDefinitions = abstractBaselineManager.detectJobsForBaseLineComputation();
    assertTrue(" Base line detection jobs for HBT "+tuningJobDefinitions.size(), tuningJobDefinitions.size()==1);
    assertTrue (" Calculating Base line ",abstractBaselineManager.calculateBaseLine(tuningJobDefinitions));
    for(TuningJobDefinition tuningJobDefinition : tuningJobDefinitions){
      assertTrue(" Average Resource usage is not null "+tuningJobDefinition.averageResourceUsage!=null);
    }
    abstractBaselineManager.updateDataBase(tuningJobDefinitions);
    tuningJobDefinitions = abstractBaselineManager.detectJobsForBaseLineComputation();
    assertTrue ("Base line Done . No jobs for Baseline  ",(tuningJobDefinitions ==null ||tuningJobDefinitions.size()==0));
  }
}
