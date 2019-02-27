package com.linkedin.drelephant.spark.exception;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.exceptions.core.ExceptionFingerprintingRunner;
import models.AppResult;
import models.JobExecution;
import com.linkedin.drelephant.exceptions.util.Constant.*;
import static common.DBTestUtil.*;

import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class ExceptionFingerprintingRunnerTest implements Runnable {
  private HadoopApplicationData data ;
  private AnalyticJob _analyticJob;
  public ExceptionFingerprintingRunnerTest(HadoopApplicationData data,AnalyticJob analyticJob){
    this._analyticJob = analyticJob;
    this.data = data;
  }
  private void populateTestData() {
    try {
      initDBIPSO();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void run() {
    populateTestData();
    AppResult _appResult = AppResult.find.byId("application_1458194917883_1453361");
    ExceptionFingerprintingRunner runner =
        new ExceptionFingerprintingRunner(_analyticJob, _appResult, data, ExecutionEngineType.SPARK);
    runner.run();
    JobExecution jobExecution = JobExecution.find.where()
        .eq(JobExecution.TABLE.jobExecId,
            "https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0")
        .findUnique();
    assertTrue("Auto tuning fault " + jobExecution.autoTuningFault, jobExecution.autoTuningFault == true);
  }

}
