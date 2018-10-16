package com.linkedin.drelephant.tuning;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.DrElephant;
import com.linkedin.drelephant.ElephantContext;
import java.util.HashMap;
import java.util.Map;

import static common.DBTestUtil.*;
import static common.TestConstants.*;

import models.TuningAlgorithm;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import controllers.*;
import play.test.FakeApplication;
import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.*;
import static play.test.Helpers.*;

import org.junit.Before;
import org.junit.Test;


public class TuningManagerTest {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TuningManagerTest.class);
  private static FakeApplication fakeApp;
  private int numParametersToTune;

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<String, String>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        LOGGER.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(dbConn, gs);
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Boolean autoTuningEnabled = configuration.getBoolean(DrElephant.AUTO_TUNING_ENABLED, false);
   // org.junit.Assume.assumeTrue(autoTuningEnabled);
  }

  @Test
  public void testIPSOManager() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new IPSOManagerTestRunner());

  }

  @Test
  public void testFlowTestRunner(){
    running(testServer(TEST_SERVER_PORT, fakeApp), new FlowTestRunner());
  }


  @Test
  public void testBaselineManagerTestRunner(){
    running(testServer(TEST_SERVER_PORT, fakeApp), new BaselineManagerTestRunner());
  }

  @Test
  public void testJobStatusManagerTestRunner(){
    running(testServer(TEST_SERVER_PORT, fakeApp), new JobStatusManagerTestRunner());
  }


  @Test
  public void testFitnessManagerTestRunner(){
    running(testServer(TEST_SERVER_PORT, fakeApp), new FitnessManagerTestRunner());
  }

  @Test
  public void testParamGenerterTestRunner(){
    running(testServer(TEST_SERVER_PORT, fakeApp), new ParameterGenerateManagerTestRunner());
  }

  @Test
  public void testAlgoBasedOnVersion(){
    assertTrue("Alorithm Based on Version Test", controllers.Application.getAlgoBasedOnVersion(1).equals(
        TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name()));
    assertTrue("Alorithm Based on Version Test", controllers.Application.getAlgoBasedOnVersion(2).equals(
        TuningAlgorithm.OptimizationAlgo.HBT.name()));
    assertTrue("Alorithm Based on Version Test", controllers.Application.getAlgoBasedOnVersion(3).equals(
        TuningAlgorithm.OptimizationAlgo.HBT.name()));
  }
}
