package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.tuning.hbt.BaselineManagerHBT;
import com.linkedin.drelephant.tuning.obt.BaselineManagerOBT;
import java.util.List;

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
    Flow flow = new Flow();
    flow.createBaseLineManagersPipeline();
    List<List<Manager>> pipeline = flow.getPipeline();
    List<Manager> baseLineManagers = pipeline.get(0);
    testManagerCreations(baseLineManagers);
  }

  private void testManagerCreations(List<Manager> baseLineManagers){
    assertTrue("Base line Manager HBT ", baseLineManagers.get(0) instanceof BaselineManagerHBT);
    assertTrue("Base line Manager OBT ", baseLineManagers.get(1) instanceof BaselineManagerOBT);
    BaselineManagerHBT baselineManagerHBT = (BaselineManagerHBT)baseLineManagers.get(0);
    BaselineManagerOBT baselineManagerOBT = (BaselineManagerOBT)baseLineManagers.get(1);
  }
}
