/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.security;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;

import static common.TestConstants.*;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_KEY;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_VALUE;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;


public class HadoopSecurityTest {


  private static final Logger logger = LoggerFactory.getLogger(HadoopSecurityTest.class);
  private static FakeApplication fakeApp;
  private static FakeApplication fakeAppWithFakeKeytab;
  private static FakeApplication fakeAppWithKeytab;

  @Before
  public void setup() {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    properties.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    properties.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    properties.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        logger.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(properties, gs);

    properties.put("keytab.user", "username");
    properties.put("keytab.location", "fake-location.txt");
    fakeAppWithFakeKeytab = fakeApplication(properties, gs);

    properties.put("keytab.location", HadoopSecurityTest.class.getClassLoader().getResource("keytab/emptyKeytab").getFile());
    fakeAppWithKeytab = fakeApplication(properties, gs);

  }

  @Test
  public void testLoginWithoutSecurity() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        boolean success = true;

        new MockUp<UserGroupInformation>() {
          @Mock
          boolean isSecurityEnabled() {
            return false;
          }
        };

        new MockUp<HadoopSecurity>() {
          @Mock
          public void checkLogin(){
            return;
          }
        };

        try {
          HadoopSecurity security = new HadoopSecurity();
          security.login();
        } catch (IOException e) {
          success = false;
        }
        assertTrue("Cannot login. This cluster is security enabled.", success);
      }
    });
  }

  @Test
  public void testLoginWithoutKeytabCredentials() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        boolean success = false;

        new MockUp<UserGroupInformation>() {
          @Mock
          boolean isSecurityEnabled() {
            return true;
          }
        };

        new MockUp<HadoopSecurity>() {
          @Mock
          public void checkLogin(){
            return;
          }
        };

        try {
          HadoopSecurity security = new HadoopSecurity();
          security.login();
        } catch (IOException e) {
          success = true;
        }
        assertTrue("This test should throw an IOException due to missing keytab credentials", success);
      }
    });
  }

  @Test
  public void testLoginWithMissingKeytabFile() {
    running(testServer(TEST_SERVER_PORT, fakeAppWithFakeKeytab), new Runnable() {
      public void run() {
        boolean success = false;

        new MockUp<UserGroupInformation>() {
          @Mock
          boolean isSecurityEnabled() {
            return true;
          }
        };

        new MockUp<HadoopSecurity>() {
          @Mock
          public void checkLogin(){
            return;
          }
        };

        try {
          HadoopSecurity security = new HadoopSecurity();
          security.login();
        } catch (IOException e) {
          success = true;
        }
        assertTrue("This test should throw an IOException due to fake keytab location", success);
      }
    });
  }

  @Test
  public void testLoginWithKeytab() {
    running(testServer(TEST_SERVER_PORT, fakeAppWithKeytab), new Runnable() {
      public void run() {
        boolean success = true;

        new MockUp<UserGroupInformation>() {
          @Mock
          boolean isSecurityEnabled() {
            return true;
          }
        };

        new MockUp<HadoopSecurity>() {
          @Mock
          public void checkLogin(){
            return;
          }
        };

        try {
          HadoopSecurity security = new HadoopSecurity();
          security.login();
        } catch (IOException e) {
          success = false;
        }
        assertTrue("This test shouldn't throw an IOException", success);
      }
    });
  }
}
