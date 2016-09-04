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

package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.AnalyticJobGeneratorHadoop2;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.DelayQueue;
import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ElephantRunnerTest {

  @Test
  public void testRetryOrDrop() {
    final AnalyticJob analyticJob = new AnalyticJob();
    ElephantRunner elephantRunner = new ElephantRunner();

    new MockUp<AnalyticJobGeneratorHadoop2>() {
      @Mock
      public void addIntoRetries(AnalyticJob analyticJob) {
        return;
      }
    };

    Deencapsulation.invoke(elephantRunner, "retryOrDrop", analyticJob);
    assertEquals(1, analyticJob.getRetries());
  }

  @Test
  public void testFetchApplicationsEmptyList() {
    new MockUp<HadoopSystemContext>() {
      @Mock
      public boolean isHadoop2Env() {
        return true;
      }
    };

    new MockUp<ElephantRunner>() {
      @Mock
      private void loadGeneralConfiguration() {
        return;
      }
    };

    new MockUp<ElephantContext>() {
      @Mock
      public void $init() {
        return;
      }
    };

    new MockUp<AnalyticJobGeneratorHadoop2>() {
      @Mock
      public List<AnalyticJob> fetchCompletedAnalyticJobs(long from, long to){
        return new ArrayList<AnalyticJob>();
      }

      @Mock
      public List<AnalyticJob> fetchUndefinedAnalyticJobs(long from, long to){
        return new ArrayList<AnalyticJob>();
      }
    };

    ElephantRunner elephantRunner = new ElephantRunner();
    elephantRunner.run();
    Deencapsulation.invoke(elephantRunner, "fetchApplications", (long) 0, (long) 1);

    assertEquals(0, elephantRunner.getCompletedThreadPoolQueueSize());
    assertEquals(0, elephantRunner.getUndefinedThreadPoolQueueSize());
  }

  @Test
  @Ignore
  public void testFetchApplicationsNonEmptyList() {
    new MockUp<AnalyticJobGeneratorHadoop2>() {
      @Mock
      public List<AnalyticJob> fetchCompletedAnalyticJobs(long from, long to) {
        List<AnalyticJob> analyticJobList = new ArrayList<AnalyticJob>();
        analyticJobList.add(new AnalyticJob().setAppId("application_1"));
        return analyticJobList;
      }

      @Mock
      public List<AnalyticJob> fetchUndefinedAnalyticJobs(long from, long to) {
        List<AnalyticJob> analyticJobList = new ArrayList<AnalyticJob>();
        analyticJobList.add(new AnalyticJob().setAppId("application_2"));
        return analyticJobList;
      }
    };

    new MockUp<HadoopSystemContext>() {
      @Mock
      public boolean isHadoop2Env() {
        return true;
      }
    };

    new MockUp<ElephantRunner>() {
      @Mock
      private void loadGeneralConfiguration() {
        return;
      }
    };

    new MockUp<ElephantContext>() {
      @Mock
      public void $init() {
        return;
      }
    };

    ElephantRunner elephantRunner = new ElephantRunner();
    Deencapsulation.invoke(elephantRunner, "fetchApplications", (long) 0, (long) 1);

    assertEquals(1, elephantRunner.getUndefinedThreadPoolQueueSize());
    assertEquals(1, elephantRunner.getCompletedThreadPoolQueueSize());

    elephantRunner.run();

    // Zero, because the UndefinedExecutorThread will pick the job from the UndefinedJobQueue
    assertEquals(0, elephantRunner.getUndefinedThreadPoolQueueSize());

    // One, because the Completed job is available only after the time has expired
    assertEquals(1, elephantRunner.getCompletedThreadPoolQueueSize());
  }

  @Test
  public void testKill() {
    new MockUp<HadoopSystemContext>() {
      @Mock
      public boolean isHadoop2Env() {
        return true;
      }
    };

    new MockUp<ElephantRunner>() {
      @Mock
      private void loadGeneralConfiguration() {
        return;
      }
    };

    new MockUp<ElephantContext>() {
      @Mock
      public void $init() {
        return;
      }
    };

    boolean success = false;
    ElephantRunner elephantRunner = new ElephantRunner();
    elephantRunner.run();
    elephantRunner.kill();
    success = elephantRunner.isKilled();

    assertTrue("Dr. Elephant has not terminated.", success);
  }

  @Test
  public void testGetLongFromConf() {
    Configuration conf = new Configuration();
    ElephantRunner elephantRunner = new ElephantRunner();
    conf.set("test", "1000");
    assertEquals(1000l, Deencapsulation.invoke(elephantRunner, "getLongFromConf", conf, "test", 2000l));
    assertEquals(2000l, Deencapsulation.invoke(elephantRunner, "getLongFromConf", conf, "no-property", 2000l));
  }
}
