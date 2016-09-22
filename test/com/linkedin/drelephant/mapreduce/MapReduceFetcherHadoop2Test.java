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

package com.linkedin.drelephant.mapreduce;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MapReduceFetcherHadoop2Test {

  @Test
  public void testDiagnosticMatcher() {
    Assert.assertEquals("Task[\\s\\u00A0]+(.*)[\\s\\u00A0]+failed[\\s\\u00A0]+([0-9])[\\s\\u00A0]+times[\\s\\u00A0]+",
        ThreadContextMR2.getDiagnosticMatcher("Task task_1443068695259_9143_m_000475 failed 1 time")
            .pattern()
            .toString());

    Assert.assertEquals(2,
        ThreadContextMR2.getDiagnosticMatcher("Task task_1443068695259_9143_m_000475 failed 1 time").groupCount());
  }

  @Test
  public void testFetchData() {
    new MockUp<MapReduceFetcherHadoop2>() {
      @Mock
      private MapReduceApplicationData fetchCompletedJobsData(AnalyticJob analyticJob)
          throws IOException, AuthenticationException {
        return new MapReduceApplicationData().setAppId("application_1234").setStatus("COMPLETED");
      }

      @Mock
      private MapReduceApplicationData fetchRunningJobsData(AnalyticJob analyticJob, Job job)
          throws IOException, AuthenticationException, InterruptedException {
        return new MapReduceApplicationData().setAppId("application_1234").setStatus("RUNNING");
      }

      @Mock
      private void initCluster() {
        return;
      }
    };

    try {
      ElephantFetcher fetcher = new MapReduceFetcherHadoop2(null);
      AnalyticJob analyticJob = new AnalyticJob();

      analyticJob.setAppId("application_1234").setJobStatus("SUCCEEDED");
      HadoopApplicationData mrAppData = fetcher.fetchData(analyticJob, null);
      assertEquals("COMPLETED", mrAppData.getStatus());
      assertEquals("application_1234", mrAppData.getAppId());

      analyticJob.setJobStatus("FAILED");
      mrAppData = fetcher.fetchData(analyticJob, null);
      assertEquals("COMPLETED", mrAppData.getStatus());

      analyticJob.setJobStatus("RUNNING");
      mrAppData = fetcher.fetchData(analyticJob, null);
      assertEquals("RUNNING", mrAppData.getStatus());

      try {
        analyticJob.setJobStatus("UNKNOWN");
        fetcher.fetchData(analyticJob, null);
        assertTrue(false);
      } catch (RuntimeException e) {
        assertTrue(true);
      }
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void testFetchCompletedJobsDataInSucceededState() {

    new MockUp<MapReduceFetcherHadoop2.URLFactory>() {
      @Mock
      private void verifyURL(String url)
          throws IOException {
        return;
      }

      @Mock
      private URL getJobConfigURL(String jobId)
          throws MalformedURLException {
        return new URL("http://sample");
      }

      @Mock
      private URL getJobURL(String jobId)
          throws MalformedURLException {
        return new URL("http://sample");
      }

      @Mock
      private URL getJobCounterURL(String jobId)
          throws MalformedURLException {
        return new URL("http://sample");
      }

      @Mock
      private URL getTaskListURL(String jobId)
          throws MalformedURLException {
        return new URL("http://sample");
      }
    };

    new MockUp<MapReduceFetcherHadoop2.JSONFactory>() {
      @Mock
      private Properties getProperties(URL url)
          throws IOException, AuthenticationException {
        Properties jobConf = new Properties();
        jobConf.put("key", "value");
        return jobConf;
      }

      @Mock
      private String getState(URL url)
          throws IOException, AuthenticationException {
        return "SUCCEEDED";
      }

      @Mock
      private long getSubmitTime(URL url)
          throws IOException, AuthenticationException {
        return 1000;
      }

      @Mock
      private long getStartTime(URL url)
          throws IOException, AuthenticationException {
        return 1010;
      }

      @Mock
      private long getFinishTime(URL url)
          throws IOException, AuthenticationException {
        return 2000;
      }

      @Mock
      private MapReduceCounterData getJobCounter(URL url)
          throws IOException, AuthenticationException {
        return new MapReduceCounterData();
      }

      @Mock
      private void getTaskDataAll(URL url, String jobId, List<MapReduceTaskData> mapperList,
          List<MapReduceTaskData> reducerList)
          throws IOException, AuthenticationException {
        return;
      }
    };

    new MockUp<ThreadContextMR2>() {
      @Mock
      public void updateAuthToken() {
        return;
      }
    };

    AnalyticJob analyticJob = new AnalyticJob();
    analyticJob.setAppId("application_1234").setJobStatus("SUCCEEDED");
    try {
      ElephantFetcher fetcher = new MapReduceFetcherHadoop2(null);
      MapReduceApplicationData mrAppData = Deencapsulation.invoke(fetcher, "fetchCompletedJobsData", analyticJob);
      assertEquals("job_1234", mrAppData.getJobId());
      assertEquals("SUCCEEDED", mrAppData.getStatus());
      assertEquals(2000, mrAppData.getFinishTime());
      assertEquals(1010, mrAppData.getStartTime());
      assertEquals(1000, mrAppData.getSubmitTime());
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  @Test
  public void testGetMRTaskData() {
    final TaskReport taskReport1 =
        new TaskReport(new TaskID(new JobID("job_1234", 0), TaskType.MAP, 0), 0, null, null, TIPStatus.FAILED, 0, 0,
            null);
    final TaskReport taskReport2 =
        new TaskReport(new TaskID(new JobID("job_1234", 0), TaskType.MAP, 0), 0, null, null, TIPStatus.COMPLETE, 10000,
            20000, null);

    new MockUp<MapReduceFetcherHadoop2>() {
      @Mock
      private void initCluster() {
        return;
      }
    };

    new MockUp<TaskAttemptID>() {
      @Mock
      public String toString() {
        return "task_1234_1";
      }
    };

    try {
      ElephantFetcher fetcher = new MapReduceFetcherHadoop2(null);

      List<MapReduceTaskData> taskList = Deencapsulation.invoke(fetcher, "getMRTaskData", Arrays.asList(), 1);
      assertEquals(0, taskList.size());

      taskList = Deencapsulation.invoke(fetcher, "getMRTaskData", Arrays.asList(taskReport1), 1);
      assertEquals(0, taskList.size());

      taskList = Deencapsulation.invoke(fetcher, "getMRTaskData", Arrays.asList(taskReport2), 0);
      assertEquals(0, taskList.size());

      taskList = Deencapsulation.invoke(fetcher, "getMRTaskData", Arrays.asList(taskReport2), 1);
      assertEquals(1, taskList.size());
      assertEquals(10000, taskList.get(0).getStartTimeMs());
      assertEquals(20000, taskList.get(0).getFinishTimeMs());
      assertEquals("task_1234_1", taskList.get(0).getAttemptId());
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  @Test
  public void testFetchRunningJobsData() {

    new MockUp<Job>() {
      @Mock
      public TaskReport[] getTaskReports(final TaskType type)
          throws IOException, InterruptedException {
        TaskReport[] taskReportList = new TaskReport[1];
        return taskReportList;
      }

      @Mock
      public Counters getCounters()
          throws IOException {
        return new Counters();
      }

      @Mock
      public long getStartTime() {
        return 1000;
      }
    };

    new MockUp<MapReduceFetcherHadoop2.URLFactory>() {
      @Mock
      private void verifyURL(String url)
          throws IOException {
        return;
      }
    };

    new MockUp<MapReduceFetcherHadoop2.JSONFactory>() {
      @Mock
      private Properties getProperties(URL url)
          throws IOException, AuthenticationException {
        return new Properties();
      }
    };

    new MockUp<MapReduceFetcherHadoop2>() {
      @Mock
      private int getSampled(List<TaskReport> taskReportList) {
        return 0;
      }
    };

    AnalyticJob analyticJob = new AnalyticJob();
    analyticJob.setAppId("application_1234").setJobStatus("RUNNING");
    try {
      ElephantFetcher fetcher = new MapReduceFetcherHadoop2(null);
      MapReduceApplicationData mrAppData =
          Deencapsulation.invoke(fetcher, "fetchRunningJobsData", analyticJob, Job.class);
      assertNull(mrAppData);

      mrAppData = Deencapsulation.invoke(fetcher, "fetchRunningJobsData", analyticJob, new Job());
      assertNotNull(mrAppData);
      assertEquals("job_1234", mrAppData.getJobId());
      assertEquals("RUNNING", mrAppData.getStatus());
      assertEquals(1000, mrAppData.getSubmitTime());
      assertNotNull(mrAppData.getCounters());
    } catch (IOException e) {
      assertTrue(false);
    }
  }
}
