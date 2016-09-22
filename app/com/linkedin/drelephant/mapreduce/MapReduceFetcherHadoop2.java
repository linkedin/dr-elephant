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
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.AppResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class implements the Fetcher for MapReduce Applications on Hadoop2
 */
public class MapReduceFetcherHadoop2 implements ElephantFetcher<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  private static final int MAX_SAMPLE_SIZE = 200;
  private static final String SAMPLING_ENABLED = "sampling_enabled";

  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final String RUNNING = "RUNNING";

  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private String _jhistoryWebAddr;
  private FetcherConfigurationData _fetcherConfigurationData;
  private Configuration _configuration;

  public MapReduceFetcherHadoop2(FetcherConfigurationData fetcherConfData)
      throws IOException {
    this._fetcherConfigurationData = fetcherConfData;
    this._configuration = new Configuration();
    this._jsonFactory = new JSONFactory();
    initCluster();
  }

  private void initCluster()
      throws IOException {
    final String jhistoryAddr = _configuration.get("mapreduce.jobhistory.webapp.address");
    _jhistoryWebAddr = "http://" + jhistoryAddr + "/jobhistory/job/";

    logger.info("Connecting to the job history server at " + jhistoryAddr + "...");
    _urlFactory = new URLFactory(jhistoryAddr);
    logger.info("Connection success.");
  }

  @Override
  public MapReduceApplicationData fetchData(AnalyticJob analyticJob, Job job)
      throws IOException, AuthenticationException, InterruptedException {
    String appId = analyticJob.getAppId();
    String jobId = Utils.getJobIdFromApplicationId(appId);
    analyticJob.setTrackingUrl(_jhistoryWebAddr + jobId);

    MapReduceApplicationData jobData;
    if (analyticJob.getJobStatus().equals(SUCCEEDED) || analyticJob.getJobStatus().equals(FAILED)) {
      jobData = fetchCompletedJobsData(analyticJob);
    } else if (analyticJob.getJobStatus().equals(RUNNING)) {
      jobData = fetchRunningJobsData(analyticJob, job);
    } else {
      // Dr. Elephant doesn't support states other than SUCCEEDED, FAILED and RUNNING.
      // Remove the entry from database
      AppResult jobInDb = AppResult.find.byId(analyticJob.getAppId());
      try {
        if (jobInDb != null) {
          jobInDb.delete();
        }
      } catch (Exception e) {
        logger.error("Unable to delete " + analyticJob + " from database.");
      }
      throw new RuntimeException(appId + " is in " + analyticJob.getJobStatus() + " state which is not supported.");
    }

    return jobData;
  }

  private MapReduceApplicationData fetchCompletedJobsData(AnalyticJob analyticJob)
      throws IOException, AuthenticationException {
    MapReduceApplicationData jobData = new MapReduceApplicationData();
    String appId = analyticJob.getAppId();
    String jobId = Utils.getJobIdFromApplicationId(appId);
    jobData.setAppId(appId).setJobId(jobId).setStatus(analyticJob.getJobStatus());

    try {
      // Fetch job config
      Properties jobConf = _jsonFactory.getProperties(_urlFactory.getJobConfigURL(jobId));
      jobData.setJobConf(jobConf);

      URL jobURL = _urlFactory.getJobURL(jobId);
      String state = _jsonFactory.getState(jobURL);

      jobData.setSubmitTime(_jsonFactory.getSubmitTime(jobURL));
      jobData.setStartTime(_jsonFactory.getStartTime(jobURL));
      jobData.setFinishTime(_jsonFactory.getFinishTime(jobURL));

      if (state.equals(SUCCEEDED)) {
        // Fetch job counter
        MapReduceCounterData jobCounter = _jsonFactory.getJobCounter(_urlFactory.getJobCounterURL(jobId));

        // Fetch task data
        URL taskListURL = _urlFactory.getTaskListURL(jobId);
        List<MapReduceTaskData> mapperList = new ArrayList<MapReduceTaskData>();
        List<MapReduceTaskData> reducerList = new ArrayList<MapReduceTaskData>();
        _jsonFactory.getTaskDataAll(taskListURL, jobId, mapperList, reducerList);

        MapReduceTaskData[] mapperData = mapperList.toArray(new MapReduceTaskData[mapperList.size()]);
        MapReduceTaskData[] reducerData = reducerList.toArray(new MapReduceTaskData[reducerList.size()]);

        jobData.setCounters(jobCounter).setMapperData(mapperData).setReducerData(reducerData);
      } else if (state.equals(FAILED)) {
        String diagnosticInfo;
        try {
          diagnosticInfo = parseException(jobData.getJobId(), _jsonFactory.getDiagnosticInfo(jobURL));
        } catch (Exception e) {
          diagnosticInfo = null;
        }
        jobData.setDiagnosticInfo(diagnosticInfo);
      }
    } finally {
      ThreadContextMR2.updateAuthToken();
    }

    return jobData;
  }

  /**
   * Fetch information for running jobs. When job is in Running state,
   * check for all the completed tasks and compute the metrics.
   *
   * @param analyticJob the job being analyzed
   * @return
   * @throws IOException
   * @throws AuthenticationException
   * @throws InterruptedException
   */
  private MapReduceApplicationData fetchRunningJobsData(AnalyticJob analyticJob, Job job)
      throws IOException, AuthenticationException, InterruptedException {
    if (job == null) {
      return null;
    }

    MapReduceApplicationData jobData = new MapReduceApplicationData();
    String appId = analyticJob.getAppId();
    String jobId = Utils.getJobIdFromApplicationId(appId);
    String status = analyticJob.getJobStatus();
    jobData.setAppId(appId).setJobId(jobId).setStatus(status);

    List<TaskReport> mapTaskReportList = Arrays.asList(job.getTaskReports(TaskType.MAP));
    List<TaskReport> redTaskReportList = Arrays.asList(job.getTaskReports(TaskType.REDUCE));

    int mapSampleSize = getSampled(mapTaskReportList);
    int redSampleSize = getSampled(redTaskReportList);

    // Fetch task data
    List<MapReduceTaskData> mapperList = getMRTaskData(mapTaskReportList, mapSampleSize);
    List<MapReduceTaskData> reducerList = getMRTaskData(redTaskReportList, redSampleSize);

    // Capture the job configuration
    Properties props = new Properties();
    if (job.getConfiguration() != null) {
      Iterator<Map.Entry<String, String>> entries = job.getConfiguration().iterator();
      while (entries.hasNext()) {
        Map.Entry<String, String> entry = entries.next();
        String key = entry.getKey();
        String value = entry.getValue();
        props.setProperty(key, value);
      }
    }
    jobData.setJobConf(props);

    // Capture the job counters
    MapReduceCounterData mpCounter = new MapReduceCounterData();
    if (job.getCounters() != null) {
      Iterator<CounterGroup> counterGroups = job.getCounters().iterator();
      while (counterGroups.hasNext()) {
        CounterGroup counterGroup = counterGroups.next();
        Iterator<Counter> counters = counterGroup.iterator();
        while (counters.hasNext()) {
          Counter counter = counters.next();
          mpCounter.set(counterGroup.getName(), counter.getName(), counter.getValue());
        }
      }
    }
    jobData.setCounters(mpCounter);

    MapReduceTaskData[] mapperData = mapperList.toArray(new MapReduceTaskData[mapperList.size()]);
    MapReduceTaskData[] reducerData = reducerList.toArray(new MapReduceTaskData[reducerList.size()]);
    jobData.setMapperData(mapperData).setReducerData(reducerData);

    jobData.setSubmitTime(job.getStartTime());
    jobData.setStartTime(job.getStartTime());
    jobData.setFinishTime(System.currentTimeMillis());

    return jobData;
  }

  private List<MapReduceTaskData> getMRTaskData(List<TaskReport> taskReports, int sampleSize) {
    List<MapReduceTaskData> taskList = new ArrayList<MapReduceTaskData>();
    if (sampleSize > taskReports.size()) {
      return taskList;
    }

    for (int i = 0; i < sampleSize; i++) {
      TaskReport taskReport = taskReports.get(i);
      if (taskReport.getCurrentStatus() == TIPStatus.COMPLETE) {
        MapReduceTaskData mrTaskData =
            new MapReduceTaskData(taskReport.getTaskId(), taskReport.getSuccessfulTaskAttemptId().toString());
        mrTaskData.setTime(new long[]{0, 0, 0, taskReport.getStartTime(), taskReport.getFinishTime()});

        Counters taskCounters = taskReport.getTaskCounters();
        MapReduceCounterData taskCounter = new MapReduceCounterData();
        if (taskCounters != null) {
          Iterator<CounterGroup> counterGroups = taskCounters.iterator();
          while (counterGroups.hasNext()) {
            CounterGroup counterGroup = counterGroups.next();
            Iterator<Counter> counters = counterGroup.iterator();
            while (counters.hasNext()) {
              Counter counter = counters.next();
              taskCounter.set(counterGroup.getName(), counter.getName(), counter.getValue());
            }
          }
        }
        mrTaskData.setCounter(taskCounter);

        long taskExecTime = mrTaskData.getTotalRunTimeMs();
        mrTaskData.setTime(
            new long[]{taskExecTime, mrTaskData.getShuffleTimeMs(), mrTaskData.getSortTimeMs(), mrTaskData.getStartTimeMs(), mrTaskData.getFinishTimeMs()});
        taskList.add(mrTaskData);
      } else {
        // Ignore Map/Reduce tasks which are still running
      }
    }
    return taskList;
  }

  private int getSampled(List<TaskReport> taskReportList) {
    int sampleSize = taskReportList.size();

    // check if sampling is enabled
    if (Boolean.parseBoolean(_fetcherConfigurationData.getParamMap().get(SAMPLING_ENABLED))) {
      if (sampleSize > MAX_SAMPLE_SIZE) {
        Collections.shuffle(taskReportList);
      }
      sampleSize = Math.min(sampleSize, MAX_SAMPLE_SIZE);
    }
    return sampleSize;
  }

  private String parseException(String jobId, String diagnosticInfo)
      throws MalformedURLException, IOException, AuthenticationException {
    Matcher m = ThreadContextMR2.getDiagnosticMatcher(diagnosticInfo);
    if (m.matches()) {
      if (Integer.parseInt(m.group(2)) == 0) {
        // This is due to bug in hadoop 2.3 and shoufixed in 2.4
        throw new RuntimeException("Error in diagnosticInfo");
      }
      String taskId = m.group(1);
      System.out.println("parse succedded. " + m.group(1) + " " + m.group(2));
      return _jsonFactory.getTaskFailedStackTrace(_urlFactory.getTaskAllAttemptsURL(jobId, taskId));
    }
    logger.info("Does not match regex!!");
    // Diagnostic info not present in the job. Usually due to exception during AM setup
    throw new RuntimeException("No sufficient diagnostic Info");
  }

  private URL getTaskCounterURL(String jobId, String taskId)
      throws MalformedURLException {
    return _urlFactory.getTaskCounterURL(jobId, taskId);
  }

  private URL getTaskAttemptURL(String jobId, String taskId, String attemptId)
      throws MalformedURLException {
    return _urlFactory.getTaskAttemptURL(jobId, taskId, attemptId);
  }

  public class URLFactory {

    private String _restRoot;

    public URLFactory(String hserverAddr)
        throws IOException {
      _restRoot = "http://" + hserverAddr + "/ws/v1/history/mapreduce/jobs";
      verifyURL(_restRoot);
    }

    private void verifyURL(String url)
        throws IOException {
      final URLConnection connection = new URL(url).openConnection();
      // Check service availability
      connection.connect();
    }

    private URL getJobURL(String jobId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId);
    }

    private URL getJobConfigURL(String jobId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/conf");
    }

    private URL getJobCounterURL(String jobId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/counters");
    }

    private URL getTaskListURL(String jobId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks");
    }

    private URL getTaskCounterURL(String jobId, String taskId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/counters");
    }

    private URL getTaskAllAttemptsURL(String jobId, String taskId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts");
    }

    private URL getTaskAttemptURL(String jobId, String taskId, String attemptId)
        throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts/" + attemptId);
    }
  }

  public class JSONFactory {

    private long getStartTime(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("job").path("startTime").getValueAsLong();
    }

    private long getFinishTime(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("job").path("finishTime").getValueAsLong();
    }

    private long getSubmitTime(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("job").path("submitTime").getValueAsLong();
    }

    private String getState(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("job").path("state").getValueAsText();
    }

    private String getDiagnosticInfo(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      String diag = rootNode.path("job").path("diagnostics").getValueAsText();
      return diag;
    }

    private Properties getProperties(URL url)
        throws IOException, AuthenticationException {
      Properties jobConf = new Properties();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode configs = rootNode.path("conf").path("property");

      for (JsonNode conf : configs) {
        String key = conf.get("name").getValueAsText();
        String val = conf.get("value").getValueAsText();
        jobConf.setProperty(key, val);
      }
      return jobConf;
    }

    private MapReduceCounterData getJobCounter(URL url)
        throws IOException, AuthenticationException {
      MapReduceCounterData holder = new MapReduceCounterData();

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobCounters").path("counterGroup");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String counterName = counter.get("name").getValueAsText();
          Long counterValue = counter.get("totalCounterValue").getLongValue();
          String groupName = group.get("counterGroupName").getValueAsText();
          holder.set(groupName, counterName, counterValue);
        }
      }
      return holder;
    }

    private MapReduceCounterData getTaskCounter(URL url)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("jobTaskCounters").path("taskCounterGroup");
      MapReduceCounterData holder = new MapReduceCounterData();

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counter")) {
          String name = counter.get("name").getValueAsText();
          String groupName = group.get("counterGroupName").getValueAsText();
          Long value = counter.get("value").getLongValue();
          holder.set(groupName, name, value);
        }
      }
      return holder;
    }

    private long[] getTaskExecTime(URL url)
        throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode taskAttempt = rootNode.path("taskAttempt");

      long startTime = taskAttempt.get("startTime").getLongValue();
      long finishTime = taskAttempt.get("finishTime").getLongValue();
      boolean isMapper = taskAttempt.get("type").getValueAsText().equals("MAP");

      long[] time;
      if (isMapper) {
        // No shuffle sore time in Mapper
        time = new long[]{finishTime - startTime, 0, 0, startTime, finishTime};
      } else {
        long shuffleTime = taskAttempt.get("elapsedShuffleTime").getLongValue();
        long sortTime = taskAttempt.get("elapsedMergeTime").getLongValue();
        time = new long[]{finishTime - startTime, shuffleTime, sortTime, startTime, finishTime};
      }

      return time;
    }

    private void getTaskDataAll(URL url, String jobId, List<MapReduceTaskData> mapperList,
        List<MapReduceTaskData> reducerList)
        throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode tasks = rootNode.path("tasks").path("task");

      for (JsonNode task : tasks) {
        String state = task.get("state").getValueAsText();
        if (!state.equals("SUCCEEDED")) {
          // This is a failed task.
          continue;
        }
        String taskId = task.get("id").getValueAsText();
        String attemptId = task.get("successfulAttempt").getValueAsText();
        boolean isMapper = task.get("type").getValueAsText().equals("MAP");

        if (isMapper) {
          mapperList.add(new MapReduceTaskData(taskId, attemptId));
        } else {
          reducerList.add(new MapReduceTaskData(taskId, attemptId));
        }
      }

      getTaskData(jobId, mapperList);
      getTaskData(jobId, reducerList);
    }

    private void getTaskData(String jobId, List<MapReduceTaskData> taskList)
        throws IOException, AuthenticationException {

      int sampleSize = taskList.size();

      // check if sampling is enabled
      if (Boolean.parseBoolean(_fetcherConfigurationData.getParamMap().get(SAMPLING_ENABLED))) {
        if (taskList.size() > MAX_SAMPLE_SIZE) {
          logger.info(jobId + " needs sampling.");
          Collections.shuffle(taskList);
        }
        sampleSize = Math.min(taskList.size(), MAX_SAMPLE_SIZE);
      }

      for (int i = 0; i < sampleSize; i++) {
        MapReduceTaskData data = taskList.get(i);

        URL taskCounterURL = getTaskCounterURL(jobId, data.getTaskId());
        MapReduceCounterData taskCounter = getTaskCounter(taskCounterURL);

        URL taskAttemptURL = getTaskAttemptURL(jobId, data.getTaskId(), data.getAttemptId());
        long[] taskExecTime = getTaskExecTime(taskAttemptURL);

        data.setCounter(taskCounter);
        data.setTime(taskExecTime);
      }
    }

    private String getTaskFailedStackTrace(URL taskAllAttemptsUrl)
        throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(taskAllAttemptsUrl);
      JsonNode tasks = rootNode.path("taskAttempts").path("taskAttempt");
      for (JsonNode task : tasks) {
        String state = task.get("state").getValueAsText();
        if (!state.equals("FAILED")) {
          continue;
        }
        String stacktrace = task.get("diagnostics").getValueAsText();
        if (stacktrace.startsWith("Error:")) {
          return stacktrace;
        } else {
          // This is not a valid stacktrace. Might due to a bug in hadoop2.3 and fixed in 2.4
          throw new RuntimeException("This is not a valid stack trace.");
        }
      }
      throw new RuntimeException("No failed task attempt in this failed task.");
    }
  }
}

final class ThreadContextMR2 {
  private static final Logger logger = Logger.getLogger(ThreadContextMR2.class);
  private static final AtomicInteger THREAD_ID = new AtomicInteger(1);

  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>() {
    @Override
    public Integer initialValue() {
      return THREAD_ID.getAndIncrement();
    }
  };

  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private static final ThreadLocal<Pattern> _LOCAL_DIAGNOSTIC_PATTERN = new ThreadLocal<Pattern>() {
    @Override
    public Pattern initialValue() {
      // Example: "Task task_1443068695259_9143_m_000475 failed 1 times"
      return Pattern.compile(
          "Task[\\s\\u00A0]+(.*)[\\s\\u00A0]+failed[\\s\\u00A0]+([0-9])[\\s\\u00A0]+times[\\s\\u00A0]+");
    }
  };

  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>() {
        @Override
        public AuthenticatedURL.Token initialValue() {
          _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
          // Random an interval for each executor to avoid update token at the same time
          _LOCAL_UPDATE_INTERVAL.set(
              Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS));
          logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval "
              + _LOCAL_UPDATE_INTERVAL.get() * 1.0 / Statistics.MINUTE_IN_MS + " seconds");
          return new AuthenticatedURL.Token();
        }
      };

  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>() {
    @Override
    public AuthenticatedURL initialValue() {
      return new AuthenticatedURL();
    }
  };

  private static final ThreadLocal<ObjectMapper> _LOCAL_MAPPER = new ThreadLocal<ObjectMapper>() {
    @Override
    public ObjectMapper initialValue() {
      return new ObjectMapper();
    }
  };

  private ThreadContextMR2() {
    // Empty on purpose
  }

  public static Matcher getDiagnosticMatcher(String diagnosticInfo) {
    return _LOCAL_DIAGNOSTIC_PATTERN.get().matcher(diagnosticInfo);
  }

  public static JsonNode readJsonNode(URL url)
      throws IOException, AuthenticationException {
    HttpURLConnection conn = _LOCAL_AUTH_URL.get().openConnection(url, _LOCAL_AUTH_TOKEN.get());
    return _LOCAL_MAPPER.get().readTree(conn.getInputStream());
  }

  public static void updateAuthToken() {
    long curTime = System.currentTimeMillis();
    if (curTime - _LOCAL_LAST_UPDATED.get() > _LOCAL_UPDATE_INTERVAL.get()) {
      logger.info("Executor " + _LOCAL_THREAD_ID.get() + " updates its AuthenticatedToken.");
      _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
      _LOCAL_AUTH_URL.set(new AuthenticatedURL());
      _LOCAL_LAST_UPDATED.set(curTime);
    }
  }
}
