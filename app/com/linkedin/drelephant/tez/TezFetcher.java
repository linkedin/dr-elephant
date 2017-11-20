package com.linkedin.drelephant.tez;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import com.linkedin.drelephant.util.Utils;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TezFetcher implements ElephantFetcher<TezApplicationData> {

  private static final Logger logger = Logger.getLogger(ElephantFetcher.class);
  private static final int MAX_SAMPLE_SIZE = 200;

  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private String _timelineWebAddr;
  private FetcherConfigurationData _fetcherConfigurationData;

  public TezFetcher(FetcherConfigurationData fetcherConfData) throws IOException {
    this._fetcherConfigurationData = fetcherConfData;
    final String applicationHistoryAddr = new Configuration().get("yarn.timeline-service.webapp.address");

    _urlFactory = new URLFactory(applicationHistoryAddr);
    logger.info("Connection success.");

    _jsonFactory = new JSONFactory();
    _timelineWebAddr = "http://" + _timelineWebAddr + "/ws/v1/timeline/";

  }

  public TezApplicationData fetchData(AnalyticJob analyticJob) throws IOException, AuthenticationException {
    String appId = analyticJob.getAppId();
    TezApplicationData jobData = new TezApplicationData();

    URL dagIdsUrl = _urlFactory.getDagURLByTezApplicationId(appId);
    // TODO We are getting only the first dagId
    String dagId = _jsonFactory.getDagIdsByApplicationId(dagIdsUrl).get(0);

    try {
      jobData.setAppId(appId);
      URL dagUrl = _urlFactory.getDagURL(dagId);
      String state = _jsonFactory.getState(dagUrl);
      Properties jobConf = _jsonFactory.getProperties(_urlFactory.getApplicationURL(appId));
      jobData.setConf(jobConf);
      jobData.setStartTime(_jsonFactory.getDagStartTime(dagUrl));
      jobData.setFinishTime(_jsonFactory.getDagEndTime(dagUrl));

      if (state.equals("SUCCEEDED")) {
        jobData.setSucceeded(true);

        // Fetch job counter
        TezCounterData dagCounter = _jsonFactory.getDagCounter(_urlFactory.getDagURL(dagId));
        List<TezTaskData> mapperList = new ArrayList<TezTaskData>();
        List<TezTaskData> reducerList = new ArrayList<TezTaskData>();

        // Fetch task data
        URL vertexListUrl = _urlFactory.getVertexListURL(dagId);
        _jsonFactory.getTaskDataAll(vertexListUrl, dagId, mapperList, reducerList);
        TezTaskData[] mapperData = mapperList.toArray(new TezTaskData[mapperList.size()]);
        TezTaskData[] reducerData = reducerList.toArray(new TezTaskData[reducerList.size()]);

        jobData.setCounters(dagCounter).setMapTaskData(mapperData).setReduceTaskData(reducerData);
      } else if (state.equals("FAILED")) {
        jobData.setSucceeded(false);
        String diagnosticInfo;
        //Set DiagonosticInfo
      }
    }
    finally {
      ThreadContextMR2.updateAuthToken();
    }

    return jobData;
  }

  private URL getTaskListByVertexURL(String dagId, String vertexId) throws MalformedURLException {
    return _urlFactory.getTaskListByVertexURL(dagId, vertexId);
  }

  private URL getTaskURL(String taskId) throws MalformedURLException {
    return _urlFactory.getTaksURL(taskId);
  }

  private URL getTaskAttemptURL(String dagId, String taskId, String attemptId) throws MalformedURLException {
    return _urlFactory.getTaskAttemptURL(dagId, taskId, attemptId);
  }

  private class URLFactory {

    private String _timelineWebAddr;

    private URLFactory(String hserverAddr) throws IOException {
      _timelineWebAddr = "http://" + hserverAddr + "/ws/v1/timeline";
      verifyURL(_timelineWebAddr);
    }

    private void verifyURL(String url) throws IOException {
      final URLConnection connection = new URL(url).openConnection();
      // Check service availability
      connection.connect();
      return;
    }

    private URL getDagURLByTezApplicationId(String applicationId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_DAG_ID?primaryFilter=applicationId:" + applicationId);
    }

    private URL getApplicationURL(String applicationId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_APPLICATION/tez_" + applicationId);
    }

    private URL getDagURL(String dagId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_DAG_ID/" + dagId);
    }

    private URL getVertexListURL(String dagId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_VERTEX_ID?primaryFilter=TEZ_DAG_ID:" + dagId);
    }

    private URL getTaskListByVertexURL(String dagId, String vertexId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_TASK_ID?primaryFilter=TEZ_DAG_ID:" + dagId +
          "&secondaryFilter=TEZ_VERTEX_ID:" + vertexId);
    }

    private URL getTaksURL(String taskId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_TASK_ID/" + taskId);
    }

    private URL getTaskAllAttemptsURL(String dagId, String taskId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_TASK_ATTEMPT_ID?primaryFilter=TEZ_DAG_ID:" + dagId +
          "&secondaryFilter=TEZ_TASK_ID:" + taskId);
    }

    private URL getTaskAttemptURL(String dagId, String taskId, String attemptId) throws MalformedURLException {
      return new URL(_timelineWebAddr + "/TEZ_TASK_ATTEMPT_ID/" + attemptId);
    }

  }

  private class JSONFactory {

    private String getState(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("otherinfo").path("status").getValueAsText();
    }

    private Properties getProperties(URL url) throws IOException, AuthenticationException {
      Properties jobConf = new Properties();
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode configs = rootNode.path("otherinfo").path("config");
      Iterator<String> keys = configs.getFieldNames();
      String key = "";
      String value = "";
      while (keys.hasNext()) {
        key = keys.next();
        value = configs.get(key).getValueAsText();
        jobConf.put(key, value);
      }
      return jobConf;
    }

    private List<String> getDagIdsByApplicationId(URL dagIdsUrl) throws IOException, AuthenticationException {
      List<String> dagIds = new ArrayList<String>();
      JsonNode nodes = ThreadContextMR2.readJsonNode(dagIdsUrl).get("entities");

      for (JsonNode node : nodes) {
        String dagId = node.get("entity").getValueAsText();
        dagIds.add(dagId);
      }

      return dagIds;
    }

    private TezCounterData getDagCounter(URL url) throws IOException, AuthenticationException {
      TezCounterData holder = new TezCounterData();
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("otherinfo").path("counters").path("counterGroups");

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counters")) {
          String name = counter.get("counterDisplayName").getValueAsText();
          String groupName = group.get("counterGroupDisplayName").getValueAsText();
          Long value = counter.get("counterValue").getLongValue();
          holder.set(groupName, name, value);
        }
      }

      return holder;
    }

    private long getDagStartTime(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      long startTime = rootNode.path("otherinfo").get("startTime").getLongValue();
      return startTime;
    }

    private long getDagEndTime(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      long endTime = rootNode.path("otherinfo").get("endTime").getLongValue();
      return endTime;
    }

    private void getTaskDataAll(URL vertexListUrl, String dagId, List<TezTaskData> mapperList,
                                List<TezTaskData> reducerList) throws IOException, AuthenticationException {

      JsonNode rootVertexNode = ThreadContextMR2.readJsonNode(vertexListUrl);
      JsonNode vertices = rootVertexNode.path("entities");
      boolean isMapVertex = false;

      for (JsonNode vertex : vertices) {
        String vertexId = vertex.get("entity").getValueAsText();
        String vertexClass = vertex.path("otherinfo").path("processorClassName").getValueAsText();

        if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.MapTezProcessor"))
          isMapVertex = true;
        else if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.ReduceTezProcessor"))
          isMapVertex = false;

        URL tasksByVertexURL = getTaskListByVertexURL(dagId, vertexId);
        if(isMapVertex)
          getTaskDataByVertexId(tasksByVertexURL, dagId, vertexId, mapperList, true);
        else
          getTaskDataByVertexId(tasksByVertexURL, dagId, vertexId, reducerList, false);
      }
    }

    private void getTaskDataByVertexId(URL url, String dagId, String vertexId, List<TezTaskData> taskList,
                                       boolean isMapVertex) throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode tasks = rootNode.path("entities");
      for (JsonNode task : tasks) {
        String state = task.path("otherinfo").path("status").getValueAsText();
        if (!state.equals("SUCCEEDED")) {
          // This is a failed task.
          continue;
        }
        String taskId = task.get("entity").getValueAsText();
        String attemptId = task.path("otherinfo").path("successfulAttemptId").getValueAsText();

        taskList.add(new TezTaskData(taskId, attemptId));
      }

      getTaskData(dagId, taskList, isMapVertex);

    }

    private void getTaskData(String dagId, List<TezTaskData> taskList, boolean isMapTask)
        throws IOException, AuthenticationException {

      if (taskList.size() > MAX_SAMPLE_SIZE) {
        logger.info(dagId + " needs sampling.");
        Collections.shuffle(taskList);
      }

      int sampleSize = Math.min(taskList.size(), MAX_SAMPLE_SIZE);
      for (int i=0; i<sampleSize; i++) {
        TezTaskData data = taskList.get(i);
        URL taskCounterURL = getTaskURL(data.getTaskId());
        TezCounterData taskCounter = getTaskCounter(taskCounterURL);

        URL taskAttemptURL = getTaskAttemptURL(dagId, data.getTaskId(), data.getAttemptId());
        long[] taskExecTime = getTaskExecTime(taskAttemptURL, isMapTask);

        data.setCounter(taskCounter);
        data.setTime(taskExecTime);
      }

    }

    private TezCounterData getTaskCounter(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("otherinfo").path("counters").path("counterGroups");
      TezCounterData holder = new TezCounterData();

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counters")) {
          String name = counter.get("counterDisplayName").getValueAsText();
          String groupName = group.get("counterGroupDisplayName").getValueAsText();
          Long value = counter.get("counterValue").getLongValue();
          holder.set(groupName, name, value);
        }
      }

      return holder;
    }

    private long[] getTaskExecTime(URL url, boolean isMapTask) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      JsonNode groups = rootNode.path("otherinfo").path("counters").path("counterGroups");

      long startTime = rootNode.path("otherinfo").get("startTime").getLongValue();
      long finishTime = rootNode.path("otherinfo").get("endTime").getLongValue();

      long shuffleTime = 0;
      long mergeTime = 0;

      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counters")) {
          String name = counter.get("counterDisplayName").getValueAsText();
          if (!isMapTask && name.equals("MERGE_PHASE_TIME")) {
            mergeTime = counter.get("counterValue").getLongValue();
          }
          else if (!isMapTask && name.equals("SHUFFLE_PHASE_TIME"))
            shuffleTime = counter.get("counterValue").getLongValue();
        }
      }

      long[] time = new long[] { finishTime - startTime, shuffleTime, mergeTime, startTime, finishTime };

      return time;
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
          _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong()
              % (3 * Statistics.MINUTE_IN_MS));
          logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
              / Statistics.MINUTE_IN_MS);
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

  public static JsonNode readJsonNode(URL url) throws IOException, AuthenticationException {
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