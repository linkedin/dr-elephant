/*
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
 *
 */
package com.linkedin.drelephant.tez.fetchers;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;
import com.linkedin.drelephant.util.ThreadContextMR2;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Task level data mining for Tez Tasks from timeline server API
 */

public class TezFetcher implements ElephantFetcher<TezApplicationData> {

  private static final Logger logger = Logger.getLogger(TezFetcher.class);

  private static final String TIMELINE_SERVER_URL = "yarn.timeline-service.webapp.address";

  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private String _timelineWebAddr;

  private FetcherConfigurationData _fetcherConfigurationData;

  public TezFetcher(FetcherConfigurationData fetcherConfData) throws IOException {
    this._fetcherConfigurationData = fetcherConfData;
    final String applicationHistoryAddr = new Configuration().get(TIMELINE_SERVER_URL);

    //Connection validity checked using method verifyURL(_timelineWebAddr) inside URLFactory constructor;
    _urlFactory = new URLFactory(applicationHistoryAddr);
    logger.info("Connection success.");

    _jsonFactory = new JSONFactory();
    _timelineWebAddr = "http://" + _timelineWebAddr + "/ws/v1/timeline/";

  }

  public TezApplicationData fetchData(AnalyticJob analyticJob) throws IOException, AuthenticationException {

    int maxSize = 0;
    String appId = analyticJob.getAppId();
    TezApplicationData jobData = new TezApplicationData();
    jobData.setAppId(appId);
    Properties jobConf = _jsonFactory.getProperties(_urlFactory.getApplicationURL(appId));
    jobData.setConf(jobConf);
    URL dagIdsUrl = _urlFactory.getDagURLByTezApplicationId(appId);

    List<String> dagIdsByApplicationId = _jsonFactory.getDagIdsByApplicationId(dagIdsUrl);

    List<TezTaskData> mapperListAggregate = new ArrayList<TezTaskData>();
    List<TezTaskData> reducerListAggregate = new ArrayList<TezTaskData>();

    //Iterate over dagIds and choose the dagId with the highest no. of tasks/highest impact as settings changes can be made only at DAG level.
    for(String dagId : dagIdsByApplicationId){
      try {
        //set job task independent properties

        URL dagUrl = _urlFactory.getDagURL(dagId);
        String state = _jsonFactory.getState(dagUrl);

        jobData.setStartTime(_jsonFactory.getDagStartTime(dagUrl));
        jobData.setFinishTime(_jsonFactory.getDagEndTime(dagUrl));

        if (state.equals("SUCCEEDED")) {
          jobData.setSucceeded(true);

          List<TezTaskData> mapperList = new ArrayList<TezTaskData>();
          List<TezTaskData> reducerList = new ArrayList<TezTaskData>();

          // Fetch task data
          URL vertexListUrl = _urlFactory.getVertexListURL(dagId);
          _jsonFactory.getTaskDataAll(vertexListUrl, dagId, mapperList, reducerList);

          if(mapperList.size() + reducerList.size() > maxSize){
            mapperListAggregate = mapperList;
            reducerListAggregate = reducerList;
            maxSize = mapperList.size() + reducerList.size();
          }
        }
        if (state.equals("FAILED")) {
          jobData.setSucceeded(false);
        }
      }
      finally {
        ThreadContextMR2.updateAuthToken();
      }
    }

    TezTaskData[] mapperData = mapperListAggregate.toArray(new TezTaskData[mapperListAggregate.size()]);
    TezTaskData[] reducerData = reducerListAggregate.toArray(new TezTaskData[reducerListAggregate.size()]);

    TezCounterData dagCounter = _jsonFactory.getDagCounter(_urlFactory.getDagURL(_jsonFactory.getDagIdsByApplicationId(dagIdsUrl).get(0)));

    jobData.setCounters(dagCounter).setMapTaskData(mapperData).setReduceTaskData(reducerData);

    return jobData;
  }

  private URL getTaskListByVertexURL(String dagId, String vertexId) throws MalformedURLException {
    return _urlFactory.getTaskListByVertexURL(dagId, vertexId);
  }

  private URL getTaskURL(String taskId) throws MalformedURLException {
    return _urlFactory.getTasksURL(taskId);
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
          "&secondaryFilter=TEZ_VERTEX_ID:" + vertexId + "&limit=500000");
    }

    private URL getTasksURL(String taskId) throws MalformedURLException {
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

  /**
   * JSONFactory class provides functionality to parse mined job data from timeline server.
   */

  private class JSONFactory {

    private String getState(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(url);
      return rootNode.path("otherinfo").path("status").getTextValue();
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
        value = configs.get(key).getTextValue();
        jobConf.put(key, value);
      }
      return jobConf;
    }

    private List<String> getDagIdsByApplicationId(URL dagIdsUrl) throws IOException, AuthenticationException {
      List<String> dagIds = new ArrayList<String>();
      JsonNode nodes = ThreadContextMR2.readJsonNode(dagIdsUrl).get("entities");

      for (JsonNode node : nodes) {
        String dagId = node.get("entity").getTextValue();
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
          String name = counter.get("counterName").getTextValue();
          String groupName = group.get("counterGroupName").getTextValue();
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
        String vertexId = vertex.get("entity").getTextValue();
        String vertexClass = vertex.path("otherinfo").path("processorClassName").getTextValue();

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
        String state = task.path("otherinfo").path("status").getTextValue();
        String taskId = task.get("entity").getValueAsText();
        String attemptId = task.path("otherinfo").path("successfulAttemptId").getTextValue();
        if (state.equals("SUCCEEDED")) {
          attemptId = task.path("otherinfo").path("successfulAttemptId").getTextValue();
        }
        else{
          JsonNode firstAttempt = getTaskFirstFailedAttempt(_urlFactory.getTaskAllAttemptsURL(dagId,taskId));
          if(firstAttempt != null){
            attemptId = firstAttempt.get("entity").getTextValue();
          }
        }

        taskList.add(new TezTaskData(taskId, attemptId));
      }

      getTaskData(dagId, taskList, isMapVertex);

    }

    private JsonNode getTaskFirstFailedAttempt(URL taskAllAttemptsUrl) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextMR2.readJsonNode(taskAllAttemptsUrl);
      long firstAttemptFinishTime = Long.MAX_VALUE;
      JsonNode firstAttempt = null;
      JsonNode taskAttempts = rootNode.path("entities");
      for (JsonNode taskAttempt : taskAttempts) {
        String state = taskAttempt.path("otherinfo").path("counters").path("status").getTextValue();
        if (state.equals("SUCCEEDED")) {
          continue;
        }
        long finishTime = taskAttempt.path("otherinfo").path("counters").path("endTime").getLongValue();
        if( finishTime < firstAttemptFinishTime) {
          firstAttempt = taskAttempt;
          firstAttemptFinishTime = finishTime;
        }
      }
      return firstAttempt;
    }



    private void getTaskData(String dagId, List<TezTaskData> taskList, boolean isMapTask)
        throws IOException, AuthenticationException {

      for(int i=0; i<taskList.size(); i++) {
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

      //Fetch task level metrics
      for (JsonNode group : groups) {
        for (JsonNode counter : group.path("counters")) {
          String name = counter.get("counterName").getTextValue();
          String groupName = group.get("counterGroupName").getTextValue();
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
          String name = counter.get("counterName").getTextValue();
          if (!isMapTask && name.equals("MERGE_PHASE_TIME")) {
            mergeTime = counter.get("counterValue").getLongValue();
          }
          else if (!isMapTask && name.equals("SHUFFLE_PHASE_TIME")){
            shuffleTime = counter.get("counterValue").getLongValue();
          }

        }
      }

      long[] time = new long[] { finishTime - startTime, shuffleTime, mergeTime, startTime, finishTime };

      return time;
    }
  }
}