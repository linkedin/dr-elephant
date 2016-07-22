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

package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import controllers.AzkabanFetchFlowGraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppJobNameMap;
import models.AppResult;
import org.apache.log4j.Logger;
import org.json.JSONArray;


/**
 * This class wraps some basic meta data of a completed application run (notice that the information is generally the
 * same regardless of hadoop versions and application types), and then promises to return the analyzed result later.
 */
public class AnalyticJob {
  private static final Logger logger = Logger.getLogger(AnalyticJob.class);

  private static final String UNKNOWN_JOB_TYPE = "Unknown";   // The default job type when the data matches nothing.
  private static final String PIG_PARENT_PROPERTY = "pig.parent.jobid"; // Comma separated list of parent job ids
  private static final String AZKABAN_PROJECT_NAME_PROPERTY = "azkaban.flow.projectname";
  private static final String AZKABAN_FLOW_NAME_PROPERTY = "azkaban.flow.flowid";
  private static final int _RETRY_LIMIT = 3;                  // Number of times a job needs to be tried before dropping

  private int _retries = 0;
  private ApplicationType _type;
  private String _appId;
  private String _name;
  private String _queueName;
  private String _user;
  private String _trackingUrl;
  private long _startTime;
  private long _finishTime;

  /**
   * Returns the application type
   * E.g., Mapreduce or Spark
   *
   * @return The application type
   */
  public ApplicationType getAppType() {
    return _type;
  }

  /**
   * Set the application type of this job.
   *
   * @param type The Application type
   * @return The analytic job
   */
  public AnalyticJob setAppType(ApplicationType type) {
    _type = type;
    return this;
  }

  /**
   * Returns the application id
   *
   * @return The analytic job
   */
  public String getAppId() {
    return _appId;
  }

  /**
   * Set the application id of this job
   *
   * @param appId The application id of the job obtained resource manager
   * @return The analytic job
   */
  public AnalyticJob setAppId(String appId) {
    _appId = appId;
    return this;
  }

  /**
   * Returns the name of the analytic job
   *
   * @return the analytic job's name
   */
  public String getName() {
    return _name;
  }

  /**
   * Set the name of the analytic job
   *
   * @param name
   * @return The analytic job
   */
  public AnalyticJob setName(String name) {
    _name = name;
    return this;
  }

  /**
   * Returns the user who ran the job
   *
   * @return The user who ran the analytic job
   */
  public String getUser() {
    return _user;
  }

  /**
   * Sets the user who ran the job
   *
   * @param user The username of the user
   * @return The analytic job
   */
  public AnalyticJob setUser(String user) {
    _user = user;
    return this;
  }

  /**
   * Returns the time at which the job was submitted by the resource manager
   *
   * @return The start time
   */
  public long getStartTime() {
    return _startTime;
  }

  /**
   * Sets the start time of the job
   * Start time is the time at which the job was submitted by the resource manager
   *
   * @param startTime
   * @return The analytic job
   */
  public AnalyticJob setStartTime(long startTime) {
    // TIMESTAMP range starts from FROM_UNIXTIME(1) = 1970-01-01 00:00:01
    if (startTime <= 0) {
      startTime = 1000; // 1 sec
    }
    _startTime = startTime;
    return this;
  }

  /**
   * Returns the finish time of the job.
   *
   * @return The finish time
   */
  public long getFinishTime() {
    return _finishTime;
  }

  /**
   * Sets the finish time of the job
   *
   * @param finishTime
   * @return The analytic job
   */
  public AnalyticJob setFinishTime(long finishTime) {
    // TIMESTAMP range starts from FROM_UNIXTIME(1) = 1970-01-01 00:00:01
    if (finishTime <= 0) {
      finishTime = 1000; // 1 sec
    }
    _finishTime = finishTime;
    return this;
  }

  /**
   * Returns the tracking url of the job
   *
   * @return The tracking url in resource manager
   */
  public String getTrackingUrl() {
    return _trackingUrl;
  }

  /**
   * Sets the tracking url for the job
   *
   * @param trackingUrl The url to track the job
   * @return The analytic job
   */
  public AnalyticJob setTrackingUrl(String trackingUrl) {
    _trackingUrl = trackingUrl;
    return this;
  }

  /**
   * Returns the queue in which the application was submitted
   *
   * @return The queue name
   */
  public String getQueueName() {
    return _queueName;
  }

  /**
   * Set the queue name in which the analytic jobs was submitted
   *
   * @param name the name of the queue
   * @return The analytic job
   */
  public AnalyticJob setQueueName(String name) {
    _queueName = name;
    return this;
  }

  /**
   * Returns the analysed AppResult that could be directly serialized into DB.
   *
   * This method fetches the data using the appropriate application fetcher, runs all the heuristics on them and
   * loads it into the AppResult model.
   *
   * @throws Exception if the analysis process encountered a problem.
   * @return the analysed AppResult
   */
  public AppResult getAnalysis()
      throws Exception {
    ElephantFetcher fetcher = ElephantContext.instance().getFetcherForApplicationType(getAppType());
    HadoopApplicationData data = fetcher.fetchData(this);

    // Run all heuristics over the fetched data
    List<HeuristicResult> analysisResults = new ArrayList<HeuristicResult>();
    if (data == null || data.isEmpty()) {
      // Example: a MR job has 0 mappers and 0 reducers
      logger.info("No Data Received for analytic job: " + getAppId());
      analysisResults.add(HeuristicResult.NO_DATA);
    } else {
      List<Heuristic> heuristics = ElephantContext.instance().getHeuristicsForApplicationType(getAppType());
      for (Heuristic heuristic : heuristics) {
        HeuristicResult result = heuristic.apply(data);
        if (result != null) {
          analysisResults.add(result);
        }
      }
    }

    JobType jobType = ElephantContext.instance().matchJobType(data);
    String jobTypeName = jobType == null ? UNKNOWN_JOB_TYPE : jobType.getName();

    HadoopMetricsAggregator hadoopMetricsAggregator =
        ElephantContext.instance().getAggregatorForApplicationType(getAppType());
    hadoopMetricsAggregator.aggregate(data);
    HadoopAggregatedData hadoopAggregatedData = hadoopMetricsAggregator.getResult();

    // Load app information
    AppResult result = new AppResult();
    result.id = Utils.truncateField(getAppId(), AppResult.ID_LIMIT, getAppId());
    result.trackingUrl = Utils.truncateField(getTrackingUrl(), AppResult.TRACKING_URL_LIMIT, getAppId());
    result.queueName = Utils.truncateField(getQueueName(), AppResult.QUEUE_NAME_LIMIT, getAppId());
    result.username = Utils.truncateField(getUser(), AppResult.USERNAME_LIMIT, getAppId());
    result.startTime = getStartTime();
    result.finishTime = getFinishTime();
    result.name = Utils.truncateField(getName(), AppResult.APP_NAME_LIMIT, getAppId());
    result.jobType = Utils.truncateField(jobTypeName, AppResult.JOBTYPE_LIMIT, getAppId());
    result.resourceUsed = hadoopAggregatedData.getResourceUsed();
    result.totalDelay = hadoopAggregatedData.getTotalDelay();
    result.resourceWasted = hadoopAggregatedData.getResourceWasted();

    // TODO: Make Parent configurable
    result.parents = Utils.truncateField(data.getConf().getProperty(PIG_PARENT_PROPERTY), AppResult.PARENT_LEN_LIMIT, getAppId());;

    // Load App Heuristic information
    int jobScore = 0;
    result.yarnAppHeuristicResults = new ArrayList<AppHeuristicResult>();
    Severity worstSeverity = Severity.NONE;
    for (HeuristicResult heuristicResult : analysisResults) {
      AppHeuristicResult detail = new AppHeuristicResult();
      detail.heuristicClass = Utils.truncateField(heuristicResult.getHeuristicClassName(),
          AppHeuristicResult.HEURISTIC_CLASS_LIMIT, getAppId());
      detail.heuristicName = Utils.truncateField(heuristicResult.getHeuristicName(),
          AppHeuristicResult.HEURISTIC_NAME_LIMIT, getAppId());
      detail.severity = heuristicResult.getSeverity();
      detail.score = heuristicResult.getScore();

      // Load Heuristic Details
      for (HeuristicResultDetails heuristicResultDetails : heuristicResult.getHeuristicResultDetails()) {
        AppHeuristicResultDetails heuristicDetail = new AppHeuristicResultDetails();
        heuristicDetail.yarnAppHeuristicResult = detail;
        heuristicDetail.name = Utils.truncateField(heuristicResultDetails.getName(),
            AppHeuristicResultDetails.NAME_LIMIT, getAppId());
        heuristicDetail.value = Utils.truncateField(heuristicResultDetails.getValue(),
            AppHeuristicResultDetails.VALUE_LIMIT, getAppId());
        heuristicDetail.details = Utils.truncateField(heuristicResultDetails.getDetails(),
            AppHeuristicResultDetails.DETAILS_LIMIT,
                getAppId());
        // This was added for AnalyticTest. Commenting this out to fix a bug. Also disabling AnalyticJobTest.
        //detail.yarnAppHeuristicResultDetails = new ArrayList<AppHeuristicResultDetails>();
        detail.yarnAppHeuristicResultDetails.add(heuristicDetail);
      }
      result.yarnAppHeuristicResults.add(detail);
      worstSeverity = Severity.max(worstSeverity, detail.severity);
      jobScore += detail.score;
    }
    result.severity = worstSeverity;
    result.score = jobScore;

    // Retrieve information from job configuration like scheduler information and store them into result.
    InfoExtractor.loadInfo(result, data);

    // TODO: Move this logic to Azkaban class and make it configurable.
    String project_name = data.getConf().getProperty(AZKABAN_PROJECT_NAME_PROPERTY);
    String flow_name = data.getConf().getProperty(AZKABAN_FLOW_NAME_PROPERTY);
    AzkabanFetchFlowGraph fetchGraph = new AzkabanFetchFlowGraph();

    // If we already have an entry for this flow execution id and jobname, it means we already have information for
    // that flow and hence return the result.
    if (AppJobNameMap.find.select("*")
        .where()
        .eq(AppJobNameMap.TABLE.FLOW_EXEC_ID, result.flowExecId)
        .eq(AppJobNameMap.TABLE.JOB_NAME, result.jobName)
        .findList()
        .size() != 0) {
      return result;
    }

    // Fetching information for this flow
    JSONArray jarr = fetchGraph.fetch(result.flowExecId, project_name, flow_name);
    String ownName, oneIn;

    int jLen, i, j, uidNum;
    if (jarr == null) {
      jLen = 0;
    } else {
      jLen = jarr.length();
    }

    HashMap<String, Integer> jobId = new HashMap<String, Integer>();
    int count, arrLen;

    // Saving information for a flow in the table "AppJobNameMap"
    for (i = 0; i < jLen; i++) {
      String inStr = null;
      ownName = (String) jarr.getJSONObject(i).get("id");
      if (AppJobNameMap.find.select("*")
          .where()
          .eq(AppJobNameMap.TABLE.FLOW_EXEC_ID, result.flowExecId)
          .eq(AppJobNameMap.TABLE.JOB_NAME, ownName)
          .findList()
          .size() == 0) {
        AppJobNameMap jobNameMap = new AppJobNameMap();
        jobNameMap.flowExecId = result.flowExecId;
        jobNameMap.jobName = ownName;
        if (jobId.get(jobNameMap.jobName) == null) {
          count = jobId.size();
          jobNameMap.jobNameId = count + 1;
        } else {
          jobNameMap.jobNameId = jobId.get(jobNameMap.jobName);
        }

        // Adding this entry in the hashmap.
        jobId.put(jobNameMap.jobName, jobNameMap.jobNameId);

        if (jarr.getJSONObject(i).has("in")) {
          arrLen = jarr.getJSONObject(i).getJSONArray("in").length();
          for (j = 0; j < arrLen; j++) {
            oneIn = (String) jarr.getJSONObject(i).getJSONArray("in").get(j);

            if (jobId.get(oneIn) == null) {
              jobId.put(oneIn, jobId.size() + 1);
            }
            uidNum = jobId.get(oneIn);

            if (inStr != null && !inStr.isEmpty()) {
              inStr += ",";
            }
            if (inStr == null) {
              inStr = Integer.toString(uidNum);
            } else {
              inStr += uidNum;
            }
          }
        }
        jobNameMap.jobInnodes = inStr;

        jobNameMap.save();
      }
    }

    return result;
  }

  /**
   * Indicate this promise should retry itself again.
   *
   * @return true if should retry, else false
   */
  public boolean retry() {
    return (_retries++) < _RETRY_LIMIT;
  }
}
