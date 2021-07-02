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

package controllers.api.v1;

import com.avaje.ebean.Query;
import com.avaje.ebean.Junction;
import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.SqlQuery;
import com.avaje.ebean.Ebean;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.JobType;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.exceptions.ExceptionFinder;
import com.linkedin.drelephant.exceptions.HadoopException;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import controllers.ControllerUtil;
import controllers.IdUrlPair;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import javax.naming.AuthenticationException;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import play.data.DynamicForm;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import controllers.Application;


/**
 * The Web controller defines the rest interfaces for the Dr. Elephant User interface.
 */
public class Web extends Controller {

  private static final Logger logger = Logger.getLogger(Web.class);

  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;

  private static final int MAX_APPLICATIONS = 50;
  private static final int MAX_APPLICATIONS_IN_WORKFLOW = 5000;
  private static final int MAX_APPLICATIONS_IN_JOB = 5000;
  private static final int MAX_FLOW_LIMIT = 25;
  private static final int MAX_JOB_LIMIT = 25;
  private static final int SEARCH_DEFAULT_PAGE_OFFSET = 0;
  private static final int SEARCH_DEFAULT_PAGE_LIMIT = 25;
  private static final int SEARCH_APPLICATION_MAX_OFFSET = 500;

  private static long _lastFetch = 0;
  private static int _numJobsAnalyzed = 0;
  private static int _numJobsCritical = 0;
  private static int _numJobsSevere = 0;
  private static int _numJobsModerate = 0;
  private static int _numJobsLow = 0;
  private static int _numJobsNone = 0;

  /**
   * Returns the json object for the dashboard summaries of jobs analzyed in last day.
   */
  public static Result restDashboardSummaries() {

    long now = System.currentTimeMillis();
    long finishDate = now - DAY;

    //Update statistics only after FETCH_DELAY
    if (now - _lastFetch > FETCH_DELAY) {
      _numJobsAnalyzed = AppResult.find.where()
          .gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .findRowCount();
      _numJobsCritical = AppResult.find.where()
          .gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.CRITICAL.getValue())
          .findRowCount();
      _numJobsSevere = AppResult.find.where()
          .gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.SEVERE.getValue())
          .findRowCount();
      _numJobsModerate = AppResult.find.where().gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.MODERATE.getValue())
          .findRowCount();
      _numJobsLow = AppResult.find.where().gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.LOW.getValue())
          .findRowCount();
      _numJobsNone = AppResult.find.where().gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.NONE.getValue())
          .findRowCount();
      _lastFetch = now;
    }

    JsonObject dashboard = new JsonObject();
    dashboard.addProperty(JsonKeys.ID, "dashboard");
    dashboard.addProperty(JsonKeys.TOTAL, _numJobsAnalyzed);
    dashboard.addProperty(JsonKeys.CRITICAL, _numJobsCritical);
    dashboard.addProperty(JsonKeys.SEVERE, _numJobsSevere);
    dashboard.addProperty(JsonKeys.MODERATE, _numJobsModerate);
    dashboard.addProperty(JsonKeys.LOW, _numJobsLow);
    dashboard.addProperty(JsonKeys.NONE, _numJobsNone);
    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.DASHBOARD_SUMMARIES, dashboard);

    return ok(new Gson().toJson(parent));
  }

  /**
   * Returns the list of AppResults for the given username limit by maxApplications
   * @param username The username for which applications need to be fetched.
   * @param maxApplications The max number of applications that should be fetched
   * @return The list of Applications that should for the given username limit by maxApplications
   */
  private static List<AppResult> getApplications(String username, int maxApplications) {
    List<AppResult> results = AppResult.find.select("*").where().eq(AppResult.TABLE.USERNAME, username).order()
        .desc(AppResult.TABLE.FINISH_TIME).setMaxRows(maxApplications).findList();
    return results;
  }

  /**
   * Returns the list of AppResults limit by maxApplications
   * @param maxApplications The max number of applications that should be fetched
   * @return The list of Applications limit by maxApplications
   */
  private static List<AppResult> getApplications(int maxApplications) {
    List<AppResult> results =
        AppResult.find.select("*").order().desc(AppResult.TABLE.FINISH_TIME).setMaxRows(maxApplications).findList();
    return results;
  }

  /**
   * Returns the list of AppResults scheduled by a scheduler for the given username limit by maxApplications.
   * @param username The username for which applications need to be fetched.
   * @param maxApplications The max number of applications that should be fetched
   * @return The list of Applications scheduled by a scheduler that should be fetched for the given username limit by maxApplications
   */
  private static List<AppResult> getSchedulerApplications(String username, int maxApplications) {
    List<AppResult> results =
        AppResult.find.select("*").where().eq(AppResult.TABLE.USERNAME, username).ne(AppResult.TABLE.FLOW_EXEC_ID, null)
            .ne(AppResult.TABLE.FLOW_EXEC_ID, "").order().desc(AppResult.TABLE.FINISH_TIME).setMaxRows(maxApplications)
            .findList();
    return results;
  }

  /**
   * Returns the list of AppResults scheduled by a scheduler limit by maxApplications
   * @param maxApplications The max number of applications that should be fetched
   * @return The list of Applications scheduled by a scheduler limit by maxApplications
   */
  private static List<AppResult> getSchedulerApplications(int maxApplications) {
    List<AppResult> results =
        AppResult.find.select("*").where().ne(AppResult.TABLE.FLOW_EXEC_ID, null).ne(AppResult.TABLE.FLOW_EXEC_ID, "")
            .order().desc(AppResult.TABLE.FINISH_TIME).setMaxRows(maxApplications).findList();
    return results;
  }

  /**
   * Returns a list of AppResult with the the given flowExecId
   * @param flowExecId The flow execution id of the flow
   * @return The list of AppResult filtered by flow execution id
   */
  private static List<AppResult> getRestFlowResultsFromFlowExecutionId(String flowExecId) {
    List<AppResult> results = AppResult.find.select("*").where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId).order()
        .desc(AppResult.TABLE.FINISH_TIME).findList();
    return results;
  }

  ;

  /**
   * Returns a list of AppResult with the given jobExecId
   * @param jobExecId The job execution id of the job
   * @return The list of AppResult filtered by job execution id
   */
  private static List<AppResult> getRestJobResultsFromJobExecutionId(String jobExecId) {
    List<AppResult> results =
        AppResult.find.select(AppResult.getSearchFields()).where().eq(AppResult.TABLE.JOB_EXEC_ID, jobExecId).order()
            .desc(AppResult.TABLE.FINISH_TIME)
            .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields()).findList();
    return results;
  }

  /**
   * Returns the AppResult with the given applicationId
   * @param applicationId The application id of the application
   * @return The AppResult for the given application Id
   */
  private static AppResult getAppResultFromApplicationId(String applicationId) {
    AppResult result = AppResult.find.select("*").fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where().idEq(applicationId).order().desc(AppResult.TABLE.FINISH_TIME).findUnique();
    return result;
  }

  /**
   * This method returns the json object for the application-summaries based on the username
   * @param username The username for which application-summaries json must be returned
   * @return The application-summaries json for the given username
   * response object:
   * <pre>
   *{
   *  "application-summaries": [
   *  {
   *    "id": "sample_app_0000000001",
   *      "username": "user",
   *      "starttime": 1471910835628,
   *      "finishtime": 1471911099238,
   *      "runtime": 263610,
   *      "waittime": 46234,
   *      "resourceused": 101382144,
   *      "resourcewasted": 15993417,
   *      "severity": "Moderate",
   *      "heuristicsummary": [
   *    {
   *      "name": "Mapper Data Skew",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Mapper GC",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Mapper Time",
   *        "severity": "Moderate"
   *    },
   *    {
   *      "name": "Mapper Speed",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Mapper Spill",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Mapper Memory",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Reducer Data Skew",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Reducer GC",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Reducer Time",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Reducer Memory",
   *        "severity": "None"
   *    },
   *    {
   *      "name": "Shuffle & Sort",
   *        "severity": "Low"
   *    }
   *    ]
   *  }
   *  ]
   *}
   * </pre>
   * */
  public static Result restApplicationSummariesForUser(String username) {
    JsonArray applicationSummaryArray = new JsonArray();

    List<AppResult> results = null;
    if (username == null || username.isEmpty()) {
      results = getApplications(MAX_APPLICATIONS);
    } else {
      results = getApplications(username, MAX_APPLICATIONS);
    }

    for (AppResult application : results) {
      JsonObject applicationObject = new JsonObject();
      JsonArray heuristicsArray = new JsonArray();
      List<AppHeuristicResult> appHeuristicResult = application.yarnAppHeuristicResults;

      for (AppHeuristicResult heuristic : appHeuristicResult) {
        JsonObject heuristicObject = new JsonObject();
        heuristicObject.addProperty(JsonKeys.NAME, heuristic.heuristicName);
        heuristicObject.addProperty(JsonKeys.SEVERITY, heuristic.severity.getText());
        heuristicsArray.add(heuristicObject);
      }

      applicationObject.addProperty(JsonKeys.ID, application.id);
      applicationObject.addProperty(JsonKeys.USERNAME, application.username);
      applicationObject.addProperty(JsonKeys.JOB_NAME, application.jobName);
      applicationObject.addProperty(JsonKeys.JOB_TYPE, application.jobType);
      applicationObject.addProperty(JsonKeys.START_TIME, application.startTime);
      applicationObject.addProperty(JsonKeys.FINISH_TIME, application.finishTime);
      applicationObject.addProperty(JsonKeys.RUNTIME, application.finishTime - application.startTime);
      applicationObject.addProperty(JsonKeys.WAITTIME, application.totalDelay);
      applicationObject.addProperty(JsonKeys.RESOURCE_USED, application.resourceUsed);
      applicationObject.addProperty(JsonKeys.RESOURCE_WASTED, application.resourceWasted);
      applicationObject.addProperty(JsonKeys.QUEUE, application.queueName);
      applicationObject.addProperty(JsonKeys.SEVERITY, application.severity.getText());

      applicationObject.add(JsonKeys.HEURISTICS_SUMMARY, heuristicsArray);

      applicationSummaryArray.add(applicationObject);
    }

    JsonArray sortedApplicationSummaryArray = getSortedJsonArrayByFinishTime(applicationSummaryArray);

    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.APPLICATION_SUMMARIES, sortedApplicationSummaryArray);
    return ok(new Gson().toJson(parent));
  }

  /**
   * This method returns the json object for job-summaries for the given user
   * @param username The given username for which job-summaries json object should be returned
   * @return The job-summaries json object for the given username
   * response object:
   * <pre>
   *{
   *  "job-summaries": [
   *  {
   *    "id": "job-exec-id",
   *      "jobname": "jobname",
   *      "jobtype": "Pig",
   *      "username": "username",
   *      "starttime": 1471910835628,
   *      "finishtime": 1471911099238,
   *      "runtime": 263610,
   *      "waittime": 46234,
   *      "resourceused": 101382144,
   *      "resourcewasted": 15993417,
   *      "severity": "Moderate",
   *      "scheduler": "azkaban",
   *      "tasksseverity": [
   *    {
   *      "severity": "Moderate",
   *        "count": 1
   *    }
   *    ]
   *  }
   *  ]
   *}
   * </pre>
   **/
  public static Result restJobSummariesForUser(String username) {

    JsonArray jobSummaryArray = new JsonArray();

    List<AppResult> results = null;
    if (username == null || username.isEmpty()) {
      results = getSchedulerApplications(MAX_APPLICATIONS_IN_WORKFLOW);
    } else {
      results = getSchedulerApplications(username, MAX_APPLICATIONS_IN_WORKFLOW);
    }

    Map<IdUrlPair, List<AppResult>> jobExecIdToJobsMap = ControllerUtil
        .limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.JOB_EXECUTION_ID), results.size(),
            MAX_JOB_LIMIT);

    for (IdUrlPair jobDefPair : jobExecIdToJobsMap.keySet()) {
      long totalJobMemoryUsed = 0L;
      long totalJobMemoryWasted = 0L;
      long totalJobDelay = 0L;
      long totalJobRuntime = 0L;
      long jobStartTime = Long.MAX_VALUE;
      long jobEndTime = 0;
      Severity jobSeverity = Severity.NONE;
      String jobType = null;
      String jobId = jobDefPair.getId();
      String jobName = "";
      String user = null;
      String queueName = "";
      String scheduler = "";
      String jobDefId = "";
      String jobExecId = "";

      Map<Severity, Long> applicationSeverityCount = new HashMap<Severity, Long>();

      for (AppResult application : jobExecIdToJobsMap.get(jobDefPair)) {

        totalJobMemoryUsed += application.resourceUsed;
        totalJobMemoryWasted += application.resourceWasted;

        jobType = application.jobType;
        jobName = application.jobName;
        jobDefId = application.jobDefId;
        jobExecId = application.jobExecId;

        queueName = application.queueName;
        scheduler = application.scheduler;

        if (application.startTime < jobStartTime) {
          jobStartTime = application.startTime;
        }

        if (application.finishTime > jobEndTime) {
          jobEndTime = application.finishTime;
        }

        if (application.severity.getValue() > jobSeverity.getValue()) {
          jobSeverity = application.severity;
        }

        if (applicationSeverityCount.containsKey(application.severity)) {
          applicationSeverityCount.put(application.severity, applicationSeverityCount.get(application.severity) + 1L);
        } else {
          applicationSeverityCount.put(application.severity, 1L);
        }

        user = application.username;
      }

      JsonArray applicationSeverity = new JsonArray();
      List<Severity> keys = getSortedSeverityKeys(applicationSeverityCount.keySet());
      for (Severity key : keys) {
        JsonObject severityObject = new JsonObject();
        severityObject.addProperty(JsonKeys.SEVERITY, key.getText());
        severityObject.addProperty(JsonKeys.COUNT, applicationSeverityCount.get(key));
        applicationSeverity.add(severityObject);
      }

      totalJobDelay = Utils.getTotalWaittime(jobExecIdToJobsMap.get(jobDefPair));
      totalJobRuntime = Utils.getTotalRuntime(jobExecIdToJobsMap.get(jobDefPair));

      JsonObject jobObject = new JsonObject();
      jobObject.addProperty(JsonKeys.ID, jobId);
      jobObject.addProperty(JsonKeys.JOB_NAME, jobName);
      jobObject.addProperty(JsonKeys.JOB_TYPE, jobType);
      jobObject.addProperty(JsonKeys.USERNAME, user);
      jobObject.addProperty(JsonKeys.START_TIME, jobStartTime);
      jobObject.addProperty(JsonKeys.FINISH_TIME, jobEndTime);
      jobObject.addProperty(JsonKeys.RUNTIME, totalJobRuntime);
      jobObject.addProperty(JsonKeys.WAITTIME, totalJobDelay);
      jobObject.addProperty(JsonKeys.RESOURCE_USED, totalJobMemoryUsed);
      jobObject.addProperty(JsonKeys.RESOURCE_WASTED, totalJobMemoryWasted);
      jobObject.addProperty(JsonKeys.QUEUE, queueName);
      jobObject.addProperty(JsonKeys.SCHEDULER, scheduler);
      jobObject.addProperty(JsonKeys.SEVERITY, jobSeverity.getText());
      jobObject.addProperty(JsonKeys.JOB_DEF_ID, jobDefId);
      jobObject.addProperty(JsonKeys.JOB_EXEC_ID, jobExecId);

      jobObject.add(JsonKeys.TASKS_SEVERITY, applicationSeverity);

      jobSummaryArray.add(jobObject);
    }

    JsonArray sortedJobSummaryArray = getSortedJsonArrayByFinishTime(jobSummaryArray);

    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.JOB_SUMMARIES, sortedJobSummaryArray);
    return ok(new Gson().toJson(parent));
  }

  /**
   * This method returns the workflow-summaries json response
   * @param username The username for which workflow-summaries must be returned
   * @return The json response of the workflow-summaries for the given user
   * Response data:
   * <pre>
   *{
   *  "workflow-summaries": [
   *  {
   *    "id": "http://workflow-id",
   *      "username": "search",
   *      "starttime": 1468818098875,
   *      "finishtime": 1468819946683,
   *      "runtime": 1855532,
   *      "waittime": 365368,
   *      "resourceused": 3306438656,
   *      "resourcewasted": 516978829,
   *      "severity": "Severe",
   *      "jobsseverity": [
   *    {
   *      "severity": "Severe",
   *        "count": 26
   *    },
   *    {
   *      "severity": "Moderate",
   *        "count": 3
   *    },
   *    {
   *      "severity": "Low",
   *        "count": 1
   *    },
   *    {
   *      "severity": "None",
   *        "count": 16
   *    }
   *    ]
   *  }
   *  ]
   *}
   * </pre>
   */
  public static Result restWorkflowSummariesForUser(String username) {
    JsonArray workflowSummaryArray = new JsonArray();
    List<AppResult> results = null;
    if (username == null || username.isEmpty()) {
      results = getSchedulerApplications(MAX_APPLICATIONS_IN_WORKFLOW);
    } else {
      results = getSchedulerApplications(username, MAX_APPLICATIONS_IN_WORKFLOW);
    }

    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap = ControllerUtil
        .limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID),
            results.size(), MAX_FLOW_LIMIT);

    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());

    for (IdUrlPair flowExecPair : keyList) {

      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));

      Map<IdUrlPair, List<AppResult>> jobDefIdToJobsMap =
          ControllerUtil.groupJobs(mrJobsList, ControllerUtil.GroupBy.JOB_EXECUTION_ID);

      Map<Severity, Long> jobSeverityCount = new HashMap<Severity, Long>();
      long totalFlowMemoryUsed = 0;
      long totalFlowMemoryWasted = 0;
      long totalFlowDelay = 0;
      long totalFlowRuntime = 0;
      Severity flowSeverity = Severity.NONE;

      for (IdUrlPair jobDefPair : jobDefIdToJobsMap.keySet()) {

        Severity jobseverity = Severity.NONE;
        long totalJobMemoryUsed = 0;
        long totalJobMemoryWasted = 0;

        for (AppResult job : jobDefIdToJobsMap.get(jobDefPair)) {

          totalJobMemoryUsed += job.resourceUsed;
          totalJobMemoryWasted += job.resourceWasted;

          if (job.severity.getValue() > jobseverity.getValue()) {
            jobseverity = job.severity;
          }
        }

        if (jobSeverityCount.containsKey(jobseverity)) {
          jobSeverityCount.put(jobseverity, jobSeverityCount.get(jobseverity) + 1);
        } else {
          jobSeverityCount.put(jobseverity, 1L);
        }

        if (jobseverity.getValue() > flowSeverity.getValue()) {
          flowSeverity = jobseverity;
        }

        totalFlowMemoryUsed += totalJobMemoryUsed;
        totalFlowMemoryWasted += totalJobMemoryWasted;
      }

      totalFlowDelay = Utils.getTotalWaittime(flowExecIdToJobsMap.get(flowExecPair));
      totalFlowRuntime = Utils.getTotalRuntime(flowExecIdToJobsMap.get(flowExecPair));

      JsonArray jobSeverity = new JsonArray();
      List<Severity> keys = getSortedSeverityKeys(jobSeverityCount.keySet());
      for (Severity key : keys) {
        JsonObject severityObject = new JsonObject();
        severityObject.addProperty(JsonKeys.SEVERITY, key.getText());
        severityObject.addProperty(JsonKeys.COUNT, jobSeverityCount.get(key));
        jobSeverity.add(severityObject);
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty(JsonKeys.ID, mrJobsList.get(0).flowExecId);
      dataset.addProperty(JsonKeys.USERNAME, mrJobsList.get(0).username);
      dataset.addProperty(JsonKeys.START_TIME, mrJobsList.get(0).startTime);
      dataset.addProperty(JsonKeys.FINISH_TIME, mrJobsList.get(mrJobsList.size() - 1).finishTime);
      dataset.addProperty(JsonKeys.RUNTIME, totalFlowRuntime);
      dataset.addProperty(JsonKeys.WAITTIME, totalFlowDelay);
      dataset.addProperty(JsonKeys.RESOURCE_USED, totalFlowMemoryUsed);
      dataset.addProperty(JsonKeys.RESOURCE_WASTED, totalFlowMemoryWasted);
      dataset.addProperty(JsonKeys.QUEUE, mrJobsList.get(0).queueName);
      dataset.addProperty(JsonKeys.SEVERITY, flowSeverity.getText());
      dataset.addProperty(JsonKeys.SCHEDULER, mrJobsList.get(0).scheduler);
      dataset.addProperty(JsonKeys.FLOW_EXEC_ID, mrJobsList.get(0).flowExecId);
      dataset.addProperty(JsonKeys.FLOW_DEF_ID, mrJobsList.get(0).flowDefId);
      dataset.add(JsonKeys.JOBS_SEVERITY, jobSeverity);
      workflowSummaryArray.add(dataset);
    }
    JsonArray sortedWorkflowSummaryArray = getSortedJsonArrayByFinishTime(workflowSummaryArray);
    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.WORKFLOW_SUMMARIES, sortedWorkflowSummaryArray);
    return ok(new Gson().toJson(parent));
  }

  /**
   * This method returns the workflow response object based on the flow execution id
   * @param flowId The flow execution id for which the flow should be returned
   * @return Return the workflow detail based on the flow execution id
   *
   * response object:
   * <pre>
   * *{
   *    "workflows": {
   *    "id": "flowid",
   *        "username": "username",
   *        "starttime": 1471910835628,
   *        "finishtime": 1471911099238,
   *        "runtime": 263610,
   *        "waittime": 46234,
   *        "resourceused": 101382144,
   *        "resourcewasted": 15993417,
   *        "severity": "Moderate",
   *        "flowexecid": "flowexecid",
   *        "flowdefid": "flowdefid",
   *        "jobssummaries": [
   *          {
   *            "id": "jobid",
   *            "jobname": "jobname",
   *            "jobtype": "Pig",
   *            "username": "username",
   *            "starttime": 1471910835628,
   *            "finishtime": 1471911099238,
   *            "runtime": 263610,
   *            "waittime": 46234,
   *            "resourceused": 101382144,
   *            "resourcewasted": 15993417,
   *            "severity": "Moderate",
   *            "tasksseverity": [
   *              {
   *                "severity": "Moderate",
   *                "count": 1
   *              }
   *             ]
   *        }
   *      ],
   *        "jobsseverity": [
   *          {
   *            "severity": "Moderate",
   *            "count": 1
   *          }
   *      ]
   *  }
   *}
   * </pre>
   */
  public static Result restWorkflowFromFlowId(String flowId) {

    if (flowId == null || flowId.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.WORKFLOWS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    JsonArray jobSeverityArray = new JsonArray();
    JsonArray jobSummaryArray = new JsonArray();
    JsonObject data = new JsonObject();

    String flowExecId = flowId;
    String username = "";
    long totalFlowResourceUsed = 0;
    long totalFlowResourceWasted = 0;
    long totalFlowRuntime = 0;
    long totalFlowDelay = 0;
    Severity flowSeverity = Severity.NONE;
    long flowStartTime = Long.MAX_VALUE;
    long flowEndTime = 0;
    String flowDefinitionId = "";
    Map<Severity, Long> jobSeverityCount = new HashMap<Severity, Long>();
    String wfQueueName = "";
    String wfSchedulerName = "";

    List<AppResult> results = getRestFlowResultsFromFlowExecutionId(flowId);

    if (results.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.WORKFLOWS, data);
      return notFound(new Gson().toJson(parent));
    }

    Map<IdUrlPair, List<AppResult>> jobExecIdToJobsMap =
        ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.JOB_EXECUTION_ID);

    for (IdUrlPair jobDefPair : jobExecIdToJobsMap.keySet()) {
      long totalJobMemoryUsed = 0;
      long totalJobMemoryWasted = 0;
      long totalJobDelay = 0;
      long totalJobRuntime = 0;
      long jobStartTime = Long.MAX_VALUE;
      long jobEndTime = 0;
      Severity jobSeverity = Severity.NONE;
      String jobType = null;
      String jobId = jobDefPair.getId();
      String jobName = "";
      String queueName = "";
      String schedulerName = "";

      Map<Severity, Long> taskSeverityCount = new HashMap<Severity, Long>();

      for (AppResult task : jobExecIdToJobsMap.get(jobDefPair)) {
        totalJobMemoryUsed += task.resourceUsed;
        totalJobMemoryWasted += task.resourceWasted;
        username = task.username;
        jobType = task.jobType;
        jobName = task.jobName;
        flowDefinitionId = task.flowDefId;
        queueName = task.queueName;
        schedulerName = task.scheduler;

        if (task.startTime < jobStartTime) {
          jobStartTime = task.startTime;
        }

        if (task.finishTime > jobEndTime) {
          jobEndTime = task.finishTime;
        }
        if (task.severity.getValue() > jobSeverity.getValue()) {
          jobSeverity = task.severity;
        }

        if (taskSeverityCount.containsKey(task.severity)) {
          taskSeverityCount.put(task.severity, taskSeverityCount.get(task.severity) + 1L);
        } else {
          taskSeverityCount.put(task.severity, 1L);
        }
      } // task scope ends here

      if (jobSeverityCount.containsKey(jobSeverity)) {
        jobSeverityCount.put(jobSeverity, jobSeverityCount.get(jobSeverity) + 1L);
      } else {
        jobSeverityCount.put(jobSeverity, 1L);
      }

      JsonArray taskSeverity = new JsonArray();
      List<Severity> keys = getSortedSeverityKeys(taskSeverityCount.keySet());
      for (Severity key : keys) {
        JsonObject severityObject = new JsonObject();
        severityObject.addProperty(JsonKeys.SEVERITY, key.getText());
        severityObject.addProperty(JsonKeys.COUNT, taskSeverityCount.get(key));
        taskSeverity.add(severityObject);
      }

      wfQueueName = queueName;
      wfSchedulerName = schedulerName;
      totalJobDelay = Utils.getTotalWaittime(jobExecIdToJobsMap.get(jobDefPair));
      totalJobRuntime = Utils.getTotalRuntime(jobExecIdToJobsMap.get(jobDefPair));

      JsonObject jobObject = new JsonObject();
      jobObject.addProperty(JsonKeys.ID, jobId);
      jobObject.addProperty(JsonKeys.JOB_NAME, jobName);
      jobObject.addProperty(JsonKeys.JOB_TYPE, jobType);
      jobObject.addProperty(JsonKeys.USERNAME, username);
      jobObject.addProperty(JsonKeys.START_TIME, jobStartTime);
      jobObject.addProperty(JsonKeys.FINISH_TIME, jobEndTime);
      jobObject.addProperty(JsonKeys.RUNTIME, totalJobRuntime);
      jobObject.addProperty(JsonKeys.WAITTIME, totalJobDelay);
      jobObject.addProperty(JsonKeys.RESOURCE_USED, totalJobMemoryUsed);
      jobObject.addProperty(JsonKeys.RESOURCE_WASTED, totalJobMemoryWasted);
      jobObject.addProperty(JsonKeys.QUEUE, queueName);
      jobObject.addProperty(JsonKeys.SCHEDULER, schedulerName);
      jobObject.addProperty(JsonKeys.SEVERITY, jobSeverity.getText());
      jobObject.add(JsonKeys.TASKS_SEVERITY, taskSeverity);

      jobSummaryArray.add(jobObject);

      totalFlowResourceUsed += totalJobMemoryUsed;
      totalFlowResourceWasted += totalJobMemoryWasted;
      if (jobSeverity.getValue() > flowSeverity.getValue()) {
        flowSeverity = jobSeverity;
      }

      if (flowStartTime > jobStartTime) {
        flowStartTime = jobStartTime;
      }

      if (flowEndTime < jobEndTime) {
        flowEndTime = jobEndTime;
      }
    }// job map scope ends here

    List<Severity> keys = getSortedSeverityKeys(jobSeverityCount.keySet());
    for (Severity key : keys) {
      JsonObject severityObject = new JsonObject();
      severityObject.addProperty(JsonKeys.SEVERITY, key.getText());
      severityObject.addProperty(JsonKeys.COUNT, jobSeverityCount.get(key));
      jobSeverityArray.add(severityObject);
    }

    totalFlowDelay = Utils.getTotalWaittime(results);
    totalFlowRuntime = Utils.getTotalRuntime(results);
    data.addProperty(JsonKeys.ID, flowExecId);
    data.addProperty(JsonKeys.USERNAME, username);
    data.addProperty(JsonKeys.START_TIME, flowStartTime);
    data.addProperty(JsonKeys.FINISH_TIME, flowEndTime);
    data.addProperty(JsonKeys.RUNTIME, totalFlowRuntime);
    data.addProperty(JsonKeys.WAITTIME, totalFlowDelay);
    data.addProperty(JsonKeys.RESOURCE_USED, totalFlowResourceUsed);
    data.addProperty(JsonKeys.RESOURCE_WASTED, totalFlowResourceWasted);
    data.addProperty(JsonKeys.SEVERITY, flowSeverity.getText());
    data.addProperty(JsonKeys.FLOW_EXEC_ID, flowExecId);
    data.addProperty(JsonKeys.FLOW_DEF_ID, flowDefinitionId);
    data.addProperty(JsonKeys.QUEUE, wfQueueName);
    data.addProperty(JsonKeys.SCHEDULER, wfSchedulerName);
    data.add(JsonKeys.JOBSSUMMARIES, jobSummaryArray);
    data.add(JsonKeys.JOBS_SEVERITY, jobSeverityArray);
    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.WORKFLOWS, data);
    return ok(new Gson().toJson(parent));
  }

  /**
   *
   * @param jobId
   * @return
   * <pre>
   **{
   *  "jobs": {
   *    "id": "jobid",
   *    "username": "username",
   *    "jobname": "jobname",
   *    "jobtype": "Pig",
   *    "starttime": 1471910835628,
   *    "finishtime": 1471911099238,
   *    "runtime": 263610,
   *    "waittime": 46234,
   *    "resourceused": 101382144,
   *    "resourcewasted": 15993417,
   *    "severity": "Moderate",
   *    "jobexecid": "jobexecid",
   *    "jobdefid": "jobdefid",
   *    "flowexecid": "flowexecid",
   *    "flowdefid": "flowdefid",
   *    "taskssummaries": [
   *      {
   *        "id": "application_id",
   *        "username": "username",
   *        "starttime": 1471910835628,
   *        "finishtime": 1471911099238,
   *        "runtime": 263610,
   *        "waittime": 46234,
   *        "resourceused": 101382144,
   *        "resourcewasted": 15993417,
   *        "severity": "Moderate",
   *        "heuristicsummary": [
   *          {
   *            "name": "Mapper Data Skew",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Mapper GC",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Mapper Time",
   *            "severity": "Moderate"
   *          },
   *          {
   *            "name": "Mapper Speed",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Mapper Spill",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Mapper Memory",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Reducer Data Skew",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Reducer GC",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Reducer Time",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Reducer Memory",
   *            "severity": "None"
   *          },
   *          {
   *            "name": "Shuffle & Sort",
   *            "severity": "Low"
   *          }
   *        ]
   *      }
   *    ],
   *    "tasksseverity": [
   *      {
   *        "severity": "Moderate",
   *        "count": 1
   *      }
   *    ]
   *  }
   *}
   *
   * </pre>
   */
  public static Result restJobFromJobId(String jobid) {

    if (jobid == null || jobid.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.JOBS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    JsonArray taskSummaryArray = new JsonArray();

    String jobDefID = jobid;
    long jobResourceUsed = 0;
    long jobResourceWasted = 0;
    long jobRuntime = 0;
    long jobDelay = 0;
    Severity jobSeverity = Severity.NONE;
    long jobStartTime = Long.MAX_VALUE;
    long jobEndTime = 0;
    String username = "";
    String jobtype = "";
    String jobExecutionId = "";
    String jobDefinitionId = "";
    String flowExecutionId = "";
    String flowDefinitionId = "";
    String jobname = "";
    String queueName = "";
    String scheduler = "";

    List<AppResult> results = getRestJobResultsFromJobExecutionId(jobid);

    if (results.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.JOBS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    Map<Severity, Long> taskSeverityCount = new HashMap<Severity, Long>();

    for (AppResult task : results) {
      username = task.username;
      jobtype = task.jobType;
      jobname = task.jobName;
      jobExecutionId = task.jobExecId;
      jobDefinitionId = task.jobDefId;
      flowExecutionId = task.flowExecId;
      flowDefinitionId = task.flowDefId;
      queueName = task.queueName;
      scheduler = task.scheduler;

      JsonObject taskObject = new JsonObject();
      JsonArray heuristicsArray = new JsonArray();
      List<AppHeuristicResult> appHeuristicResult = task.yarnAppHeuristicResults;
      for (AppHeuristicResult heuristic : appHeuristicResult) {
        JsonObject heuristicObject = new JsonObject();
        heuristicObject.addProperty(JsonKeys.NAME, heuristic.heuristicName);
        heuristicObject.addProperty(JsonKeys.SEVERITY, heuristic.severity.getText());
        heuristicsArray.add(heuristicObject);
      }

      if (task.severity.getValue() > jobSeverity.getValue()) {
        jobSeverity = task.severity;
      }

      if (taskSeverityCount.containsKey(task.severity)) {
        taskSeverityCount.put(task.severity, taskSeverityCount.get(task.severity) + 1L);
      } else {
        taskSeverityCount.put(task.severity, 1L);
      }

      taskObject.addProperty(JsonKeys.ID, task.id);
      taskObject.addProperty(JsonKeys.USERNAME, task.username);
      taskObject.addProperty(JsonKeys.START_TIME, task.startTime);
      taskObject.addProperty(JsonKeys.FINISH_TIME, task.finishTime);
      taskObject.addProperty(JsonKeys.RUNTIME, task.finishTime - task.startTime);
      taskObject.addProperty(JsonKeys.WAITTIME, task.totalDelay);
      taskObject.addProperty(JsonKeys.RESOURCE_USED, task.resourceUsed);
      taskObject.addProperty(JsonKeys.RESOURCE_WASTED, task.resourceWasted);
      taskObject.addProperty(JsonKeys.SEVERITY, task.severity.getText());
      taskObject.addProperty(JsonKeys.QUEUE, task.queueName);
      taskObject.add(JsonKeys.HEURISTICS_SUMMARY, heuristicsArray);
      taskSummaryArray.add(taskObject);

      jobResourceUsed += task.resourceUsed;
      jobResourceWasted += task.resourceWasted;

      if (jobSeverity.getValue() < task.severity.getValue()) {
        jobSeverity = task.severity;
      }
      if (jobStartTime > task.startTime) {
        jobStartTime = task.startTime;
      }

      if (jobEndTime < task.finishTime) {
        jobEndTime = task.finishTime;
      }
    }

    JsonArray taskSeverity = new JsonArray();
    List<Severity> keys = getSortedSeverityKeys(taskSeverityCount.keySet());
    for (Severity key : keys) {
      JsonObject severityObject = new JsonObject();
      severityObject.addProperty(JsonKeys.SEVERITY, key.getText());
      severityObject.addProperty(JsonKeys.COUNT, taskSeverityCount.get(key));
      taskSeverity.add(severityObject);
    }

    jobRuntime = Utils.getTotalRuntime(results);
    jobDelay = Utils.getTotalWaittime(results);
    JsonObject data = new JsonObject();
    data.addProperty(JsonKeys.ID, jobDefID);
    data.addProperty(JsonKeys.USERNAME, username);
    data.addProperty(JsonKeys.JOB_NAME, jobname);
    data.addProperty(JsonKeys.JOB_TYPE, jobtype);
    data.addProperty(JsonKeys.START_TIME, jobStartTime);
    data.addProperty(JsonKeys.FINISH_TIME, jobEndTime);
    data.addProperty(JsonKeys.RUNTIME, jobRuntime);
    data.addProperty(JsonKeys.WAITTIME, jobDelay);
    data.addProperty(JsonKeys.RESOURCE_USED, jobResourceUsed);
    data.addProperty(JsonKeys.RESOURCE_WASTED, jobResourceWasted);
    data.addProperty(JsonKeys.SEVERITY, jobSeverity.getText());
    data.addProperty(JsonKeys.JOB_EXEC_ID, jobExecutionId);
    data.addProperty(JsonKeys.JOB_DEF_ID, jobDefinitionId);
    data.addProperty(JsonKeys.FLOW_EXEC_ID, flowExecutionId);
    data.addProperty(JsonKeys.FLOW_DEF_ID, flowDefinitionId);
    data.addProperty(JsonKeys.QUEUE, queueName);
    data.addProperty(JsonKeys.SCHEDULER, scheduler);
    data.add(JsonKeys.TASKS_SUMMARIES, taskSummaryArray);
    data.add(JsonKeys.TASKS_SEVERITY, taskSeverity);

    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.JOBS, data);
    return ok(new Gson().toJson(parent));
  }

  /**
   * @param applicationId
   * @return
   * <pre>
   *  {
   *  "applications": {
   *    "id": "application_id",
   *    "username": "username",
   *    "jobtype": "Pig",
   *    "mapreducejobname": "mapreducejobname",
   *    "starttime": 1471910835628,
   *    "finishtime": 1471911099238,
   *    "runtime": 263610,
   *    "waittime": 46234,
   *    "resourceused": 101382144,
   *    "resourcewasted": 15993417,
   *    "severity": "Moderate",
   *    "trackingurl": "jobtracker_address",
   *    "jobexecid": "jobexecutionid",
   *    "jobdefid": "jobdefinitionid",
   *    "flowexeid": "flowexecutionid",
   *    "flowdefid": "flowdefinitionid",
   *    "yarnappheuristicresults": [
   *      {
   *        "name": "Mapper Data Skew",
   *        "severity": "None",
   *        "details": [
   *          {
   *            "name": "Group A",
   *            "value": "236 tasks @ 506 MB avg"
   *          },
   *          {
   *            "name": "Group B",
   *            "value": "234 tasks @ 507 MB avg"
   *          },
   *          {
   *            "name": "Number of tasks",
   *            "value": "470"
   *          }
   *        ]
   *      },
   *      {
   *        "name": "Mapper GC",
   *        "severity": "None",
   *        "details": [
   *          {
   *            "name": "Avg task CPU time (ms)",
   *            "value": "111717"
   *          },
   *          {
   *            "name": "Avg task GC time (ms)",
   *            "value": "3197"
   *          },
   *          {
   *            "name": "Avg task runtime (ms)",
   *            "value": "105633"
   *          },
   *          {
   *            "name": "Number of tasks",
   *            "value": "470"
   *          },
   *          {
   *            "name": "Task GC\/CPU ratio",
   *            "value": "0.028616951762041588"
   *          }
   *        ]
   *      }..
   *    ]
   *  }
   *}
   * </pre>
   */
  public static Result restApplicationFromApplicationId(String applicationid) {

    if (applicationid == null || applicationid.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.APPLICATIONS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    if (applicationid.startsWith("job")) {
      applicationid = applicationid.replaceAll("job", "application");
    }

    JsonObject applicationObject = new JsonObject();
    JsonArray heuristicsArray = new JsonArray();

    AppResult result = getAppResultFromApplicationId(applicationid);

    if (result == null) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.APPLICATIONS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    for (AppHeuristicResult appHeuristicResult : result.yarnAppHeuristicResults) {
      JsonArray detailsArray = new JsonArray();
      JsonObject heuristicResultObject = new JsonObject();
      for (AppHeuristicResultDetails details : appHeuristicResult.yarnAppHeuristicResultDetails) {
        JsonObject detailsObject = new JsonObject();
        detailsObject.addProperty(JsonKeys.NAME, details.name);
        detailsObject.addProperty(JsonKeys.VALUE, details.value);
        detailsObject.addProperty(JsonKeys.DETAILS, details.details);
        detailsArray.add(detailsObject);
      }
      heuristicResultObject.addProperty(JsonKeys.NAME, appHeuristicResult.heuristicName);
      heuristicResultObject.addProperty(JsonKeys.SEVERITY, appHeuristicResult.severity.getText());
      heuristicResultObject.add(JsonKeys.DETAILS, detailsArray);
      heuristicsArray.add(heuristicResultObject);
    }

    applicationObject.addProperty(JsonKeys.ID, result.id);
    applicationObject.addProperty(JsonKeys.USERNAME, result.username);
    applicationObject.addProperty(JsonKeys.JOB_TYPE, result.jobType);
    applicationObject.addProperty(JsonKeys.MAPREDUCE_JOB_NAME, result.jobName);
    applicationObject.addProperty(JsonKeys.START_TIME, result.startTime);
    applicationObject.addProperty(JsonKeys.FINISH_TIME, result.finishTime);
    applicationObject.addProperty(JsonKeys.RUNTIME, result.finishTime - result.startTime);
    applicationObject.addProperty(JsonKeys.WAITTIME, result.totalDelay);
    applicationObject.addProperty(JsonKeys.RESOURCE_USED, result.resourceUsed);
    applicationObject.addProperty(JsonKeys.RESOURCE_WASTED, result.resourceWasted);
    applicationObject.addProperty(JsonKeys.SEVERITY, result.severity.getText());
    applicationObject.addProperty(JsonKeys.TRACKING_URL, result.trackingUrl);
    applicationObject.addProperty(JsonKeys.JOB_EXEC_ID, result.jobExecId);
    applicationObject.addProperty(JsonKeys.JOB_DEF_ID, result.jobDefId);
    applicationObject.addProperty(JsonKeys.FLOW_EXEC_ID, result.flowExecId);
    applicationObject.addProperty(JsonKeys.FLOW_DEF_ID, result.flowDefId);
    applicationObject.addProperty(JsonKeys.QUEUE, result.queueName);

    applicationObject.add(JsonKeys.YARN_APP_HEURISTIC_RESULTS, heuristicsArray);

    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.APPLICATIONS, applicationObject);
    return ok(new Gson().toJson(parent));
  }

  /**
   * This returns the rest search options which are filled in the forms for the search page.
   * @return Returns the json object which should be filled in the search form.
   * return object:
   * <pre>
   *  *{
   *  "search-options": {
   *    "jobcategory": [
   *      {
   *        "name": "SPARK",
   *        "jobtypes": [
   *          {
   *            "name": "Spark"
   *          }
   *        ],
   *        "heuristics": [
   *          {
   *            "name": "Spark Configuration Best Practice"
   *          },
   *          {
   *            "name": "Spark Memory Limit"
   *          },
   *          {
   *            "name": "Spark Stage Runtime"
   *          },
   *          {
   *            "name": "Spark Job Runtime"
   *          },
   *          {
   *            "name": "Spark Executor Load Balance"
   *          },
   *          {
   *            "name": "Spark Event Log Limit"
   *          }
   *        ]
   *      },
   *      {
   *        "name": "MAPREDUCE",
   *        "jobtypes": [
   *          {
   *            "name": "Pig"
   *          },
   *          {
   *            "name": "Hive"
   *          },
   *          {
   *            "name": "Cascading"
   *          },
   *          {
   *            "name": "Voldemort"
   *          },
   *          {
   *            "name": "Kafka"
   *          },
   *          {
   *            "name": "HadoopJava"
   *          }
   *        ],
   *        "heuristics": [
   *          {
   *            "name": "Mapper Data Skew"
   *          },
   *          {
   *            "name": "Mapper GC"
   *          },
   *          {
   *            "name": "Mapper Time"
   *          },
   *          {
   *            "name": "Mapper Speed"
   *          },
   *          {
   *            "name": "Mapper Spill"
   *          },
   *          {
   *            "name": "Mapper Memory"
   *          },
   *          {
   *            "name": "Reducer Data Skew"
   *          },
   *          {
   *            "name": "Reducer GC"
   *          },
   *          {
   *            "name": "Reducer Time"
   *          },
   *          {
   *            "name": "Reducer Memory"
   *          },
   *          {
   *            "name": "Shuffle & Sort"
   *          },
   *          {
   *            "name": "Exception"
   *          }
   *        ]
   *      }
   *    ],
   *    "severities": [
   *      {
   *        "name": "Critical",
   *        "value": 4
   *      },
   *      {
   *        "name": "Severe",
   *        "value": 3
   *      },
   *      {
   *        "name": "Moderate",
   *        "value": 2
   *      },
   *      {
   *        "name": "Low",
   *        "value": 1
   *      },
   *      {
   *        "name": "None",
   *        "value": 0
   *      }
   *    ],
   *    "id": "search"
   *  }
   *}
   * </pre>
   */
  public static Result restSearchOptions() {
    JsonObject searchOptions = new JsonObject();
    JsonArray jobCategory = new JsonArray();
    JsonArray severities = new JsonArray();

    Map<ApplicationType, List<JobType>> applicationTypeListMap = ElephantContext.instance().getAppTypeToJobTypes();

    for (ApplicationType key : applicationTypeListMap.keySet()) {
      JsonObject applicationType = new JsonObject();
      JsonArray jobTypes = new JsonArray();
      JsonArray heuristics = new JsonArray();

      for (JobType jobtype : applicationTypeListMap.get(key)) {
        JsonObject jobTypeNode = new JsonObject();
        jobTypeNode.addProperty(JsonKeys.NAME, jobtype.getName());
        jobTypes.add(jobTypeNode);
      }

      for (Heuristic heuristic : ElephantContext.instance().getHeuristicsForApplicationType(key)) {
        JsonObject heuristicNode = new JsonObject();
        heuristicNode.addProperty(JsonKeys.NAME, heuristic.getHeuristicConfData().getHeuristicName());
        heuristics.add(heuristicNode);
      }

      applicationType.addProperty(JsonKeys.NAME, key.getName());
      applicationType.add(JsonKeys.JOB_TYPES, jobTypes);
      applicationType.add(JsonKeys.HEURISTICS, heuristics);
      jobCategory.add(applicationType);
    }

    for (Severity severity : Severity.values()) {
      JsonObject severityObject = new JsonObject();
      severityObject.addProperty(JsonKeys.NAME, severity.getText());
      severityObject.addProperty(JsonKeys.VALUE, severity.getValue());
      severities.add(severityObject);
    }

    searchOptions.add(JsonKeys.JOB_CATEGORY, jobCategory);
    searchOptions.add(JsonKeys.SEVERITIES, severities);
    searchOptions.addProperty(JsonKeys.ID, "search");
    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.SEARCH_OPTS, searchOptions);
    return ok(new Gson().toJson(parent));
  }

  /**
   * Returns the search results for the given query
   * @return
   * JsonObject:
   *
   * <pre>
   *   {
   *         search-results: {
   *         id: "id"
   *         start: 0,
   *         end: 20,
   *         total: 0,
   *         summaries: [
   *                  {
   *                    application_summary_object
   *                  }
   *                ]
   *          }
   *  }
   * </pre>
   */
  public static Result search() {
    DynamicForm form = Form.form().bindFromRequest(request());
    JsonObject parent = new JsonObject();

    int offset = SEARCH_DEFAULT_PAGE_OFFSET;
    int limit = SEARCH_DEFAULT_PAGE_LIMIT;
    int end = 0;
    int total = 0;

    if (form.get("offset") != null && form.get("offset") != "") {
      offset = Integer.valueOf(form.get("offset"));
    }

    if (form.get("limit") != null && form.get("limit") != "") {
      limit = Integer.valueOf(form.get("limit"));
    }

    if (offset < 0) {
      offset = 0;
    }

    if (limit > SEARCH_APPLICATION_MAX_OFFSET) {
      limit = SEARCH_APPLICATION_MAX_OFFSET;
    } else if (limit <= 0) {
      return ok(new Gson().toJson(parent));
    }

    Query<AppResult> query =
        Application.generateSearchQuery(AppResult.getSearchFields(), Application.getSearchParams());

    total = query.findRowCount();

    if (offset > total) {
      offset = total;
    }

    List<AppResult> results = query.setFirstRow(offset).setMaxRows(limit)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields()).findList();

    end = offset + results.size();

    JsonArray applicationSummaryArray = new JsonArray();

    for (AppResult application : results) {
      JsonObject applicationObject = new JsonObject();
      JsonArray heuristicsArray = new JsonArray();
      List<AppHeuristicResult> appHeuristicResult = application.yarnAppHeuristicResults;

      for (AppHeuristicResult heuristic : appHeuristicResult) {
        JsonObject heuristicObject = new JsonObject();
        heuristicObject.addProperty(JsonKeys.NAME, heuristic.heuristicName);
        heuristicObject.addProperty(JsonKeys.SEVERITY, heuristic.severity.getText());
        heuristicsArray.add(heuristicObject);
      }

      applicationObject.addProperty(JsonKeys.ID, application.id);
      applicationObject.addProperty(JsonKeys.USERNAME, application.username);
      applicationObject.addProperty(JsonKeys.START_TIME, application.startTime);
      applicationObject.addProperty(JsonKeys.FINISH_TIME, application.finishTime);
      applicationObject.addProperty(JsonKeys.RUNTIME, application.finishTime - application.startTime);
      applicationObject.addProperty(JsonKeys.WAITTIME, application.totalDelay);
      applicationObject.addProperty(JsonKeys.RESOURCE_USED, application.resourceUsed);
      applicationObject.addProperty(JsonKeys.RESOURCE_WASTED, application.resourceWasted);
      applicationObject.addProperty(JsonKeys.SEVERITY, application.severity.getText());
      applicationObject.addProperty(JsonKeys.QUEUE, application.queueName);

      applicationObject.add(JsonKeys.HEURISTICS_SUMMARY, heuristicsArray);
      applicationSummaryArray.add(applicationObject);
    }

    JsonObject searchResults = new JsonObject();
    searchResults.addProperty(JsonKeys.ID, query.toString());
    searchResults.addProperty(JsonKeys.START, offset);
    searchResults.addProperty(JsonKeys.END, end);
    searchResults.addProperty(JsonKeys.TOTAL, total);
    searchResults.add(JsonKeys.SUMMARIES, applicationSummaryArray);
    parent.add(JsonKeys.SEARCH_RESULTS, searchResults);
    return ok(new Gson().toJson(parent));
  }

  /**
   * Returns the filter parameters for the user summary
   * @return The filter parameters for the user summary
   */
  public static Map<String, String> getFilterParamsForUserSummary() {
    DynamicForm form = Form.form().bindFromRequest(request());
    Map<String, String> filterParams = new HashMap<String, String>();
    filterParams.put(Application.FINISHED_TIME_BEGIN, form.get(Application.FINISHED_TIME_BEGIN));
    filterParams.put(Application.FINISHED_TIME_END, form.get(Application.FINISHED_TIME_END));
    filterParams.put(Application.STARTED_TIME_BEGIN, form.get(Application.STARTED_TIME_BEGIN));
    filterParams.put(Application.STARTED_TIME_END, form.get(Application.STARTED_TIME_END));
    return filterParams;
  }

  /**
   *  The rest interface to return the results for a particular user. When the date is not specified, it returns the result
   *  for the last seven days.
   * @return The json object of the form:
   * result:
   * * {
   *   "user-details": {
   *     "id": "user",
   *     "totalapplications": 3,
   *     "totaljobs": 3,
   *     "totalworkflows": 3,
   *     "resourceused": 101394532,
   *     "resourcewasted": 15999828,
   *     "runtime": 312283,
   *     "waittime": 46234,
   *     "start": 0,
   *     "end": 3,
   *     "total": 3,
   *     "summaries": [
   *       {
   *         "id": "application_12432132131",
   *         "username": "user",
   *         "starttime": 1477389986871,
   *         "finishtime": 1477390004463,
   *         "runtime": 17592,
   *         "waittime": 0,
   *         "resourceused": 12288,
   *         "resourcewasted": 6360,
   *         "severity": "Critical",
   *         "queue": "spark_default",
   *         "heuristicsummary": [
   *           {
   *             "name": "Spark Configuration Best Practice",
   *             "severity": "None"
   *           },
   *           {
   *             "name": "Spark Memory Limit",
   *             "severity": "None"
   *           },
   *           {
   *             "name": "Spark Stage Runtime",
   *             "severity": "Low"
   *           },
   *           {
   *             "name": "Spark Job Runtime",
   *             "severity": "Low"
   *           },
   *           {
   *             "name": "Spark Executor Load Balance",
   *             "severity": "Critical"
   *           },
   *           {
   *             "name": "Spark Event Log Limit",
   *             "severity": "None"
   *           }
   *         ]
   *       }
   *     }
   *   }
   *
   */
  public static Result restGetUsersSummaryStats() {
    DynamicForm form = Form.form().bindFromRequest(request());
    int offset = SEARCH_DEFAULT_PAGE_OFFSET;
    int limit = SEARCH_DEFAULT_PAGE_LIMIT;
    int end = 0;
    int total = 0;

    if (form.get("offset") != null && form.get("offset") != "") {
      offset = Integer.valueOf(form.get("offset"));
    }

    if (form.get("limit") != null && form.get("limit") != "") {
      limit = Integer.valueOf(form.get("limit"));
    }

    if (offset < 0) {
      offset = 0;
    }

    if (limit > SEARCH_APPLICATION_MAX_OFFSET) {
      limit = SEARCH_APPLICATION_MAX_OFFSET;
    } else if (limit <= 0) {
      return ok(new Gson().toJson(new JsonObject()));
    }

    String sortBy = "severity";
    boolean increasing = true;

    String usernameString = form.get("usernames");
    if (usernameString == null || usernameString.isEmpty()) {
      JsonObject parent = new JsonObject();
      parent.add(JsonKeys.USER_RESULTS, new JsonObject());
      return notFound(new Gson().toJson(parent));
    }

    List<String> usernames = Arrays.asList(usernameString.split(","));

    Map<String, String> filterParamsForUserSummary = getFilterParamsForUserSummary();

    if (form.get("sortKey") != null) {
      sortBy = form.get("sortKey");
    }

    if (form.get("increasing") != null) {
      increasing = Boolean.valueOf(form.get("increasing"));
    }

    JsonObject userResult = new JsonObject();
    List<String> usernameQueryList = new ArrayList<String>();
    for (int i = 0; i < usernames.size(); i++) {
      usernameQueryList.add("username=:user" + i);
    }

    String usernameQueryString = StringUtils.join(usernameQueryList, " or ");

    // by default, fetch data from last week
    String finishedTimeBegin = String.valueOf(System.currentTimeMillis() - DAY * 7); // week of data if not specified
    String finishedTimeEnd = String.valueOf(System.currentTimeMillis());

    if (Utils.isSet(filterParamsForUserSummary.get(Application.FINISHED_TIME_BEGIN))) {
      finishedTimeBegin = filterParamsForUserSummary.get(Application.FINISHED_TIME_BEGIN);
    }

    if (Utils.isSet(filterParamsForUserSummary.get(Application.FINISHED_TIME_END))) {
      finishedTimeEnd = filterParamsForUserSummary.get(Application.FINISHED_TIME_END);
    }

    StringBuilder timeFilterStringBuilder = new StringBuilder();
    if (finishedTimeBegin != null) {
      timeFilterStringBuilder.append("finish_time");
      timeFilterStringBuilder.append(">=");
      timeFilterStringBuilder.append(parseTime(String.valueOf(finishedTimeBegin)));
      if (finishedTimeEnd != null) {
        timeFilterStringBuilder.append(" and ");
      }
    }

    if (finishedTimeEnd != null) {
      timeFilterStringBuilder.append("finish_time");
      timeFilterStringBuilder.append("<=");
      timeFilterStringBuilder.append(parseTime(String.valueOf(finishedTimeEnd)));
    }

    String timeFilterString = timeFilterStringBuilder.toString();

    String sql;
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(
        "select count(id) as num_of_applications, count(distinct(job_exec_id)) as num_of_jobs, count(distinct(flow_exec_id)) as num_of_flows, sum(resource_used) as total_resource_used, sum(resource_wasted) as total_resource_wasted, sum(finish_time) - sum(start_time) as execution_time, sum(total_delay) as total_delay from yarn_app_result where");
    if (timeFilterString != null && !timeFilterString.isEmpty()) {
      sqlBuilder.append(" ( ");
      sqlBuilder.append(usernameQueryString);
      sqlBuilder.append(" ) and ");
      sqlBuilder.append(timeFilterString);
    } else {
      sqlBuilder.append(" ");
      sqlBuilder.append(usernameQueryString);
    }

    sql = sqlBuilder.toString();
    SqlQuery query = Ebean.createSqlQuery(sql);

    int iUserIndex = 0;
    for (String username : usernames) {
      query.setParameter("user" + iUserIndex, username);
      iUserIndex++;
    }

    SqlRow resultRow = query.findUnique();
    userResult.addProperty(JsonKeys.ID, usernameString);
    userResult.addProperty(JsonKeys.TOTAL_APPLICATIONS, resultRow.getLong("num_of_applications"));
    userResult.addProperty(JsonKeys.TOTAL_JOBS, resultRow.getLong("num_of_jobs"));
    userResult.addProperty(JsonKeys.TOTAL_WORKFLOWS, resultRow.getLong("num_of_flows"));
    userResult.addProperty(JsonKeys.RESOURCE_USED, resultRow.getLong("total_resource_used"));
    userResult.addProperty(JsonKeys.RESOURCE_WASTED, resultRow.getLong("total_resource_wasted"));
    userResult.addProperty(JsonKeys.RUNTIME, resultRow.getLong("execution_time"));
    userResult.addProperty(JsonKeys.WAITTIME, resultRow.getLong("total_delay"));

    Query<AppResult> userSummaryQuery =
        generateUserApplicationSummaryQuery(usernames, filterParamsForUserSummary, sortBy, increasing);

    total = userSummaryQuery.findRowCount();

    List<AppResult> results = userSummaryQuery.setFirstRow(offset).setMaxRows(limit)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields()).findList();

    end = offset + results.size();

    JsonArray applicationSummaryArray = new JsonArray();

    for (AppResult application : results) {
      JsonObject applicationObject = new JsonObject();
      JsonArray heuristicsArray = new JsonArray();
      List<AppHeuristicResult> appHeuristicResult = application.yarnAppHeuristicResults;

      for (AppHeuristicResult heuristic : appHeuristicResult) {
        JsonObject heuristicObject = new JsonObject();
        heuristicObject.addProperty(JsonKeys.NAME, heuristic.heuristicName);
        heuristicObject.addProperty(JsonKeys.SEVERITY, heuristic.severity.getText());
        heuristicsArray.add(heuristicObject);
      }

      applicationObject.addProperty(JsonKeys.ID, application.id);

      applicationObject.addProperty(JsonKeys.USERNAME, application.username);
      applicationObject.addProperty(JsonKeys.START_TIME, application.startTime);
      applicationObject.addProperty(JsonKeys.FINISH_TIME, application.finishTime);
      applicationObject.addProperty(JsonKeys.RUNTIME, application.finishTime - application.startTime);
      applicationObject.addProperty(JsonKeys.WAITTIME, application.totalDelay);
      applicationObject.addProperty(JsonKeys.RESOURCE_USED, application.resourceUsed);
      applicationObject.addProperty(JsonKeys.RESOURCE_WASTED, application.resourceWasted);
      applicationObject.addProperty(JsonKeys.SEVERITY, application.severity.getText());
      applicationObject.addProperty(JsonKeys.QUEUE, application.queueName);

      applicationObject.add(JsonKeys.HEURISTICS_SUMMARY, heuristicsArray);
      applicationSummaryArray.add(applicationObject);
    }

    userResult.addProperty(JsonKeys.START, offset);
    userResult.addProperty(JsonKeys.END, end);
    userResult.addProperty(JsonKeys.TOTAL, total);
    userResult.add(JsonKeys.SUMMARIES, applicationSummaryArray);

    JsonObject parent = new JsonObject();
    parent.add(JsonKeys.USER_DETAILS, userResult);
    return ok(new Gson().toJson(parent));
  }

  /**
   * Returns the status of the exception feature.
   * return: JsonObject corresponding to the exception status
   * response:
   *
   * {
   * "exception-statuses": {
   * "exceptionenabled": "true",
   * "schedulers": [
   *    {
   *       "name": "azkaban"
   *    }
   *  ],
   *  "id": "exception-status"
   *  }
   * }
   */
  public static Result restExceptionStatuses () {
    JsonObject parent = new JsonObject();
    Set<String> schedulersConfigured = InfoExtractor.getSchedulersConfiguredForException();
    JsonObject exception = new JsonObject();
    if(schedulersConfigured.isEmpty()) {
      exception.addProperty(JsonKeys.EXCEPTION_ENABLED, "false");
      exception.add(JsonKeys.SCHEDULERS, new JsonArray());

      exception.addProperty(JsonKeys.ID, "exception-status");
      parent.add(JsonKeys.EXCEPTION_STATUSES, exception);
      return ok(new Gson().toJson(parent));
    }

    JsonArray schedulers = new JsonArray();
    for(String scheduler: schedulersConfigured) {
      JsonObject schedulerObject = new JsonObject();
      schedulerObject.addProperty(JsonKeys.NAME, scheduler);
      schedulers.add(schedulerObject);
    }
    exception.addProperty(JsonKeys.EXCEPTION_ENABLED, "true");
    exception.add(JsonKeys.SCHEDULERS, schedulers);
    exception.addProperty(JsonKeys.ID, "exception-status");
    parent.add(JsonKeys.EXCEPTION_STATUSES, exception);
    return ok(new Gson().toJson(parent));
  }



  /**
   * Controls Exceptions
   * @throws URISyntaxException
   */
  public static Result restExceptions() throws URISyntaxException, MalformedURLException, IOException,
                                               AuthenticationException {
    DynamicForm form = Form.form().bindFromRequest(request());
    String url = form.get("flow-exec-url");
    JsonObject parent = new JsonObject();

    String scheduler = form.get("scheduler");

    HadoopSecurity _hadoopSeverity = HadoopSecurity.getInstance();


    logger.info(String.format("scheduler + ", scheduler));
    if(scheduler==null) {
      scheduler = "azkaban";
      logger.info(String.format("Setting scheduler ", scheduler));
    }
    if(!InfoExtractor.getSchedulersConfiguredForException().contains(scheduler)) {
      logger.info("scheduler not found ");
      parent.add("workflow-exceptions", new JsonArray());
      return status(503,"Service is currently unavailable");
    }
    if (url == null || url.isEmpty()) {
      parent.add("workflow-exceptions", new JsonArray());
      return notFound(new Gson().toJson(parent));
    } else {
      ExceptionFinder expGen;
      HadoopException flowException;

      try {
        expGen = new ExceptionFinder(url, scheduler);
        flowException = expGen.getExceptions();
      } catch (RuntimeException e) {
        parent.add("workflow-exceptions", new JsonArray());
        return status(500,"Unexpected error occured");
      } catch (Exception e) {
        parent.add("workflow-exceptions", new JsonArray());
        return status(500,"Unexpected error occured");
      }

      JsonArray jobsArray = new JsonArray();

      if (!flowException.getChildExceptions().isEmpty()) {
        for (HadoopException jobException : flowException.getChildExceptions()) {
          JsonObject job = new JsonObject();
          job.addProperty(JsonKeys.NAME, jobException.getId());
          job.addProperty(JsonKeys.TYPE, jobException.getType().toString());
          job.addProperty(JsonKeys.ID, jobException.getId());

          if (jobException.getType() == HadoopException.HadoopExceptionType.SCHEDULER) {
            if (jobException.getLoggingEvent() != null && jobException.getLoggingEvent().getLog() != null) {
              job.addProperty(JsonKeys.EXCEPTION_SUMMARY, getSchedulerLog(jobException.getLoggingEvent().getLog()));
              job.addProperty(JsonKeys.STATUS, "failed");
            } else {
              job.addProperty(JsonKeys.EXCEPTION_SUMMARY, "");
              job.addProperty(JsonKeys.STATUS, "failed");
            }
          }


          if (jobException.getType() == HadoopException.HadoopExceptionType.SCRIPT) {
            if (jobException.getLoggingEvent() != null && jobException.getLoggingEvent().getLog() != null) {
              job.addProperty(JsonKeys.EXCEPTION_SUMMARY, getSchedulerLog(jobException.getLoggingEvent().getLog()));
              job.addProperty(JsonKeys.STATUS, "failed");
            } else {
              job.addProperty(JsonKeys.EXCEPTION_SUMMARY, "");
              job.addProperty(JsonKeys.STATUS, "failed");
            }
          }

          JsonArray mrExceptionsArray = new JsonArray();
          if (jobException.getType() == HadoopException.HadoopExceptionType.MR) {
            for (HadoopException mrJobException : jobException.getChildExceptions()) {
              JsonObject child = new JsonObject();
              child.addProperty(JsonKeys.NAME, mrJobException.getId());
              if (mrJobException.getLoggingEvent() != null && mrJobException.getLoggingEvent().getLog() != null) {
                child.addProperty(JsonKeys.EXCEPTION_SUMMARY, getSchedulerLog(mrJobException.getLoggingEvent().getLog()));
              } else {
                child.addProperty(JsonKeys.EXCEPTION_SUMMARY, "");
              }

              JsonArray taskExceptionsArray = new JsonArray();
              for (HadoopException mrTaskException : mrJobException.getChildExceptions()) {
                JsonObject task = new JsonObject();
                task.addProperty(JsonKeys.NAME, mrTaskException.getId());
                if (mrTaskException.getLoggingEvent() != null && mrTaskException.getLoggingEvent().getLog() != null) {
                  task.addProperty(JsonKeys.EXCEPTION_SUMMARY, getSchedulerLog(mrTaskException.getLoggingEvent().getLog()));
                } else {
                  task.addProperty(JsonKeys.EXCEPTION_SUMMARY, "");
                }
                taskExceptionsArray.add(task);
              }
              child.add(JsonKeys.TASKS, taskExceptionsArray);
              mrExceptionsArray.add(child);
            }

            if(jobException.getChildExceptions().isEmpty()) {
              JsonObject child = new JsonObject();
              child.addProperty(JsonKeys.NAME,"");
              child.add(JsonKeys.TASKS, new JsonArray());
              child.addProperty(JsonKeys.EXCEPTION_SUMMARY, getSchedulerLog(jobException.getLoggingEvent().getLog()));
              mrExceptionsArray.add(child);
            }
            job.add(JsonKeys.APPLICATIONS, mrExceptionsArray);
            job.addProperty(JsonKeys.STATUS, "failed");
          }
          jobsArray.add(job);
        }
        parent.add(JsonKeys.WORKFLOW_EXCEPTIONS, jobsArray);
        return ok(new Gson().toJson(parent));
      }
      parent.add(JsonKeys.WORKFLOW_EXCEPTIONS, jobsArray);
      return ok(new Gson().toJson(parent));
    }
  }

  /**
   * TAkes a list of strings and appends returns a single string
   * @param logs The logs by the scheduler
   * @return The scheduler logs
   */
  private static String getSchedulerLog(List<List<String>> logs) {
    if(logs==null || logs.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for(List<String> lines: logs) {
      for(String line: lines) {
        builder.append(line);
        builder.append("\n");
      }
    }
    return builder.toString();
  }


  /**
   * Generates the query for returning the application summaries
   * @param usernames The list of usernames
   * @param searchParams Any additional parameters
   * @param sortKey The key on which the applications should be sorted
   * @param increasing The boolean value to sort the output based on the key desc or increasing
   * @return The Query object based on the given above parameters
   */
  public static Query<AppResult> generateUserApplicationSummaryQuery(List<String> usernames,
      Map<String, String> searchParams, String sortKey, boolean increasing) {
    ExpressionList<AppResult> query = AppResult.find.select(AppResult.getSearchFields()).where();
    Junction<AppResult> junction = query.disjunction();
    for (String username : usernames) {
      junction.eq(AppResult.TABLE.USERNAME, username);
    }
    query.endJunction();

    String finishedTimeBegin = searchParams.get(Application.FINISHED_TIME_BEGIN);
    if (!Utils.isSet(finishedTimeBegin)) {
      finishedTimeBegin = String.valueOf(System.currentTimeMillis() - 7 * DAY); // week of data if not specified
    }

    long time = parseTime(finishedTimeBegin);
    if (time > 0) {
      query.ge(AppResult.TABLE.FINISH_TIME, time);
    }

    String finishedTimeEnd = searchParams.get(Application.FINISHED_TIME_END);
    if (!Utils.isSet(finishedTimeEnd)) {
      finishedTimeEnd = String.valueOf(System.currentTimeMillis());
    }

    time = parseTime(finishedTimeEnd);
    if (time > 0) {
      query.le(AppResult.TABLE.FINISH_TIME, time);
    }

    if (increasing) {
      return query.order(getSortKey(sortKey));
    } else {
      return query.order().desc(getSortKey(sortKey));
    }
  }

  /**
   * Maps the sort key to the actual field values
   * @param sortKey The sortKey
   * @return The value from the sort key
   */
  private static String getSortKey(String sortKey) {
    if (sortKey.equals("severity")) {
      return AppResult.TABLE.SEVERITY;
    } else if (sortKey.equals("resourceUsed")) {
      return AppResult.TABLE.RESOURCE_USAGE;
    } else if (sortKey.equals("resourceWasted")) {
      return AppResult.TABLE.WASTED_RESOURCES;
    } else if (sortKey.equals("delay")) {
      return AppResult.TABLE.TOTAL_DELAY;
    } else if (sortKey.equals("finish_time")) {
      return AppResult.TABLE.FINISH_TIME;
    }
    return "severity";
  }

  /**
   * This utility method is used to sort the jsonArray based on FinishTime
   * @param jsonArray The jsonArray to be sorted
   * @return The sorted jsonArray based on finishtime
   */
  private static JsonArray getSortedJsonArrayByFinishTime(JsonArray jsonArray) {
    JsonArray sortedJsonArray = new JsonArray();
    List<JsonObject> jsonValues = new ArrayList<JsonObject>();
    for (int i = 0; i < jsonArray.size(); i++) {
      jsonValues.add(jsonArray.get(i).getAsJsonObject());
    }
    Collections.sort(jsonValues, new Comparator<JsonObject>() {
      public int compare(JsonObject a, JsonObject b) {
        Long first = a.get(JsonKeys.FINISH_TIME).getAsLong();
        Long second = b.get(JsonKeys.FINISH_TIME).getAsLong();
        return second.compareTo(first);
      }
    });
    for (JsonObject object : jsonValues) {
      sortedJsonArray.add(object);
    }
    return sortedJsonArray;
  }

  private static List<Severity> getSortedSeverityKeys(Set<Severity> severities) {
    List<Severity> severityList = new ArrayList<Severity>();
    severityList.addAll(severities);
    Collections.sort(severityList, new Comparator<Severity>() {
      public int compare(Severity a, Severity b) {
        return b.getValue() - a.getValue();
      }
    });
    return severityList;
  }

  /**
   * Parse the string for time in long
   *
   * @param time The string to be parsed
   * @return the epoch value
   */
  private static long parseTime(String time) {
    long unixTime = 0;
    try {
      unixTime = Long.parseLong(time);
    } catch (NumberFormatException ex) {
      // return 0
    }
    return unixTime;
  }
}
