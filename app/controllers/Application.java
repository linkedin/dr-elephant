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

package controllers;

import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.Query;
import com.codahale.metrics.Timer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.Metrics;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.code.util.Constant;
import com.linkedin.drelephant.tuning.AutoTuningAPIHelper;
import com.linkedin.drelephant.tuning.Constant.AlgorithmType;
import com.linkedin.drelephant.tuning.Constant.TuningType;
import com.linkedin.drelephant.tuning.TuningInput;
import com.linkedin.drelephant.tuning.engine.SparkConfigurationConstants;
import com.linkedin.drelephant.util.Utils;
import controllers.api.v1.JsonKeys;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import models.AppHeuristicResult;
import models.AppResult;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningAlgorithm.JobType;
import models.TuningJobDefinition;
import models.TuningJobExecutionParamSet;
import models.TuningParameter;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.index;
import views.html.help.metrics.helpRuntime;
import views.html.help.metrics.helpUsedResources;
import views.html.help.metrics.helpWaittime;
import views.html.help.metrics.helpWastedResources;
import views.html.page.comparePage;
import views.html.page.flowHistoryPage;
import views.html.page.helpPage;
import views.html.page.homePage;
import views.html.page.jobHistoryPage;
import views.html.page.oldFlowHistoryPage;
import views.html.page.oldHelpPage;
import views.html.page.oldJobHistoryPage;
import views.html.page.searchPage;
import views.html.results.*;
import views.html.results.jobDetails;

import static com.linkedin.drelephant.util.Utils.*;
import static controllers.api.v1.JsonKeys.*;


public class Application extends Controller {
  private static final Logger logger = Logger.getLogger(Application.class);
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final long FETCH_DELAY = 60 * 1000;

  private static final int PAGE_LENGTH = 20;                  // Num of jobs in a search page
  private static final int PAGE_BAR_LENGTH = 5;               // Num of pages shown in the page bar
  private static final int REST_PAGE_LENGTH = 100;            // Num of jobs in a rest search page
  private static final int JOB_HISTORY_LIMIT = 5000;          // Set to avoid memory error.
  private static final int MAX_HISTORY_LIMIT = 15;            // Upper limit on the number of executions to display
  private static final int STAGE_LIMIT = 25;                  // Upper limit on the number of stages to display

  // Form and Rest parameters
  public static final String APP_ID = "id";
  public static final String FLOW_DEF_ID = "flow-def-id";
  public static final String FLOW_EXEC_ID = "flow-exec-id";
  public static final String JOB_DEF_ID = "job-def-id";
  public static final String QUEUE_NAME = "queue-name";
  public static final String SEVERITY = "severity";
  public static final String JOB_TYPE = "job-type";
  public static final String ANALYSIS = "analysis";
  public static final String STARTED_TIME_BEGIN = "started-time-begin";
  public static final String STARTED_TIME_END = "started-time-end";
  public static final String FINISHED_TIME_BEGIN = "finished-time-begin";
  public static final String FINISHED_TIME_END = "finished-time-end";
  public static final String COMPARE_FLOW_ID1 = "flow-exec-id1";
  public static final String COMPARE_FLOW_ID2 = "flow-exec-id2";
  public static final String PAGE = "page";

  private enum Version {OLD,NEW};

  // Configuration properties
  private static final String SEARCH_MATCHES_PARTIAL_CONF = "drelephant.application.search.match.partial";

  private static long _lastFetch = 0;
  private static int _numJobsAnalyzed = 0;
  private static int _numJobsCritical = 0;
  private static int _numJobsSevere = 0;


  /**
   * Serves the initial index.html page for the new user interface. This page contains the whole web app
   */
  public static Result serveAsset(String path) {
    return ok(index.render());
  }

  /**
   * Controls the Home page of Dr. Elephant.
   *
   * Displays the latest jobs which were analysed in the last 24 hours.
   */
  public static Result dashboard() {
    long now = System.currentTimeMillis();
    long finishDate = now - DAY;

    // Update statistics only after FETCH_DELAY
    if (now - _lastFetch > FETCH_DELAY) {
      _numJobsAnalyzed = AppResult.find.where().gt(AppResult.TABLE.FINISH_TIME, finishDate).findRowCount();
      _numJobsCritical = AppResult.find.where()
          .gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.CRITICAL.getValue())
          .findRowCount();
      _numJobsSevere = AppResult.find.where()
          .gt(AppResult.TABLE.FINISH_TIME, finishDate)
          .eq(AppResult.TABLE.SEVERITY, Severity.SEVERE.getValue())
          .findRowCount();
      _lastFetch = now;
    }

    // Fetch only required fields for jobs analysed in the last 24 hours up to a max of 50 jobs
    List<AppResult> results = AppResult.find.select(AppResult.getSearchFields())
        .where()
        .gt(AppResult.TABLE.FINISH_TIME, finishDate)
        .order()
        .desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(50)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        .findList();

    return ok(homePage.render(_numJobsAnalyzed, _numJobsSevere, _numJobsCritical,
        views.html.results.searchResults.render("Latest analysis", results)));
  }

  /**
   * Returns the scheduler info id/url pair for the most recent app result that has an id like value
   * (which can use % and _ SQL wild cards) for the specified field. Note that this is a pair rather
   * than merely an ID/URL because for some schedulers (e.g. Airflow) they are not equivalent and
   * usually the UI wants to display the ID with a link to the URL. While it is true that the URL
   * can probably be derived from the ID in most cases, we would need scheduler specific logic which
   * would be a mess.
   */
  private static IdUrlPair bestSchedulerInfoMatchLikeValue(String value, String schedulerIdField) {
    String schedulerUrlField;
    if (schedulerIdField.equals(AppResult.TABLE.FLOW_DEF_ID)) {
      schedulerUrlField = AppResult.TABLE.FLOW_DEF_URL;
    } else if (schedulerIdField.equals(AppResult.TABLE.FLOW_EXEC_ID)) {
      schedulerUrlField = AppResult.TABLE.FLOW_EXEC_URL;
    } else if (schedulerIdField.equals(AppResult.TABLE.JOB_DEF_ID)) {
      schedulerUrlField = AppResult.TABLE.JOB_DEF_URL;
    } else if (schedulerIdField.equals(AppResult.TABLE.JOB_EXEC_ID)) {
      schedulerUrlField = AppResult.TABLE.JOB_EXEC_URL;
    } else {
      throw new RuntimeException(String.format("%s is not a valid scheduler info id field", schedulerIdField));
    }
    AppResult result = AppResult.find
        .select(String.format("%s, %s", schedulerIdField, schedulerUrlField))
        .where().like(schedulerIdField, value)
        .order()
        .desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(1)
        .findUnique();
    if (result != null) {
      if (schedulerIdField.equals(AppResult.TABLE.FLOW_DEF_ID)) {
        return new IdUrlPair(result.flowDefId, result.flowDefUrl);
      } else if (schedulerIdField.equals(AppResult.TABLE.FLOW_EXEC_ID)) {
        return new IdUrlPair(result.flowExecId, result.flowExecUrl);
      } else if (schedulerIdField.equals(AppResult.TABLE.JOB_DEF_ID)) {
        return new IdUrlPair(result.jobDefId, result.jobDefUrl);
      } else if (schedulerIdField.equals(AppResult.TABLE.JOB_EXEC_ID)) {
        return new IdUrlPair(result.jobExecId, result.jobExecUrl);
      }
    }
    return null;
  }

  /**
   * Given a (possibly) partial scheduler info id, try to find the closest existing id.
   */
  private static IdUrlPair bestSchedulerInfoMatchGivenPartialId(String partialSchedulerInfoId, String schedulerInfoIdField) {
    IdUrlPair schedulerInfoPair;
    // check for exact match
    schedulerInfoPair = bestSchedulerInfoMatchLikeValue(partialSchedulerInfoId, schedulerInfoIdField);
    // check for suffix match if feature isn't disabled
    if (schedulerInfoPair == null && ElephantContext.instance().getGeneralConf().getBoolean(SEARCH_MATCHES_PARTIAL_CONF, true)) {
      schedulerInfoPair = bestSchedulerInfoMatchLikeValue(String.format("%s%%", partialSchedulerInfoId), schedulerInfoIdField);
    }
    // if we didn't find anything just give a buest guess
    if (schedulerInfoPair == null) {
      schedulerInfoPair = new IdUrlPair(partialSchedulerInfoId, "");
    }
    return schedulerInfoPair;
  }

  /**
   * Controls the Search Feature
   */
  public static Result search() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String appId = form.get(APP_ID);
    appId = appId != null ? appId.trim() : "";
    if (appId.contains("job")) {
      appId = appId.replaceAll("job", "application");
    }
    String partialFlowExecId = form.get(FLOW_EXEC_ID);
    partialFlowExecId = (partialFlowExecId != null) ? partialFlowExecId.trim() : null;

    String jobDefId = form.get(JOB_DEF_ID);
    jobDefId = jobDefId != null ? jobDefId.trim() : "";

    // Search and display job details when job id or flow execution url is provided.
    if (!appId.isEmpty()) {
      AppResult result = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .idEq(appId).findUnique();
      return ok(searchPage.render(null, jobDetails.render(result)));
    } else if (isSet(partialFlowExecId)) {
      IdUrlPair flowExecPair = bestSchedulerInfoMatchGivenPartialId(partialFlowExecId, AppResult.TABLE.FLOW_EXEC_ID);
      List<AppResult> results = AppResult.find
          .select(AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_EXEC_ID)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecPair.getId())
          .findList();
      Map<IdUrlPair, List<AppResult>> map = ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.JOB_EXECUTION_ID);
      return ok(searchPage.render(null, views.html.results.flowDetails.render(flowExecPair, map)));
    } else if (!jobDefId.isEmpty()) {
      List<AppResult> results = AppResult.find
          .select(AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_DEF_ID)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .where()
          .eq(AppResult.TABLE.JOB_DEF_ID, jobDefId)
          .findList();
      Map<IdUrlPair, List<AppResult>> map = ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID);

      String flowDefId = (results.isEmpty()) ? "" :  results.get(0).flowDefId;  // all results should have the same flow id
      IdUrlPair flowDefIdPair = new IdUrlPair(flowDefId, AppResult.TABLE.FLOW_DEF_URL);

      return ok(searchPage.render(null, views.html.results.flowDefinitionIdDetails.render(flowDefIdPair, map)));
    }

    // Prepare pagination of results
    PaginationStats paginationStats = new PaginationStats(PAGE_LENGTH, PAGE_BAR_LENGTH);
    int pageLength = paginationStats.getPageLength();
    paginationStats.setCurrentPage(1);
    final Map<String, String[]> searchString = request().queryString();
    if (searchString.containsKey(PAGE)) {
      try {
        paginationStats.setCurrentPage(Integer.parseInt(searchString.get(PAGE)[0]));
      } catch (NumberFormatException ex) {
        logger.error("Error parsing page number. Setting current page to 1.");
        paginationStats.setCurrentPage(1);
      }
    }
    int currentPage = paginationStats.getCurrentPage();
    int paginationBarStartIndex = paginationStats.getPaginationBarStartIndex();

    // Filter jobs by search parameters
    Query<AppResult> query = generateSearchQuery(AppResult.getSearchFields(), getSearchParams());
    List<AppResult> results = query.setFirstRow((paginationBarStartIndex - 1) * pageLength)
        .setMaxRows((paginationStats.getPageBarLength() - 1) * pageLength + 1)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        .findList();
    paginationStats.setQueryString(getQueryString());
    if (results.isEmpty() || currentPage > paginationStats.computePaginationBarEndIndex(results.size())) {
      return ok(searchPage.render(null, jobDetails.render(null)));
    } else {
      List<AppResult> resultsToDisplay = results.subList((currentPage - paginationBarStartIndex) * pageLength,
          Math.min(results.size(), (currentPage - paginationBarStartIndex + 1) * pageLength));
      return ok(searchPage.render(paginationStats, views.html.results.searchResults.render(
          String.format("Results: Showing %,d of %,d", resultsToDisplay.size(), query.findRowCount()), resultsToDisplay)));
    }
  }

  /**
   * Parses the request for the queryString
   *
   * @return URL Encoded String of Parameter Value Pair
   */
  public static String getQueryString() {
    List<BasicNameValuePair> fields = new LinkedList<BasicNameValuePair>();
    final Set<Map.Entry<String, String[]>> entries = request().queryString().entrySet();
    for (Map.Entry<String, String[]> entry : entries) {
      final String key = entry.getKey();
      final String value = entry.getValue()[0];
      if (!key.equals(PAGE)) {
        fields.add(new BasicNameValuePair(key, value));
      }
    }
    if (fields.isEmpty()) {
      return null;
    } else {
      return URLEncodedUtils.format(fields, "utf-8");
    }
  }

  public static Map<String, String> getSearchParams() {
    Map<String, String> searchParams = new HashMap<String, String>();

    DynamicForm form = Form.form().bindFromRequest(request());
    String username = form.get(USERNAME);
    username = username != null ? username.trim().toLowerCase() : null;
    searchParams.put(USERNAME, username);
    String queuename = form.get(QUEUE_NAME);
    queuename = queuename != null ? queuename.trim().toLowerCase() : null;
    searchParams.put(QUEUE_NAME, queuename);
    searchParams.put(SEVERITY, form.get(SEVERITY));
    searchParams.put(JOB_TYPE, form.get(JOB_TYPE));
    searchParams.put(ANALYSIS, form.get(ANALYSIS));
    searchParams.put(FINISHED_TIME_BEGIN, form.get(FINISHED_TIME_BEGIN));
    searchParams.put(FINISHED_TIME_END, form.get(FINISHED_TIME_END));
    searchParams.put(STARTED_TIME_BEGIN, form.get(STARTED_TIME_BEGIN));
    searchParams.put(STARTED_TIME_END, form.get(STARTED_TIME_END));

    return searchParams;
  }

  /**
   * Build SQL predicates for Search Query
   *
   * @param selectParams The fields to select from the table
   * @param searchParams The fields to query on the table
   * @return An sql expression on App Result
   */
  public static Query<AppResult> generateSearchQuery(String selectParams, Map<String, String> searchParams) {
    if (searchParams == null || searchParams.isEmpty()) {
      return AppResult.find.select(selectParams).order().desc(AppResult.TABLE.FINISH_TIME);
    }
    ExpressionList<AppResult> query = AppResult.find.select(selectParams).where();

    // Build predicates
    String username = searchParams.get(USERNAME);
    if (isSet(username)) {
      query = query.eq(AppResult.TABLE.USERNAME, username);
    }

    String queuename = searchParams.get(QUEUE_NAME);
    if (isSet(queuename)) {
      query = query.eq(AppResult.TABLE.QUEUE_NAME, queuename);
    }
    String jobType = searchParams.get(JOB_TYPE);
    if (isSet(jobType)) {
      query = query.eq(AppResult.TABLE.JOB_TYPE, jobType);
    }
    String severity = searchParams.get(SEVERITY);
    if (isSet(severity)) {
      String analysis = searchParams.get(ANALYSIS);
      if (isSet(analysis)) {
        query =
            query.eq(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.HEURISTIC_NAME, analysis)
                .ge(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.SEVERITY, severity);
      } else {
        query = query.ge(AppResult.TABLE.SEVERITY, severity);
      }
    }

    // Time Predicates. Both the startedTimeBegin and startedTimeEnd are inclusive in the filter
    String startedTimeBegin = searchParams.get(STARTED_TIME_BEGIN);
    if (isSet(startedTimeBegin)) {
      long time = parseTime(startedTimeBegin);
      if (time > 0) {
        query = query.ge(AppResult.TABLE.START_TIME, time);
      }
    }
    String startedTimeEnd = searchParams.get(STARTED_TIME_END);
    if (isSet(startedTimeEnd)) {
      long time = parseTime(startedTimeEnd);
      if (time > 0) {
        query = query.le(AppResult.TABLE.START_TIME, time);
      }
    }

    String finishedTimeBegin = searchParams.get(FINISHED_TIME_BEGIN);
    if (isSet(finishedTimeBegin)) {
      long time = parseTime(finishedTimeBegin);
      if (time > 0) {
        query = query.ge(AppResult.TABLE.FINISH_TIME, time);
      }
    }
    String finishedTimeEnd = searchParams.get(FINISHED_TIME_END);
    if (isSet(finishedTimeEnd)) {
      long time = parseTime(finishedTimeEnd);
      if (time > 0) {
        query = query.le(AppResult.TABLE.FINISH_TIME, time);
      }
    }

    // If queried by start time then sort the results by start time.
    if (isSet(startedTimeBegin) || isSet(startedTimeEnd)) {
      return query.order().desc(AppResult.TABLE.START_TIME);
    } else {
      return query.order().desc(AppResult.TABLE.FINISH_TIME);
    }
  }

  /**
   Controls the Compare Feature
   */
  public static Result compare() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String partialFlowExecId1 = form.get(COMPARE_FLOW_ID1);
    partialFlowExecId1 = (partialFlowExecId1 != null) ? partialFlowExecId1.trim() : null;
    String partialFlowExecId2 = form.get(COMPARE_FLOW_ID2);
    partialFlowExecId2 = (partialFlowExecId2 != null) ? partialFlowExecId2.trim() : null;

    List<AppResult> results1 = null;
    List<AppResult> results2 = null;
    if (partialFlowExecId1 != null && !partialFlowExecId1.isEmpty() && partialFlowExecId2 != null && !partialFlowExecId2.isEmpty()) {
      IdUrlPair flowExecIdPair1 = bestSchedulerInfoMatchGivenPartialId(partialFlowExecId1, AppResult.TABLE.FLOW_EXEC_ID);
      IdUrlPair flowExecIdPair2 = bestSchedulerInfoMatchGivenPartialId(partialFlowExecId2, AppResult.TABLE.FLOW_EXEC_ID);
      results1 = AppResult.find
          .select(AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL
              + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecIdPair1.getId()).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .findList();
      results2 = AppResult.find
          .select(
              AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL + ","
                  + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecIdPair2.getId()).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .findList();
    }
    return ok(comparePage.render(views.html.results.compareResults.render(compareFlows(results1, results2))));
  }

  /**
   * Helper Method for the compare controller.
   * This Compares 2 flow executions at job level.
   *
   * @param results1 The list of jobs under flow execution 1
   * @param results2 The list of jobs under flow execution 2
   * @return A map of Job Urls to the list of jobs corresponding to the 2 flow execution urls
   */
  private static Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> compareFlows(List<AppResult> results1, List<AppResult> results2) {

    Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> jobDefMap = new HashMap<IdUrlPair, Map<IdUrlPair, List<AppResult>>>();

    if (results1 != null && !results1.isEmpty() && results2 != null && !results2.isEmpty()) {

      IdUrlPair flow1 = new IdUrlPair(results1.get(0).flowExecId, results1.get(0).flowExecUrl);
      IdUrlPair flow2 = new IdUrlPair(results2.get(0).flowExecId, results2.get(0).flowExecUrl);

      Map<IdUrlPair, List<AppResult>> map1 = ControllerUtil.groupJobs(results1, ControllerUtil.GroupBy.JOB_DEFINITION_ID);
      Map<IdUrlPair, List<AppResult>> map2 = ControllerUtil.groupJobs(results2, ControllerUtil.GroupBy.JOB_DEFINITION_ID);

      final Set<IdUrlPair> group1 = new TreeSet<IdUrlPair>(new Comparator<IdUrlPair>() {
        public int compare(final IdUrlPair o1, final IdUrlPair o2) {
          return o1.getId().compareToIgnoreCase(o2.getId());
        }
      });
      group1.addAll(map1.keySet());
      final Set<IdUrlPair> group2 = new TreeSet<IdUrlPair>(new Comparator<IdUrlPair>() {
        public int compare(final IdUrlPair o1, final IdUrlPair o2) {
          return o1.getId().compareToIgnoreCase(o2.getId());
        }
      });
      group2.addAll(map2.keySet());

      // Display jobs that are common to the two flows first followed by jobs in flow 1 and flow 2.
      Set<IdUrlPair> CommonJobs = Sets.intersection(group1, group2);
      Set<IdUrlPair> orderedFlowSet = Sets.union(CommonJobs, group1);
      Set<IdUrlPair> union = Sets.union(orderedFlowSet, group2);

      for (IdUrlPair pair : union) {
        Map<IdUrlPair, List<AppResult>> flowExecMap = new LinkedHashMap<IdUrlPair, List<AppResult>>();
        flowExecMap.put(flow1, map1.get(pair));
        flowExecMap.put(flow2, map2.get(pair));
        jobDefMap.put(pair, flowExecMap);
      }
    }
    return jobDefMap;
  }

  /**
   * Returns the new version of flow history
   */
  public static Result flowHistory() {
    return getFlowHistory(Version.NEW);
  }

  /**
   * Returns the old version of flow history
   */
  public static Result oldFlowHistory() {
    return getFlowHistory(Version.OLD);
  }

  /**
   * Returns the flowHistory based on the version provided
   *
   * @param version Can be either new or old
   * @return The flowhistory page based on the version provided
   */
  private static Result getFlowHistory(Version version) {
    DynamicForm form = Form.form().bindFromRequest(request());
    String partialFlowDefId = form.get(FLOW_DEF_ID);
    partialFlowDefId = (partialFlowDefId != null) ? partialFlowDefId.trim() : null;

    boolean hasSparkJob = false;

    String graphType = form.get("select-graph-type");

    // get the graph type
    if (graphType == null) {
      graphType = "resources";
    }

    if (!isSet(partialFlowDefId)) {
      if (version.equals(Version.NEW)) {
        return ok(flowHistoryPage
            .render(partialFlowDefId, graphType, views.html.results.flowHistoryResults.render(null, null, null, null)));
      } else {
        return ok(
            oldFlowHistoryPage.render(partialFlowDefId, graphType, views.html.results.oldFlowHistoryResults.render(null, null, null, null)));
      }
    }

    IdUrlPair flowDefPair = bestSchedulerInfoMatchGivenPartialId(partialFlowDefId, AppResult.TABLE.FLOW_DEF_ID);

    List<AppResult> results;

    if (graphType.equals("time") || graphType.equals("resources")) {

      // if graph type is time or resources, we don't need the result from APP_HEURISTIC_RESULTS
      results = AppResult.find.select(
          AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL + ","
              + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL + "," + AppResult.TABLE.JOB_NAME)
          .where()
          .eq(AppResult.TABLE.FLOW_DEF_ID, flowDefPair.getId())
          .order()
          .desc(AppResult.TABLE.FINISH_TIME)
          .setMaxRows(JOB_HISTORY_LIMIT)
          .findList();
    } else {

      // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
      results = AppResult.find.select(
          AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL + ","
              + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL + "," + AppResult.TABLE.JOB_NAME)
          .where()
          .eq(AppResult.TABLE.FLOW_DEF_ID, flowDefPair.getId())
          .order()
          .desc(AppResult.TABLE.FINISH_TIME)
          .setMaxRows(JOB_HISTORY_LIMIT)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .findList();
    }
    if (results.size() == 0) {
      return notFound("Unable to find record for flow def id: " + flowDefPair.getId());
    }

    for (AppResult result : results) {
      if (result.jobType.equals("Spark")) {
        hasSparkJob = true;
      }
    }

    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap = ControllerUtil
        .limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID),
            results.size(), MAX_HISTORY_LIMIT);

    // Compute flow execution data
    List<AppResult> filteredResults = new ArrayList<AppResult>();     // All jobs starting from latest execution
    List<Long> flowExecTimeList = new ArrayList<Long>();         // To map executions to resp execution time
    Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> executionMap =
        new LinkedHashMap<IdUrlPair, Map<IdUrlPair, List<AppResult>>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry : flowExecIdToJobsMap.entrySet()) {

      // Reverse the list content from desc order of finish time to increasing order so that when grouping we get
      // the job list in the order of completion.
      List<AppResult> mrJobsList = Lists.reverse(entry.getValue());

      // Flow exec time is the finish time of the last mr job in the flow
      flowExecTimeList.add(mrJobsList.get(mrJobsList.size() - 1).finishTime);

      filteredResults.addAll(mrJobsList);
      executionMap.put(entry.getKey(), ControllerUtil.groupJobs(mrJobsList, ControllerUtil.GroupBy.JOB_DEFINITION_ID));
    }

    // Calculate unique list of jobs (job def url) to maintain order across executions. List will contain job def urls
    // from latest execution first followed by any other extra job def url that may appear in previous executions.
    final Map<IdUrlPair, String> idPairToJobNameMap = new ListOrderedMap() ;

    Map<IdUrlPair, List<AppResult>> filteredTempMap =
        ControllerUtil.groupJobs(filteredResults, ControllerUtil.GroupBy.JOB_DEFINITION_ID);

    List<Map.Entry<IdUrlPair, List<AppResult>>> filteredMapList =
        new LinkedList<Map.Entry<IdUrlPair, List<AppResult>>>( filteredTempMap.entrySet() );

    Collections.sort(filteredMapList, new Comparator<Map.Entry<IdUrlPair, List<AppResult>>>() {
      @Override
      public int compare(Map.Entry<IdUrlPair, List<AppResult>> idUrlPairListMap, Map.Entry<IdUrlPair, List<AppResult>> t1) {
        return ( new Long(idUrlPairListMap.getValue().get(0).finishTime)).compareTo(t1.getValue().get(0).finishTime);
      }
    });


    for (Map.Entry<IdUrlPair, List<AppResult>> entry : filteredMapList) {
      idPairToJobNameMap.put(entry.getKey(), entry.getValue().get(0).jobName);
    }

    if (version.equals(Version.NEW)) {
      if (graphType.equals("heuristics")) {
        return ok(flowHistoryPage.render(flowDefPair.getId(), graphType,
            views.html.results.flowHistoryResults.render(flowDefPair, executionMap, idPairToJobNameMap, flowExecTimeList)));
      } else if (graphType.equals("resources") || graphType.equals("time")) {
        return ok(flowHistoryPage.render(flowDefPair.getId(), graphType, views.html.results.flowMetricsHistoryResults
            .render(flowDefPair, graphType, executionMap, idPairToJobNameMap, flowExecTimeList)));
      }
    } else {
      if (graphType.equals("heuristics")) {
        return ok(oldFlowHistoryPage.render(flowDefPair.getId(), graphType,
            views.html.results.oldFlowHistoryResults.render(flowDefPair, executionMap, idPairToJobNameMap, flowExecTimeList)));
      } else if (graphType.equals("resources") || graphType.equals("time")) {
        if (hasSparkJob) {
          return notFound("Cannot plot graph for " + graphType + " since it contains a spark job. " + graphType
              + " graphs are not supported for spark right now");
        } else {
          return ok(oldFlowHistoryPage.render(flowDefPair.getId(), graphType, views.html.results.oldFlowMetricsHistoryResults
              .render(flowDefPair, graphType, executionMap, idPairToJobNameMap, flowExecTimeList)));
        }
      }
    }
    return notFound("Unable to find graph type: " + graphType);
  }

  /**
   * Controls Job History. Displays at max MAX_HISTORY_LIMIT executions. Old version of the job history
   */
  public static Result oldJobHistory() {
    return getJobHistory(Version.OLD);
  }

  /**
   * Controls Job History. Displays at max MAX_HISTORY_LIMIT executions. New version of the job history
   */
  public static Result jobHistory() {
    return getJobHistory(Version.NEW);
  }

  /**
   * Returns the job history. Returns at max MAX_HISTORY_LIMIT executions.
   *
   * @param version The version of job history to return
   * @return The job history page based on the version.
   */
  private static Result getJobHistory(Version version) {
    DynamicForm form = Form.form().bindFromRequest(request());
    String partialJobDefId = form.get(JOB_DEF_ID);
    partialJobDefId = (partialJobDefId != null) ? partialJobDefId.trim() : null;

    boolean hasSparkJob = false;
    // get the graph type
    String graphType = form.get("select-graph-type");

    if (graphType == null) {
      graphType = "resources";
    }

    if (!isSet(partialJobDefId)) {
      if (version.equals(Version.NEW)) {
        return ok(
            jobHistoryPage.render(partialJobDefId, graphType, views.html.results.jobHistoryResults.render(null, null, -1, null)));
      } else {
        return ok(oldJobHistoryPage.render(partialJobDefId, graphType, views.html.results.oldJobHistoryResults.render(null, null, -1, null)));
      }
    }
    IdUrlPair jobDefPair = bestSchedulerInfoMatchGivenPartialId(partialJobDefId, AppResult.TABLE.JOB_DEF_ID);

    List<AppResult> results;

    if (graphType.equals("time") || graphType.equals("resources")) {
      // we don't need APP_HEURISTIC_RESULT_DETAILS data to plot for time and resources
      results = AppResult.find.select(
          AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where()
          .eq(AppResult.TABLE.JOB_DEF_ID, jobDefPair.getId())
          .order()
          .desc(AppResult.TABLE.FINISH_TIME)
          .setMaxRows(JOB_HISTORY_LIMIT)
          .findList();
    } else {
      // Fetch all job executions
      results = AppResult.find.select(
          AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where()
          .eq(AppResult.TABLE.JOB_DEF_ID, jobDefPair.getId())
          .order()
          .desc(AppResult.TABLE.FINISH_TIME)
          .setMaxRows(JOB_HISTORY_LIMIT)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
          .findList();
    }

    for (AppResult result : results) {
      if (result.jobType.equals("Spark")) {
        hasSparkJob = true;
      }
    }

    if (results.size() == 0) {
      return notFound("Unable to find record for job def id: " + jobDefPair.getId());
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap = ControllerUtil
        .limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID),
            results.size(), MAX_HISTORY_LIMIT);

    // Compute job execution data
    List<Long> flowExecTimeList = new ArrayList<Long>();
    int maxStages = 0;
    Map<IdUrlPair, List<AppResult>> executionMap = new LinkedHashMap<IdUrlPair, List<AppResult>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry : flowExecIdToJobsMap.entrySet()) {

      // Reverse the list content from desc order of finish time to increasing order so that when grouping we get
      // the job list in the order of completion.
      List<AppResult> mrJobsList = Lists.reverse(entry.getValue());

      // Get the finish time of the last mr job that completed in current flow.
      flowExecTimeList.add(mrJobsList.get(mrJobsList.size() - 1).finishTime);

      // Find the maximum number of mr stages for any job execution
      int stageSize = flowExecIdToJobsMap.get(entry.getKey()).size();
      if (stageSize > maxStages) {
        maxStages = stageSize;
      }

      executionMap.put(entry.getKey(), Lists.reverse(flowExecIdToJobsMap.get(entry.getKey())));
    }
    if (maxStages > STAGE_LIMIT) {
      maxStages = STAGE_LIMIT;
    }
    if (version.equals(Version.NEW)) {
      if (graphType.equals("heuristics")) {
        return ok(jobHistoryPage.render(jobDefPair.getId(), graphType,
            views.html.results.jobHistoryResults.render(jobDefPair, executionMap, maxStages, flowExecTimeList)));
      } else if (graphType.equals("resources") || graphType.equals("time")) {
        return ok(jobHistoryPage.render(jobDefPair.getId(), graphType,
            views.html.results.jobMetricsHistoryResults.render(jobDefPair, graphType, executionMap, maxStages, flowExecTimeList)));
      }
    } else {
      if (graphType.equals("heuristics")) {
        return ok(oldJobHistoryPage.render(jobDefPair.getId(), graphType,
            views.html.results.oldJobHistoryResults.render(jobDefPair, executionMap, maxStages, flowExecTimeList)));
      } else if (graphType.equals("resources") || graphType.equals("time")) {
        if (hasSparkJob) {
          return notFound("Resource and time graph are not supported for spark right now");
        } else {
          return ok(oldJobHistoryPage.render(jobDefPair.getId(), graphType,
              views.html.results.oldJobMetricsHistoryResults.render(jobDefPair, graphType, executionMap, maxStages, flowExecTimeList)));
        }
      }
    }
    return notFound("Unable to find graph type: " + graphType);
  }

  /**
   * Returns the help based on the version
   *
   * @param version The version for which help page has to be returned
   * @return The help page based on the version
   */
  private static Result getHelp(Version version) {
    DynamicForm form = Form.form().bindFromRequest(request());
    String topic = form.get("topic");
    Html page = null;
    String title = "Help";
    if (topic != null && !topic.isEmpty()) {
      // Currently code heuristic view is loaded from the code and not from
      // config file (since it has only one view)
      // todo: load view for code level heuristic also from configuration file
      if(topic.toLowerCase().equals(Constant.CODE_HEURISTIC_NAME)){
        page = getView();
      }
      else {
        page = ElephantContext.instance().getHeuristicToView().get(topic);
      }

      // check if it is a metrics help
      if (page == null) {
        page = getMetricsNameView().get(topic);
      }

      if (page != null) {
        title = topic;
      }
    }

    if (version.equals(Version.NEW)) {
      return ok(helpPage.render(title, page));
    }
    return ok(oldHelpPage.render(title, page));
  }

  private static  Html getView(){
    try {
      Class<?> viewClass = Class.forName(Constant.CODE_HEURISTIC_VIEW_NAME);
      Method render = viewClass.getDeclaredMethod("render");
      Html page = (Html) render.invoke(null);
      logger.info("Load View : " +viewClass);
      return page;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not find view ", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access render on view" , e);
    } catch (RuntimeException e) {
      // More descriptive on other runtime exception such as ClassCastException
      throw new RuntimeException( " is not a valid view class.", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Could not invoke view " , e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method render for view " , e);
    }
  }

  /**
   * Controls the new Help Page
   */
  public static Result oldHelp() {
    return getHelp(Version.OLD);
  }

  /**
   * Controls the old Help Page
   */
  public static Result help() {
    return getHelp(Version.NEW);
  }


  private static Map<String, Html> getMetricsNameView() {
    Map<String,Html> metricsViewMap = new HashMap<String, Html>();
    metricsViewMap.put(Metrics.RUNTIME.getText(), helpRuntime.render());
    metricsViewMap.put(Metrics.WAIT_TIME.getText(), helpWaittime.render());
    metricsViewMap.put(Metrics.USED_RESOURCES.getText(), helpUsedResources.render());
    metricsViewMap.put(Metrics.WASTED_RESOURCES.getText(), helpWastedResources.render());
    return metricsViewMap;
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

  /**
   * Rest API for searching a particular job information
   * E.g, localhost:8080/rest/job?id=xyz
   */
  public static Result restAppResult(String id) {

    if (id == null || id.isEmpty()) {
      return badRequest("No job id provided.");
    }
    if (id.contains("job")) {
      id = id.replaceAll("job", "application");
    }

    AppResult result = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .idEq(id)
        .findUnique();

    if (result != null) {
      return ok(Json.toJson(result));
    } else {
      return notFound("Unable to find record on id: " + id);
    }
  }

  /**
   * Rest API for getting current run parameters from auto tuning framework
   * @return
   */
  public static Result getCurrentRunParameters() {
    final Timer.Context context = AutoTuningMetricsController.getCurrentRunParametersTimerContext();
    String jsonString = request().body().asJson().toString();
    logger.debug("Started processing getCurrentRunParameters request with following parameters " + jsonString);
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> paramValueMap = new HashMap<String, String>();
    TuningInput tuningInput = new TuningInput();
    try {
      paramValueMap = (Map<String, String>) mapper.readValue(jsonString, Map.class);
      String flowDefId = paramValueMap.get("flowDefId");
      String jobDefId = paramValueMap.get("jobDefId");
      String flowDefUrl = paramValueMap.get("flowDefUrl");
      String jobDefUrl = paramValueMap.get("jobDefUrl");
      String flowExecId = paramValueMap.get("flowExecId");
      String jobExecId = paramValueMap.get("jobExecId");
      String flowExecUrl = paramValueMap.get("flowExecUrl");
      String jobExecUrl = paramValueMap.get("jobExecUrl");
      String jobName = paramValueMap.get("jobName");
      String userName = paramValueMap.get("userName");
      String client = paramValueMap.get("client");
      String scheduler = paramValueMap.get("scheduler");
      String defaultParams = paramValueMap.get("defaultParams");
      Integer version = 1;
      if (paramValueMap.containsKey("version")) {
        version = Integer.parseInt(paramValueMap.get("version"));
      }
      Boolean isRetry = false;
      if (paramValueMap.containsKey("isRetry")) {
        isRetry = Boolean.parseBoolean(paramValueMap.get("isRetry"));
      }
      Boolean skipExecutionForOptimization = false;
      if (paramValueMap.containsKey("skipExecutionForOptimization")) {
        skipExecutionForOptimization = Boolean.parseBoolean(paramValueMap.get("skipExecutionForOptimization"));
      }
      String jobType = paramValueMap.get("autoTuningJobType");
      String optimizationAlgo =
          paramValueMap.get("optimizationAlgo") /*== null ? TuningAlgorithm.OptimizationAlgo.HBT.name()
              : paramValueMap.get("optimizationAlgo")*/;
      String optimizationAlgoVersion = paramValueMap.get("optimizationAlgoVersion");
      String optimizationMetric = paramValueMap.get("optimizationMetric");

      Double allowedMaxExecutionTimePercent = null;
      Double allowedMaxResourceUsagePercent = null;
      if (paramValueMap.containsKey("allowedMaxResourceUsagePercent")) {
        allowedMaxResourceUsagePercent = Double.parseDouble(paramValueMap.get("allowedMaxResourceUsagePercent"));
      }
      if (paramValueMap.containsKey("allowedMaxExecutionTimePercent")) {
        allowedMaxExecutionTimePercent = Double.parseDouble(paramValueMap.get("allowedMaxExecutionTimePercent"));
      }
      /*
       This logic is for backward compatailbity. Ideally we should remove this logic in next release
       because HBT should be the default optimization algorithm for all the jobs.
       */
      optimizationAlgo = getAlgoBasedOnVersion(jobType, version);
      tuningInput.setFlowDefId(flowDefId);
      tuningInput.setJobDefId(jobDefId);
      tuningInput.setFlowDefUrl(flowDefUrl);
      tuningInput.setJobDefUrl(jobDefUrl);
      tuningInput.setFlowExecId(flowExecId);
      tuningInput.setJobExecId(jobExecId);
      tuningInput.setFlowExecUrl(flowExecUrl);
      tuningInput.setJobExecUrl(jobExecUrl);
      tuningInput.setJobName(jobName);
      tuningInput.setUserName(userName);
      tuningInput.setClient(client);
      tuningInput.setScheduler(scheduler);
      tuningInput.setDefaultParams(defaultParams);
      tuningInput.setVersion(version);
      tuningInput.setRetry(isRetry);
      tuningInput.setSkipExecutionForOptimization(skipExecutionForOptimization);
      tuningInput.setJobType(jobType);
      tuningInput.setOptimizationAlgo(optimizationAlgo);
      tuningInput.setOptimizationAlgoVersion(optimizationAlgoVersion);
      tuningInput.setOptimizationMetric(optimizationMetric);
      tuningInput.setAllowedMaxExecutionTimePercent(allowedMaxExecutionTimePercent);
      tuningInput.setAllowedMaxResourceUsagePercent(allowedMaxResourceUsagePercent);
      return getCurrentRunParameters(tuningInput);
    } catch (Exception e) {
      AutoTuningMetricsController.markGetCurrentRunParametersFailures();
      logger.error("Exception parsing input: ", e);
      return notFound("Error parsing input " + e.getMessage());
    } finally {
      if (context != null) {
        context.stop();
      }
    }
  }

  public static String getAlgoBasedOnVersion(String jobType, Integer version) {
    if (jobType.equals(JobType.PIG.name()) && version == 1) {
      return TuningAlgorithm.OptimizationAlgo.PSO_IPSO.name();
    } else if (jobType.equals(JobType.PIG.name()) && version > 1) {
      return TuningAlgorithm.OptimizationAlgo.HBT.name();
    } else if (jobType.equals(JobType.SPARK.name())) {
      return TuningAlgorithm.OptimizationAlgo.HBT.name();
    } else {
      return TuningAlgorithm.OptimizationAlgo.HBT.name();
    }
  }

  private static JsonNode formatGetCurrentRunParametersOutput(Map<String, Double> outputParams, TuningInput tuningInput) {
    if (tuningInput.getVersion() == 1) {
      return Json.toJson(outputParams);
    } else if (tuningInput.getJobType().equals(JobType.SPARK.name())) {
      return formatSparkOutputToJson(outputParams);
    } else {
      return formatMROutput(outputParams);
    }
  }

  private static JsonNode formatMROutput(Map<String, Double> outputParams) {
    Map<String, String> outputParamFormatted = new HashMap<String, String>();

    //Temporarily removing input split parameters
//    outputParams.remove("pig.maxCombinedSplitSize");
//    outputParams.remove("mapreduce.input.fileinputformat.split.maxsize");

    for (Map.Entry<String, Double> param : outputParams.entrySet()) {
      if (param.getKey().equals("mapreduce.map.sort.spill.percent")) {
        outputParamFormatted.put(param.getKey(), String.valueOf(param.getValue()));
      } else if (param.getKey().equals("mapreduce.map.java.opts")
          || param.getKey().equals("mapreduce.reduce.java.opts")) {
        outputParamFormatted.put(param.getKey(),
            "-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:ParallelGCThreads=5 -Djava.net.preferIPv4Stack=true -Xms512m -Xmx"
                + Math.round(param.getValue()) + "m");
      } else {
        outputParamFormatted.put(param.getKey(), String.valueOf(Math.round(param.getValue())));
      }
    }
    return Json.toJson(outputParamFormatted);
  }

  private static JsonNode formatSparkOutputToJson(Map<String, Double> outputParams) {
    return Json.toJson(formatSparkOutput(outputParams));
  }
  private static Map<String, String> formatSparkOutput(Map<String, Double> outputParams) {
    Map<String, String> outputParamFormatted = new HashMap<String, String>();
    for (Map.Entry<String, Double> param : outputParams.entrySet()) {
      if (param.getKey().equals(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_KEY)) {
        outputParamFormatted.put(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_KEY, param.getValue().intValue()
            + "m");
      } else if (param.getKey().equals(SparkConfigurationConstants.SPARK_EXECUTOR_CORES_KEY)) {
        outputParamFormatted.put(SparkConfigurationConstants.SPARK_EXECUTOR_CORES_KEY,
            String.valueOf(param.getValue().intValue()));
      }else if (param.getKey().equals(SparkConfigurationConstants.SPARK_EXECUTOR_INSTANCES_KEY)) {
        outputParamFormatted.put(SparkConfigurationConstants.SPARK_EXECUTOR_INSTANCES_KEY,
            String.valueOf(param.getValue().intValue()));
      } else if (param.getKey().equals(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_OVERHEAD)) {
        outputParamFormatted.put(SparkConfigurationConstants.SPARK_EXECUTOR_MEMORY_OVERHEAD, param.getValue()
            .intValue() + "m");
      } else if (param.getKey().equals(SparkConfigurationConstants.SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD)) {
        outputParamFormatted.put(SparkConfigurationConstants.SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD, param.getValue()
            .intValue() + "m");
      } else if (param.getKey().equals(SparkConfigurationConstants.SPARK_DRIVER_MEMORY_KEY)) {
        outputParamFormatted
            .put(SparkConfigurationConstants.SPARK_DRIVER_MEMORY_KEY, param.getValue().intValue() + "m");
      } else {
        outputParamFormatted.put(param.getKey(), param.getValue().toString());
      }
    }
    return outputParamFormatted;
  }

  private static Result getCurrentRunParameters(TuningInput tuningInput) throws Exception {
    AutoTuningAPIHelper autoTuningAPIHelper = new AutoTuningAPIHelper();
    Map<String, Double> outputParams = autoTuningAPIHelper.getCurrentRunParameters(tuningInput);
    if (outputParams != null) {
      JsonNode outputJSON=formatGetCurrentRunParametersOutput(outputParams, tuningInput);
      logger.info("Output JSON " + outputJSON.toString());
      return ok(formatGetCurrentRunParametersOutput(outputParams, tuningInput));
    } else {
      AutoTuningMetricsController.markGetCurrentRunParametersFailures();
      return notFound("Unable to find parameters. Job id: " + tuningInput.getJobDefId() + " Flow id: "
          + tuningInput.getFlowDefId());
    }
  }

  /**
   * Rest API for searching all jobs triggered by a particular Scheduler Job
   * E.g., localhost:8080/rest/jobexec?id=xyz
   */
  public static Result restJobExecResult(String jobExecId) {

    if (jobExecId == null || jobExecId.isEmpty()) {
      return badRequest("No job exec url provided.");
    }

    List<AppResult> result = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.JOB_EXEC_ID, jobExecId)
        .findList();

    if (result.size() == 0) {
      return notFound("Unable to find record on job exec url: " + jobExecId);
    }

    return ok(Json.toJson(result));
  }

  /**
   * Rest API for searching all jobs under a particular flow execution
   * E.g., localhost:8080/rest/flowexec?id=xyz
   */
  public static Result restFlowExecResult(String flowExecId) {

    if (flowExecId == null || flowExecId.isEmpty()) {
      return badRequest("No flow exec url provided.");
    }

    List<AppResult> results = AppResult.find.select("*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .where()
        .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId)
        .findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec url: " + flowExecId);
    }

    Map<IdUrlPair, List<AppResult>> groupMap = ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.JOB_EXECUTION_ID);

    Map<String, List<AppResult>> resMap = new HashMap<String, List<AppResult>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry : groupMap.entrySet()) {
      IdUrlPair jobExecPair = entry.getKey();
      List<AppResult> value = entry.getValue();
      resMap.put(jobExecPair.getId(), value);
    }

    return ok(Json.toJson(resMap));
  }





  /**
   * The Rest API for Search Feature
   *
   * http://localhost:8080/rest/search?username=abc&job-type=HadoopJava
   */
  public static Result restSearch() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String appId = form.get(APP_ID);
    appId = appId != null ? appId.trim() : "";
    if (appId.contains("job")) {
      appId = appId.replaceAll("job", "application");
    }
    String flowExecId = form.get(FLOW_EXEC_ID);
    flowExecId = (flowExecId != null) ? flowExecId.trim() : null;
    if (!appId.isEmpty()) {
      AppResult result = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .idEq(appId)
          .findUnique();
      if (result != null) {
        return ok(Json.toJson(result));
      } else {
        return notFound("Unable to find record on id: " + appId);
      }
    } else if (flowExecId != null && !flowExecId.isEmpty()) {
      List<AppResult> results = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId)
          .findList();
      if (results.size() == 0) {
        return notFound("Unable to find record on flow execution: " + flowExecId);
      } else {
        return ok(Json.toJson(results));
      }
    }

    int page = 1;
    if (request().queryString().containsKey(PAGE)) {
      page = Integer.parseInt(request().queryString().get(PAGE)[0]);
      if (page <= 0) {
        page = 1;
      }
    }

    Query<AppResult> query = generateSearchQuery("*", getSearchParams());
    List<AppResult> results = query.setFirstRow((page - 1) * REST_PAGE_LENGTH)
        .setMaxRows(REST_PAGE_LENGTH)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .findList();

    if (results.size() == 0) {
      return notFound("No records");
    } else {
      return ok(Json.toJson(results));
    }
  }

  /**
   * The Rest API for Compare Feature
   * E.g., localhost:8080/rest/compare?flow-exec-id1=abc&flow-exec-id2=xyz
   */
  public static Result restCompare() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String flowExecId1 = form.get(COMPARE_FLOW_ID1);
    flowExecId1 = (flowExecId1 != null) ? flowExecId1.trim() : null;
    String flowExecId2 = form.get(COMPARE_FLOW_ID2);
    flowExecId2 = (flowExecId2 != null) ? flowExecId2.trim() : null;

    List<AppResult> results1 = null;
    List<AppResult> results2 = null;
    if (flowExecId1 != null && !flowExecId1.isEmpty() && flowExecId2 != null && !flowExecId2.isEmpty()) {
      results1 = AppResult.find.select("*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId1)
          .setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .findList();
      results2 = AppResult.find.select("*")
          .where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId2)
          .setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS,
              "*")
          .findList();
    }

    Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> compareResults = compareFlows(results1, results2);

    Map<String, Map<String, List<AppResult>>> resMap = new HashMap<String, Map<String, List<AppResult>>>();
    for (Map.Entry<IdUrlPair, Map<IdUrlPair, List<AppResult>>> entry : compareResults.entrySet()) {
      IdUrlPair jobExecPair = entry.getKey();
      Map<IdUrlPair, List<AppResult>> value = entry.getValue();
      for (Map.Entry<IdUrlPair, List<AppResult>> innerEntry : value.entrySet()) {
        IdUrlPair flowExecPair = innerEntry.getKey();
        List<AppResult> results = innerEntry.getValue();
        Map<String, List<AppResult>> resultMap = new HashMap<String, List<AppResult>>();
        resultMap.put(flowExecPair.getId(), results);
        resMap.put(jobExecPair.getId(), resultMap);
      }
    }

    return ok(Json.toJson(resMap));
  }

  /**
   * The data for plotting the flow history graph
   *
   * <pre>
   * {@code
   *   [
   *     {
   *       "flowtime": <Last job's finish time>,
   *       "score": 1000,
   *       "jobscores": [
   *         {
   *           "jobdefurl:" "url",
   *           "jobexecurl:" "url",
   *           "jobscore": 500
   *         },
   *         {
   *           "jobdefurl:" "url",
   *           "jobexecurl:" "url",
   *           "jobscore": 500
   *         }
   *       ]
   *     },
   *     {
   *       "flowtime": <Last job's finish time>,
   *       "score": 700,
   *       "jobscores": [
   *         {
   *           "jobdefurl:" "url",
   *           "jobexecurl:" "url",
   *           "jobscore": 0
   *         },
   *         {
   *           "jobdefurl:" "url",
   *           "jobexecurl:" "url",
   *           "jobscore": 700
   *         }
   *       ]
   *     }
   *   ]
   * }
   * </pre>
   */
  public static Result restFlowGraphData(String flowDefId) {
    JsonArray datasets = new JsonArray();
    if (flowDefId == null || flowDefId.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<AppResult> results = getRestFlowAppResults(flowDefId);

    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =
        ControllerUtil.limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for (int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int flowPerfScore = 0;
      JsonArray jobScores = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));
      Map<IdUrlPair, List<AppResult>> jobDefIdToJobsMap = ControllerUtil.groupJobs(mrJobsList, ControllerUtil.GroupBy.JOB_DEFINITION_ID);

      // Compute the execution records. Note that each entry in the jobDefIdToJobsMap will have at least one AppResult
      for (IdUrlPair jobDefPair : jobDefIdToJobsMap.keySet()) {
        // Compute job perf score
        int jobPerfScore = 0;
        for (AppResult job : jobDefIdToJobsMap.get(jobDefPair)) {
          jobPerfScore += job.score;
        }

        // A job in jobscores list
        JsonObject jobScore = new JsonObject();
        jobScore.addProperty("jobscore", jobPerfScore);
        jobScore.addProperty("jobdefurl", jobDefPair.getUrl());
        jobScore.addProperty("jobexecurl", jobDefIdToJobsMap.get(jobDefPair).get(0).jobExecUrl);

        jobScores.add(jobScore);
        flowPerfScore += jobPerfScore;
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", Utils.getFlowTime(mrJobsList.get(mrJobsList.size() - 1)));
      dataset.addProperty("score", flowPerfScore);
      dataset.add("jobscores", jobScores);

      datasets.add(dataset);
    }

    JsonArray sortedDatasets = Utils.sortJsonArray(datasets);

    return ok(new Gson().toJson(sortedDatasets));
  }



  /**
   * The data for plotting the job history graph. While plotting the job history
   * graph an ajax call is made to this to fetch the graph data.
   *
   * Data Returned:
   * <pre>
   * {@code
   *   [
   *     {
   *       "flowtime": <Last job's finish time>,
   *       "score": 1000,
   *       "stagescores": [
   *         {
   *           "stageid:" "id",
   *           "stagescore": 500
   *         },
   *         {
   *           "stageid:" "id",
   *           "stagescore": 500
   *         }
   *       ]
   *     },
   *     {
   *       "flowtime": <Last job's finish time>,
   *       "score": 700,
   *       "stagescores": [
   *         {
   *           "stageid:" "id",
   *           "stagescore": 0
   *         },
   *         {
   *           "stageid:" "id",
   *           "stagescore": 700
   *         }
   *       ]
   *     }
   *   ]
   * }
   * </pre>
   */
  public static Result restJobGraphData(String jobDefId) {
    JsonArray datasets = new JsonArray();
    if (jobDefId == null || jobDefId.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<AppResult> results = getRestJobAppResults(jobDefId);

    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =
        ControllerUtil.limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for (int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int jobPerfScore = 0;
      JsonArray stageScores = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));
      for (AppResult appResult : mrJobsList) {

        // Each MR job triggered by jobDefId for flowExecId
        int mrPerfScore = 0;
        for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
          mrPerfScore += appHeuristicResult.score;
        }

        // A particular mr stage
        JsonObject stageScore = new JsonObject();
        stageScore.addProperty("stageid", appResult.id);
        stageScore.addProperty("stagescore", mrPerfScore);

        stageScores.add(stageScore);
        jobPerfScore += mrPerfScore;
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", Utils.getFlowTime(mrJobsList.get(mrJobsList.size() - 1)));
      dataset.addProperty("score", jobPerfScore);
      dataset.add("stagescores", stageScores);

      datasets.add(dataset);
    }

    JsonArray sortedDatasets = Utils.sortJsonArray(datasets);

    return ok(new Gson().toJson(sortedDatasets));
  }

  /**
   * The data for plotting the job history graph using time and resource metrics. While plotting the job history
   * graph an ajax call is made to this to fetch the graph data.
   *
   * Data Returned:
   * <pre>
   * [
   *  {
   *    "flowtime": 1461234105456,
   *    "runtime": 2312107,
   *    "waittime": 118879,
   *    "resourceused": 304934912,
   *    "resourcewasted": 172913,
   *    "jobmetrics": [
   *      {
   *        "stageid": "application_1458194917883_1587177",
   *        "runtime": 642986,
   *        "waittime": 14016,
   *        "resourceused": 277352448,
   *        "resourcewasted": 0
   *    }],
   *  },
   *  {
   *    "flowtime": 1461237538639,
   *    "runtime": 2155354,
   *    "waittime": 112187,
   *    "resourceused": 293096448,
   *    "resourcewasted": 400461,
   *    "jobmetrics": [
   *      {
   *        "stageid": "application_1458194917883_1589302",
   *        "runtime": 548924,
   *        "waittime": 16903,
   *        "resourceused": 266217472,
   *        "resourcewasted": 0
   *      }]
   *  }
   *  ]
   *
   * </pre>
   */
  public static Result restJobMetricsGraphData(String jobDefId) {
    JsonArray datasets = new JsonArray();
    if (jobDefId == null || jobDefId.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    List<AppResult> results = getRestJobAppResults(jobDefId);

    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =
        ControllerUtil.limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for (int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int jobPerfScore = 0;
      JsonArray stageMetrics = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));

      long totalMemoryUsed = 0;
      long totalMemoryWasted = 0;
      long totalDelay = 0;

      for (AppResult appResult : flowExecIdToJobsMap.get(flowExecPair)) {

        // Each MR job triggered by jobDefId for flowExecId
        int mrPerfScore = 0;

        for (AppHeuristicResult appHeuristicResult : appResult.yarnAppHeuristicResults) {
          mrPerfScore += appHeuristicResult.score;
        }

        // A particular mr stage
        JsonObject stageMetric = new JsonObject();
        stageMetric.addProperty("stageid", appResult.id);
        stageMetric.addProperty("runtime", appResult.finishTime - appResult.startTime);
        stageMetric.addProperty("waittime", appResult.totalDelay);
        stageMetric.addProperty("resourceused", appResult.resourceUsed);
        stageMetric.addProperty("resourcewasted", appResult.resourceWasted);

        stageMetrics.add(stageMetric);
        jobPerfScore += mrPerfScore;
        totalMemoryUsed += appResult.resourceUsed;
        totalMemoryWasted += appResult.resourceWasted;
      }

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", Utils.getFlowTime(mrJobsList.get(mrJobsList.size() - 1)));
      dataset.addProperty("runtime", Utils.getTotalRuntime(mrJobsList));
      dataset.addProperty("waittime", Utils.getTotalWaittime(mrJobsList));
      dataset.addProperty("resourceused", totalMemoryUsed);
      dataset.addProperty("resourcewasted", totalMemoryWasted);
      dataset.add("jobmetrics", stageMetrics);

      datasets.add(dataset);
    }

    JsonArray sortedDatasets = Utils.sortJsonArray(datasets);

    return ok(new Gson().toJson(sortedDatasets));
  }

  /**
   *
   * @param startTime - beginning of the time window
   * @param endTime - end of the time window
   * @return Json of resourceUsage data for each user for the given time window
   *    eg. [{"user":"bmr","resourceUsed":168030208,"resourceWasted":27262750},
   *        {"user":"payments","resourceUsed":18432,"resourceWasted":3447},
   *        {"user":"myu","resourceUsed":558211072,"resourceWasted":81573818}]
   */
  public static Result restResourceUsageDataByUser(String startTime, String endTime) {
    try {
      JsonArray datasets = new JsonArray();
      if(startTime.length() != endTime.length() ||
          (startTime.length() != 10 && startTime.length() != 13)) {
        return status(300);
      }
      SimpleDateFormat tf = null ;
      if( startTime.length() == 10 ) {
        tf = new SimpleDateFormat("yyyy-MM-dd");
      }
      else {
        tf = new SimpleDateFormat("yyyy-MM-dd-HH");
      }
      Date start = tf.parse(startTime);
      Date end = tf.parse(endTime);
      Collection<AppResourceUsageData> result = getUserResourceUsage(start, end);

      return ok(new Gson().toJson(result));
    }
    catch(ParseException ex) {
      return status(300,"Invalid datetime format : " + ex.getMessage());
    }
  }


  /**
   * Rest data to plot flot history graph using time and resource metrics. While plotting the flow history
   * graph an ajax call is made to this to fetch the graph data.
   * [
   * {
   *  "flowtime": 1461744881991,
   *  "runtime": 3190223,
   *  "waittime": 368011,
   *  "resourceused": 180488192,
   *  "resourcewasted": 0,
   *  "jobmetrics": [
   *          {
   *         "runtime": 3190223,
   *         "waittime": 368011,
   *         "resourceused": 180488192,
   *         "resourcewasted": 0,
   *         "jobdefurl": "sampleURL"
   *         "jobexecurl": "sampleURL"
   *          }
   *        ]
   * },
   * {
   *  "flowtime": 1461818409959,
   *  "runtime": 897490,
   *  "waittime": 100703,
   *  "resourceused": 12863488,
   *  "resourcewasted": 0,
   *  "jobmetrics": [
   *          {
   *         "runtime": 897490,
   *         "waittime": 100703,
   *         "resourceused": 12863488,
   *         "resourcewasted": 0,
   *         "jobdefurl": "sampleURL"
   *         "jobexecurl": "sampleURL"
   * }
   * ]
   *}
   *]
   **/
  public static Result restFlowMetricsGraphData(String flowDefId) {
    JsonArray datasets = new JsonArray();
    if (flowDefId == null || flowDefId.isEmpty()) {
      return ok(new Gson().toJson(datasets));
    }

    List<AppResult> results = getRestFlowAppResults(flowDefId);

    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =
        ControllerUtil.limitHistoryResults(ControllerUtil.groupJobs(results, ControllerUtil.GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for (int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int flowPerfScore = 0;
      JsonArray jobScores = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));
      Map<IdUrlPair, List<AppResult>> jobDefIdToJobsMap = ControllerUtil.groupJobs(mrJobsList, ControllerUtil.GroupBy.JOB_DEFINITION_ID);

      long totalFlowMemoryUsed = 0;
      long totalFlowMemoryWasted = 0;
      long totalFlowDelay = 0;
      long totalFlowRuntime = 0;
      // Compute the execution records. Note that each entry in the jobDefIdToJobsMap will have at least one AppResult
      for (IdUrlPair jobDefPair : jobDefIdToJobsMap.keySet()) {
        // Compute job perf score
        long totalJobMemoryUsed = 0;
        long totalJobMemoryWasted = 0;
        long totalJobDelay = 0;
        long totalJobRuntime = 0;

        totalJobRuntime = Utils.getTotalRuntime(jobDefIdToJobsMap.get(jobDefPair));
        totalJobDelay = Utils.getTotalWaittime(jobDefIdToJobsMap.get(jobDefPair));

        for (AppResult job : jobDefIdToJobsMap.get(jobDefPair)) {
          totalJobMemoryUsed += job.resourceUsed;
          totalJobMemoryWasted += job.resourceWasted;
        }

        // A job in jobscores list
        JsonObject jobScore = new JsonObject();
        jobScore.addProperty("runtime", totalJobRuntime);
        jobScore.addProperty("waittime", totalJobDelay);
        jobScore.addProperty("resourceused", totalJobMemoryUsed);
        jobScore.addProperty("resourcewasted", totalJobMemoryWasted);
        jobScore.addProperty("jobdefurl", jobDefPair.getUrl());
        jobScore.addProperty("jobexecurl", jobDefIdToJobsMap.get(jobDefPair).get(0).jobExecUrl);

        jobScores.add(jobScore);
        totalFlowMemoryUsed += totalJobMemoryUsed;
        totalFlowMemoryWasted += totalJobMemoryWasted;
      }

      totalFlowDelay = Utils.getTotalWaittime(flowExecIdToJobsMap.get(flowExecPair));
      totalFlowRuntime = Utils.getTotalRuntime(flowExecIdToJobsMap.get(flowExecPair));

      // Execution record
      JsonObject dataset = new JsonObject();
      dataset.addProperty("flowtime", Utils.getFlowTime(mrJobsList.get(mrJobsList.size() - 1)));
      dataset.addProperty("runtime", totalFlowRuntime);
      dataset.addProperty("waittime", totalFlowDelay);
      dataset.addProperty("resourceused", totalFlowMemoryUsed);
      dataset.addProperty("resourcewasted", totalFlowMemoryWasted);
      dataset.add("jobmetrics", jobScores);

      datasets.add(dataset);
    }

    JsonArray sortedDatasets = Utils.sortJsonArray(datasets);

    return ok(new Gson().toJson(sortedDatasets));
  }

  /** Rest Api implementation for providing the tuning parameters details for a job
   * @param jobId - job execution id passed as a parameter for the GET request
   * @result JSON containing suggested tuning paramter details
   * eg: Formatted JSON Data
   * {
   *   "tunein":{
   *   "id":"https://localhost:9080/executor?execid=1234567&job=sampleJob&attempt=0",
   *   "jobSuggestedParamSetId":1234,
   *   "tuningAlgorithmId":2,
   *   "autoApply":false,
   *   "tuningAlgorithm":"HBT",
   *   "tuningAlgorithms":[
   *      {
   *         "name":"HBT"
   *      },
   *      {
   *         "name":"OBT"
   *      }
   *   ],
   *   "iterationCount":10,
   *   "tuningParameters":[
   *      {
   *         "name":"mapreduce.map.memory.mb",
   *         "jobSuggestedParamValue":"1113.33",
   *         "userSuggestedParamValue":"1113.33",
   *         "currentParamValue":"1113.33"
   *      },
   *      {
   *         "name":"mapreduce.map.java.opts",
   *         "jobSuggestedParamValue":"839",
   *         "userSuggestedParamValue":"839",
   *         "currentParamValue":"839"
   *      }
   *   ]
   * }
   *}
   **/
  public static Result getTuningParameter(String jobId) {
    logger.info("Getting TuneIn Parameters for " + jobId);
    JsonObject parent = new JsonObject();
    JsonObject tuneIn = new JsonObject();
    try {
      JobExecution jobExecution = JobExecution.find.select("*")
          .where()
          .eq(JobExecution.TABLE.jobExecId, jobId)
          .findUnique();
      Integer jobDefinitionId = 0;
      if (jobExecution == null) {
        throw new Exception("No Job Execution entry found for job execution: " + jobId);
      } else {
        jobDefinitionId = jobExecution.job.id;
      }
      logger.info("Job DefinitionId:  " + jobDefinitionId);
      JobSuggestedParamSet jobSuggestedParamSet = JobSuggestedParamSet.find.select("*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id , jobDefinitionId)
          .eq(JobSuggestedParamSet.TABLE.isParamSetSuggested, true)
          .order()
          .desc(JobSuggestedParamSet.TABLE.createdTs)
          .setMaxRows(1)
          .findUnique();

      JobSuggestedParamSet userSuggestedParamSet = JobSuggestedParamSet.find.select("*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id , jobDefinitionId)
          .eq(JobSuggestedParamSet.TABLE.isManuallyOverridenParameter, true)
          .order()
          .desc(JobSuggestedParamSet.TABLE.createdTs)
          .setMaxRows(1)
          .findUnique();

      TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find
          .select("*")
          .where()
          .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinitionId)
          .order()
          .desc(TuningJobDefinition.TABLE.createdTs)
          .findUnique();
      logger.info("Respective JobSuggestedParamSet: " + jobSuggestedParamSet.id);
      if (userSuggestedParamSet == null) {
        logger.info("No user suggested param set for jobDefinitionId: "  + jobDefinitionId);
      } else {
        logger.info("User suggested param set: " + userSuggestedParamSet.id);
      }
      logger.info("Tuning Job Definition Id: " + tuningJobDefinition.job.id);
      Boolean autoApply = tuningJobDefinition.autoApply;
      TuningAlgorithm tuningAlgorithm = jobSuggestedParamSet.tuningAlgorithm;
      List<TuningParameter> parametersList = getTuningParametersListForJob(tuningAlgorithm.id);
      JsonArray tuningParameters = getTuningParameterDetails(parametersList, jobSuggestedParamSet, userSuggestedParamSet);
      String currentTuningAlgorithm = getCurrentTuningAlgorithmName(tuningAlgorithm);
      JsonArray tuningAlgorithmList = new JsonArray();
      tuneIn.addProperty(ID, jobId);
      tuneIn.addProperty(JOB_DEFINTITION_ID, jobDefinitionId);
      //Two tuning types {HBT, OBT}
      JsonObject hbtAlgo = new JsonObject();
      hbtAlgo.addProperty(NAME, TuningType.HBT.name());
      JsonObject obtAlgo = new JsonObject();
      obtAlgo.addProperty(NAME, TuningType.OBT.name());
      tuningAlgorithmList.add(hbtAlgo);
      tuningAlgorithmList.add(obtAlgo);
      tuneIn.addProperty(JOB_SUGGESTED_PARAM_SET_ID, jobSuggestedParamSet.id);
      tuneIn.addProperty(TUNING_ALGORITHM_ID, tuningAlgorithm.id);
      tuneIn.addProperty(AUTO_APPLY, autoApply);
      tuneIn.addProperty(TUNING_ALGORITHM, currentTuningAlgorithm);
      tuneIn.addProperty(REASON_TO_DISABLE_TUNING, reasonForDisablingTuning(tuningJobDefinition));
      tuneIn.add(TUNING_ALGORITHM_LIST, tuningAlgorithmList);
      tuneIn.addProperty(ITERATION_COUNT,tuningJobDefinition.numberOfIterations);
      tuneIn.add(TUNING_PARAMETERS, tuningParameters);
      parent.add(TUNEIN, tuneIn);
      if (logger.isDebugEnabled()) {
        logger.debug("TuneIn Json: " + tuneIn);
      }
      return ok(new Gson().toJson(parent));
    } catch (Exception ex) {
      logger.error(ex);
      tuneIn.addProperty(JOB_SUGGESTED_PARAM_SET_ID, "null");
      parent.add(TUNEIN, tuneIn);
      return ok(new Gson().toJson(parent));
    }
  }

  public static Result updateShowRecommendationCount() {
    JsonNode requestBodyRoot = request().body().asJson();
    Integer jobDefinitionId = requestBodyRoot.path("id").asInt();
    TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.select("*")
        .where()
        .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinitionId)
        .findUnique();
    if (tuningJobDefinition.showRecommendationCount == null) {
      tuningJobDefinition.showRecommendationCount = 0;
    }
    tuningJobDefinition.showRecommendationCount += 1;
    tuningJobDefinition.save();
    JsonObject parent = new JsonObject();
    parent.addProperty(SHOW_RECOMMENDATION_COUNT, tuningJobDefinition.showRecommendationCount);
    return ok(new Gson().toJson(parent));
  }

  public static Result getJobBestParameter(String jobDefId) {
    JobDefinition jobDefinition = JobDefinition.find
        .select("*")
        .where()
        .eq(JobDefinition.TABLE.jobDefId, jobDefId)
        .findUnique();
    if (jobDefinition == null) {
      logger.info("No Tuning Job found for jobDefUrl: " + jobDefId);
      return notFound("No job found for provided Job Definition Id");
    }
    JobSuggestedParamSet bestJobSuggestedParamSet = JobSuggestedParamSet.find
        .select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + '.' + JobDefinition.TABLE.id, jobDefinition.id)
        .eq(JobSuggestedParamSet.TABLE.isParamSetBest, 1)
        .findUnique();
    if (bestJobSuggestedParamSet == null) {
      logger.info("No best Param set found for jobDefitionId: "  + jobDefinition.id);
      return notFound("No Best Param found");
    }
    logger.info("JobSuggestedParamSetId for jobDefId : " + jobDefId + " is " + bestJobSuggestedParamSet.id);
    Map<String, String> bestParamValueMap = formatSparkOutput(getSparkParamsMap(bestJobSuggestedParamSet.id));
    TuningJobExecutionParamSet bestTuningJobExecutionParamSet = TuningJobExecutionParamSet.find
        .select("*")
        .where()
        .eq(TuningJobExecutionParamSet.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id, bestJobSuggestedParamSet.id)
        .order()
        .desc(TuningJobExecutionParamSet.TABLE.jobExecution + '.' + JobExecution.TABLE.id)
        .setMaxRows(1)
        .findUnique();
    bestParamValueMap.put("executionId", bestTuningJobExecutionParamSet.jobExecution.jobExecUrl);
    return ok(Json.toJson(bestParamValueMap));
  }

  /** Rest Api implementation for authenticating the user using Scheduler's Authentication API
   * @result JSON containing session.id on successful authentication else the error message
   * eg: Formatted JSON Data
   * {
   *  "status" : "success"
   *  "session_id" : "4b52bfe1-5a22-9a56-8ced-955dd5b9f0ea"
   *}
   **/
  public static Result authenticateUser() {
    DynamicForm form = Form.form().bindFromRequest(request());
    final String username = form.get(USERNAME);
    final String password = form.get(PASSWORD);
    final String schedulerUrl = form.get(SCHEDULER_URL);
    logger.info("Authenticating user: " + username + " " + schedulerUrl);
    if (!isSet(username) || !isSet(password)) {
      return badRequest("Username or Password cannot be empty");
    } else if (!isSet(schedulerUrl)) {
      return badRequest("Scheduler Url is empty!!");
    }
    Map<String, String> response;
    try {
      response = authenticateUser(username, password, schedulerUrl + AZKABAN_AUTHENTICATION_URL_SUFFIX);
    } catch (Exception ex) {
      logger.error("Some error occured while authenticating the user ", ex);
      return internalServerError();
    }
    return ok(Json.toJson(response));
  }

  /** Rest Api implementation for providing the status of user's authorization to WRITE
   * to a respective project of a Scheduler
   *
   * @param jobDefId - JobDefintion Id specific to the project
   * @param schedulerUrl - The url of the scheduler to which the job belongs
   * @param sessionId - the scheduler session_id of the user
   * @result JSON containing a Boolean (hasWritePermission) which tells if
   * the user has WRITE access to the project
   * eg: Formatted JSON Data
   * {
   *  "hasWritePermission" : "true"
   *}
   **/
  public static Result getUserAuthorizationStatus(String sessionId, String jobDefId, String schedulerUrl) {
    logger.info("Checking for user authorization");
    if (!isSet(sessionId) || !isSet(jobDefId) || !isSet(schedulerUrl)) {
      return badRequest("SessionId or JobDefId or SchedulerUrl cannot be empty");
    }
    final Map<String, String> responseMap = new HashMap();
    try {
      Map<String, String> queryParams = new HashMap();
      queryParams.put(AZKABAN_SESSION_ID_KEY, sessionId);
      queryParams.put(AJAX, AUTHORIZATION_AJAX_ENDPOINT);
      String projectName = getProjectName(jobDefId);
      if (projectName != null) {
        queryParams.put(PROJECT_KEY, projectName);
      } else {
        return badRequest("Job Definition doesn't contain Project name");
      }
      HttpResponse response = httpGetCall(schedulerUrl + AZKABAN_AUTHORIZATION_URL_SUFFIX, queryParams);
      HttpEntity entity = response.getEntity();
      handleAuthorizationResponse(entity, responseMap);
    } catch (Exception ex) {
      logger.error("Error while fetching User's Project Authorization status ",ex);
      return internalServerError("Something went wrong while authorizing the user");
    }
    return ok(Json.toJson(responseMap));
  }

  /** Rest API implementation for updating the TuneIn details like Tuning Param's value, AutoApply or #iterations
   *
   * @result JSON containing updated Tuning Params if params are changed and current value of AutoApply amd iterationCount
   *
   * {"updatedParamDetails":
   *    {
   *      "userSuggestedParamSetId":1501,
   *      "tuningParameters":
   *          [{
   *            "paramId":21,
   *           "name":"spark.driver.memory",
   *           "currentParamValue":1512.04
   *          },
   *
   *          {
   *             "paramId":22,
   *             "name":"spark.executor.cores",
   *             "currentParamValue":90.0
   *           },
   *           {
   *              "paramId":20,
   *              "name":"spark.executor.memory",
   *              "currentParamValue":7949.0
   *            },
   *            {
   *               "paramId":23,
   *               "name":"spark.memory.fraction",
   *               "currentParamValue":0.14
   *             }
   *           ]
   *    },
   *    "autoApply":true,
   *    "iterationCount":20
   *  }
   **/
  public static Result updateTuneinDetails() {
    JsonNode requestBodyRoot = request().body().asJson();
    JsonNode tunein = requestBodyRoot.path(TUNEIN);
    JsonNode job = requestBodyRoot.path(JOB);
    String userName = requestBodyRoot.path(USERNAME).asText();
    JsonNode tuningParameters = tunein.path(TUNING_PARAMETERS);
    Boolean autoApply = tunein.path(AUTO_APPLY).asBoolean();
    Integer jobDefinitionId = tunein.path(JOB_DEFINTITION_ID).asInt();
    String jobType = job.path(JsonKeys.JOB_TYPE).asText().toUpperCase();
    int iterationCount = tunein.path(ITERATION_COUNT).asInt();
    JsonObject parent = new JsonObject();
    logger.info(String.format("Updating TuneIn details for jobDefId %s on the request of %s",
        jobDefinitionId, userName));
    try {
      logger.info("Validating the TuneIn details");
      validateTuneInDetails(tunein);
    } catch (IllegalArgumentException ex) {
      logger.error(ex);
      return badRequest(ex.getMessage());
    }
    try {
      if (isJobParamUpdated(tuningParameters)) {
        logger.info("Job Params are changed for jobDefinitionId " + jobDefinitionId);
        parent.add(UPDATED_PARAM_DETAILS, updateJobParams(tunein, jobType));
      }
      TuningJobDefinition tuningJobDefinition = TuningJobDefinition.find.where()
          .eq(TuningJobDefinition.TABLE.job + '.' + JobDefinition.TABLE.id, jobDefinitionId)
          .findUnique();
      updateAutoApplyProperty(tuningJobDefinition, autoApply);
      updateIterationCount(tuningJobDefinition, iterationCount);
      tuningJobDefinition.update();
      parent.addProperty(AUTO_APPLY, tuningJobDefinition.autoApply);
      parent.addProperty(ITERATION_COUNT, tuningJobDefinition.numberOfIterations);
    } catch (Exception ex) {
      logger.error("Something went wrong while updating Tunein details", ex);
      return internalServerError();
    }
    logger.info("TuneIn update was successful");
    return ok(new Gson().toJson(parent));
  }

  /**
   * Returns a list of AppResults after quering the FLOW_EXEC_ID from the database
   * @return The list of AppResults
   */
  private static List<AppResult> getRestJobAppResults(String jobDefId) {
    List<AppResult> results = AppResult.find.select(
        AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
        .where()
        .eq(AppResult.TABLE.JOB_DEF_ID, jobDefId)
        .order()
        .desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(JOB_HISTORY_LIMIT)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .findList();

    return results;
  }

  /**
   * Returns the list of AppResults after quering the FLOW_DEF_ID from the database
   * @return The list of AppResults
   */
  private static List<AppResult> getRestFlowAppResults(String flowDefId) {
    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<AppResult> results = AppResult.find.select("*")
        .where()
        .eq(AppResult.TABLE.FLOW_DEF_ID, flowDefId)
        .order()
        .desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(JOB_HISTORY_LIMIT)
        .findList();

    return results;
  }

  private static class AppResourceUsageData {
    String user;
    double resourceUsed;
    double resourceWasted;
  }

  /**
   * Returns the list of users with their resourceUsed and resourceWasted Data for the given time range
   * @return list of AppResourceUsageData
   **/
  private static Collection<AppResourceUsageData> getUserResourceUsage(Date start, Date end) {
    long resourceUsed = 0;
    Map<String, AppResourceUsageData> userResourceUsage = new HashMap<String, AppResourceUsageData>();
    // Fetch all the appresults for the given time range [startTime, endTime).
    List<AppResult> results = AppResult.find.select("*")
        .where()
        .ge(AppResult.TABLE.START_TIME, start.getTime())
        .lt(AppResult.TABLE.START_TIME, end.getTime()).findList();

    // aggregate the resourceUsage data at the user level
    for (AppResult result : results) {
      if (!userResourceUsage.containsKey(result.username)) {
        AppResourceUsageData data = new AppResourceUsageData();
        data.user = result.username;
        userResourceUsage.put(result.username, data);
      }
      userResourceUsage.get(result.username).resourceUsed += Utils.MBSecondsToGBHours(result.resourceUsed);
      userResourceUsage.get(result.username).resourceWasted += Utils.MBSecondsToGBHours(result.resourceWasted);
    }

    return userResourceUsage.values();
  }

  private static List<TuningParameter> getTuningParametersListForJob(Integer tuningAlgorithmId) {
    List<TuningParameter> parametersList =
        TuningParameter.find.select("*")
            .where()
            .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id, tuningAlgorithmId)
            .order()
            .asc(TuningParameter.TABLE.paramName)
            .findList();
    logger.info("Size of paramList " + parametersList.size());
    return parametersList;
  }

  private static JsonArray getTuningParameterDetails(List<TuningParameter> parametersList,
      JobSuggestedParamSet jobSuggestedParamSet, JobSuggestedParamSet userSuggestedParamSet) {
    JsonArray tuningParameters = new JsonArray();
    DecimalFormat truncateParamValueFormat = new DecimalFormat("#.##");
    for (TuningParameter tuningParam : parametersList) {
      String paramName = tuningParam.paramName;
      Integer id = tuningParam.id;
      JobSuggestedParamValue userSuggestedParameterValue = null;
      if (userSuggestedParamSet != null) {
        userSuggestedParameterValue = JobSuggestedParamValue.find.select("*")
            .where()
            .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.id, userSuggestedParamSet.id)
            .eq(JobSuggestedParamValue.TABLE.tuningParameter + "." + TuningParameter.TABLE.id, id)
            .findUnique();
      }

      JobSuggestedParamValue jobSuggestedParameterValue = JobSuggestedParamValue.find.select("*")
          .where()
          .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + "." + JobSuggestedParamSet.TABLE.id, jobSuggestedParamSet.id)
          .eq(JobSuggestedParamValue.TABLE.tuningParameter + "." + TuningParameter.TABLE.id, id)
          .findUnique();

      if (jobSuggestedParameterValue == null) {
        logger.info("No Job Suggested Param Value");
        continue;
      }
      JsonObject param = new JsonObject();
      param.addProperty(PARAM_ID,id);
      param.addProperty(NAME, paramName);
      param.addProperty(JOB_SUGGESTED_PARAM_VALUE,  truncateParamValueFormat.format(jobSuggestedParameterValue.paramValue));
      if (userSuggestedParameterValue != null) {
        param.addProperty(USER_SUGGESTED_PARAM_VALUE,  truncateParamValueFormat.format(userSuggestedParameterValue.paramValue));
        param.addProperty(CURRENT_PARAM_VALUE,  truncateParamValueFormat.format(userSuggestedParameterValue.paramValue));
      } else {
        param.addProperty(USER_SUGGESTED_PARAM_VALUE,  truncateParamValueFormat.format(jobSuggestedParameterValue.paramValue));
        param.addProperty(CURRENT_PARAM_VALUE,  truncateParamValueFormat.format(jobSuggestedParameterValue.paramValue));
      }
      tuningParameters.add(param);
    }
    return tuningParameters;
  }

  private static String getCurrentTuningAlgorithmName(TuningAlgorithm tuningAlgorithm) {
    return tuningAlgorithm.optimizationAlgo.name().contains(AlgorithmType.PSO_IPSO.toString()) ? TuningType.OBT.name()
        : TuningType.HBT.name();
  }

  private static String getOptimizationAlgo(String tuningAlgorithmName) {
    //Currently OBT's default optimization Algo is PSO_IPSO
    return tuningAlgorithmName.equals(TuningType.OBT.name()) ? AlgorithmType.PSO_IPSO.name() :
        AlgorithmType.HBT.name();
  }

  private static String reasonForDisablingTuning(TuningJobDefinition tuningJobDefinition) {
    if (!tuningJobDefinition.tuningEnabled) {
      logger.debug("Tuning is disabled for this application");
      if (tuningJobDefinition.tuningDisabledReason != null) {
        return tuningJobDefinition.tuningDisabledReason;
      } else {
        logger.info("No Reason provided to disable tuning for this application");
      }
    }
    return "NONE";
  }

  private static Map<String, Double> getSparkParamsMap(Long jobSuggestedParamSetId) {
    logger.debug("Fetching params for JobSuggestedParamSet id: " + jobSuggestedParamSetId);
    List<JobSuggestedParamValue> paramValues = JobSuggestedParamValue.find.select("*")
        .where()
        .eq(JobSuggestedParamValue.TABLE.jobSuggestedParamSet + '.' + JobSuggestedParamSet.TABLE.id,
            jobSuggestedParamSetId)
        .findList();
    Map<String, Double> paramValueMap = new HashMap();
    DecimalFormat truncateParamValueFormat = new DecimalFormat("#.##");
    for (JobSuggestedParamValue param: paramValues) {
      paramValueMap.put(param.tuningParameter.paramName,
          Double.parseDouble(truncateParamValueFormat.format(param.paramValue)));
    }
    return paramValueMap;
  }

  private static Map<String, String> authenticateUser(String username, String password, String url) throws Exception {
    logger.info("Making authentication call to " + url);
    Map<String, String> postCallEntityParams = new HashMap();
    postCallEntityParams.put(USERNAME, username);
    postCallEntityParams.put(PASSWORD, password);
    HttpResponse response = httpPostCall(url, postCallEntityParams);
    HttpEntity entity = response.getEntity();
    Map<String, String> responseMap = new HashMap();
    if (entity != null) {
      String responseEntity = EntityUtils.toString(entity);
      JSONObject result = new JSONObject(responseEntity);
      if (result.has(ERROR_KEY)) {
        responseMap.put(ERROR_KEY, result.get(ERROR_KEY).toString());
      } else {
        logger.info(result.get(STATUS));
        if (result.get(STATUS) != null && result.get(STATUS).toString().equals(SUCCESS)) {
          responseMap.put(STATUS, result.get(STATUS).toString());
          responseMap.put(SESSION_ID_KEY, result.get(AZKABAN_SESSION_ID_KEY).toString());
        }
      }
    } else {
      logger.error("Empty response entity encountered while authenticating " + username);
      responseMap.put(ERROR_KEY, "Something went wrong while processing the request");
    }
    return responseMap;
  }

  private static HttpResponse httpGetCall(String url, Map<String,String> queryParams)
      throws IOException, URISyntaxException {
    if (url == null) {
      throw new IllegalArgumentException("URL cannot be NULL for GET call");
    }
    URIBuilder builder = new URIBuilder(url);
    logger.info("Making a GET call to URL " + url);
    if (queryParams != null) {
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        builder.setParameter(entry.getKey(), entry.getValue());
      }
    }
    URI getUri = builder.build();
    HttpGet request = new HttpGet(getUri);
    CloseableHttpClient client = HttpClients.createDefault();
    return client.execute(request);
  }

  private static HttpResponse httpPostCall(String url, Map<String,String> postParams)
      throws IOException {
    if (!isSet(url)) {
      throw new IllegalArgumentException("URL cannot be NULL for POST call");
    }
    logger.info("Making a POST call to URL " + url);
    List<NameValuePair> postEntity = new ArrayList();
    if (postParams != null) {
      for (Map.Entry<String, String> entry : postParams.entrySet()) {
        postEntity.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
      }
    }
    HttpPost httpPost = new HttpPost(url);
    httpPost.setEntity(new UrlEncodedFormEntity(postEntity));
    CloseableHttpClient client = HttpClients.createDefault();
    return client.execute(httpPost);
  }

  /**
   * Method for extracting the project name from jobDefId which is a URL
   * @param jobDefId Job Definition Id
   * @return Project name if exists in the JobDefId else returns `null`
   */
  private static String getProjectName(String jobDefId) throws URISyntaxException {
    List<NameValuePair> jobDefQueryParams = URLEncodedUtils.parse(new java.net.URI(jobDefId), "UTF-8");
    for (NameValuePair param: jobDefQueryParams) {
      if (param.getName().equals(PROJECT_KEY)) {
        return param.getValue();
      }
    }
    return null;
  }

  /**
   * This method is responsible for parsing the response for Authorization call to Scheduler and then populate the
   * response for the TuneIn's Authorization API
   * @param entity  Response entity from Scheduler Authorization call
   * @param responseMap Map containing response for the authorization API
   */
  private static void handleAuthorizationResponse(HttpEntity entity, final Map<String, String> responseMap)
      throws IOException, JSONException {
    if (entity != null) {
      JSONObject result = new JSONObject(EntityUtils.toString(entity));
      if (result.has(ERROR_KEY)) {
        responseMap.put(ERROR_KEY, result.get(ERROR_KEY).toString());
      } else if (result.get(IS_USER_AUTHORISED_KEY) != null){
        responseMap.put(IS_USER_AUTHORISED_KEY, result.get(IS_USER_AUTHORISED_KEY).toString());
      }
    }
  }

  /**
   * Returns a Boolean value, true is user changed the tuning parameters' values else return false
   * @return  The Boolean type
   */
  private static Boolean isJobParamUpdated(JsonNode tuningParameters) {
    boolean isParamChanged = false;
    for(JsonNode parameter: tuningParameters) {
      String paramName = parameter.path(NAME).asText();
      // this is the currently saved value in the system for the param
      Double currentValue = parameter.path(CURRENT_PARAM_VALUE).asDouble();
      // this is the param value suggested by user and from now on Job will be executed with this param value
      Double userSuggestedParamValue = parameter.path(USER_SUGGESTED_PARAM_VALUE).asDouble();
      if(!userSuggestedParamValue.equals(currentValue) ) {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("Param %s is changed from %f to %f", paramName, currentValue,
              userSuggestedParamValue));
        }
        isParamChanged = true;
        // If any one of the param is modified then we need to create new Param set for all the params
        break;
      }
    }
    return isParamChanged;
  }

  /**
   * Returns a JsonObject with the updated Tuning Params and the newly created JobSuggestedParamSet Id
   * @return JsonObject - Json containing the updated param values and
   *            the user_suggested_param_set id
   */
  private static JsonObject updateJobParams(JsonNode tunein, String jobType) {
    Integer jobDefinitionId = tunein.path(JOB_DEFINTITION_ID).asInt();
    String optimizationAlgo = getOptimizationAlgo(tunein.path(TUNING_ALGORITHM).asText());
    JsonNode tuningParameters = tunein.path(TUNING_PARAMETERS);
    JobDefinition jobDefinition = JobDefinition.find.select("*")
        .where()
        .eq(JobDefinition.TABLE.id, jobDefinitionId)
        .findUnique();
    TuningAlgorithm tuningAlgorithm = TuningAlgorithm.find.select("*")
        .where()
        .eq(TuningAlgorithm.TABLE.jobType, jobType)
        .eq(TuningAlgorithm.TABLE.optimizationAlgo, optimizationAlgo)
        .findUnique();
    //Creating new param set for the user suggested param values
    JobSuggestedParamSet userSuggestedParamSet = createUserSuggestedParamSet(jobDefinition, tuningAlgorithm);
    //Creating new parameter values suggested by the user for the new Param set
    JsonObject paramDetails = new JsonObject();
    JsonArray updatedTuningParams = createUserSuggestedParamValue(tuningParameters, userSuggestedParamSet);
    paramDetails.addProperty(USER_SUGGESTED_PARAM_SET_ID, userSuggestedParamSet.id);
    paramDetails.add(TUNING_PARAMETERS, updatedTuningParams);
    return paramDetails;
  }

  /**
   * Returns a JobSuggestedParamSet which is created in the database when the user changed the tuning parameters' values
   * @return The JobSuggestedParamSet
   */
  private static JobSuggestedParamSet createUserSuggestedParamSet(JobDefinition jobDefinition,
      TuningAlgorithm tuningAlgorithm) {
    JobSuggestedParamSet userSuggestedParamSet = new JobSuggestedParamSet();
    userSuggestedParamSet.jobDefinition = jobDefinition;
    userSuggestedParamSet.tuningAlgorithm = tuningAlgorithm;
    userSuggestedParamSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.CREATED;
    userSuggestedParamSet.isParamSetDefault = false;
    userSuggestedParamSet.isParamSetBest = false;
    userSuggestedParamSet.isParamSetSuggested = false;
    userSuggestedParamSet.areConstraintsViolated = false;
    userSuggestedParamSet.isManuallyOverridenParameter = true;
    userSuggestedParamSet.save();
    logger.info("Latest created userSuggestedParamSetId: " + userSuggestedParamSet.id);
    discardPreviousCreatedParamSet(jobDefinition.id,userSuggestedParamSet.id);
    return userSuggestedParamSet;
  }

  /**
   * Function to discard the existing JobSuggestedParamSet till now
   */
  private static void discardPreviousCreatedParamSet(int jobDefinitionId, Long userCreatedParamSetId) {
    List<JobSuggestedParamSet> jobSuggestedParamSetList = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinitionId)
        .ne(JobSuggestedParamSet.TABLE.id, userCreatedParamSetId)
        .findList();
    //Marking all the ParamSetStatus as discarded except the latest userSuggestedParamSet
    for (JobSuggestedParamSet paramSet : jobSuggestedParamSetList) {
      logger.info("Discarding param set id: " + paramSet.id);
      paramSet.paramSetState = JobSuggestedParamSet.ParamSetStatus.DISCARDED;
      paramSet.update();
    }
  }

  /**
   * Create the new parameter values in JobSuggestedParameterValue in the database
   */
  private static JsonArray createUserSuggestedParamValue(JsonNode tuningParameters,
      JobSuggestedParamSet userSuggestedParamSet) {
    logger.info("Creating user suggested Param Values");
    JsonArray updateTuningParams = new JsonArray();
    for(JsonNode param: tuningParameters) {
      JsonObject tuningParam = new JsonObject();
      Integer tuningParameterId = param.path(PARAM_ID).asInt();
      Double paramValue = param.path(USER_SUGGESTED_PARAM_VALUE).asDouble();
      TuningParameter tuningParameter = TuningParameter.find
          .select("*")
          .where()
          .eq(TuningParameter.TABLE.id, tuningParameterId)
          .findUnique();
      JobSuggestedParamValue userSuggestedParamValue = new JobSuggestedParamValue();
      userSuggestedParamValue.paramValue = paramValue;
      userSuggestedParamValue.jobSuggestedParamSet = userSuggestedParamSet;
      userSuggestedParamValue.tuningParameter = tuningParameter;
      userSuggestedParamValue.save();
      tuningParam.addProperty(PARAM_ID, userSuggestedParamValue.tuningParameter.id);
      tuningParam.addProperty(NAME, userSuggestedParamValue.tuningParameter.paramName);
      tuningParam.addProperty(CURRENT_PARAM_VALUE, userSuggestedParamValue.paramValue);
      updateTuningParams.add(tuningParam);
    }
    return updateTuningParams;
  }

  /**
   * Update autoApply property in TuningJobDefintion table as per the user suggestion if modified
   * @param autoApply - Boolean value for the autoapply
   * @param tuningJobDefinition - Tuning Job Definition for the respective Job
   */
  private static void updateAutoApplyProperty(TuningJobDefinition tuningJobDefinition,
      Boolean autoApply) {
    if(tuningJobDefinition.autoApply != autoApply) {
      tuningJobDefinition.autoApply = autoApply;
      logger.info(String.format("Changing AutoApply for %s to  %s", tuningJobDefinition.job.id, autoApply));
    }
  }

  /**
   * Update iterationCount as per the user suggestion if modified in TuningJobDefintion table
   * @param numberOfIteration - Iteration count
   * @param tuningJobDefinition - Tuning Job Definition for the respective Job
   */
  private static void updateIterationCount(TuningJobDefinition tuningJobDefinition,
      int numberOfIteration) {
    if (tuningJobDefinition.numberOfIterations != numberOfIteration) {
      tuningJobDefinition.numberOfIterations = numberOfIteration;
      logger.info(String.format("Changing the #iteration as per user suggestion for %d to %d",
          tuningJobDefinition.job.id, tuningJobDefinition.numberOfIterations));
    }
  }

  /**
   * Method to validate the tunein details received as param for Update request
   * Here it is validated if the tuneIn params are of expected dataType and
   * adhere to the range mentioned in TuningParameters
   * @param tunein - TuneIn model which may be modified by the user
   */
  private static void validateTuneInDetails(JsonNode tunein) {
    validateJobParameters(tunein.path(TUNING_PARAMETERS));
    // validate iteration count's datatype
    try {
      Integer.parseInt(tunein.path(ITERATION_COUNT).asText());
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Iteration count is not of type Integer");
    }
  }

  /**
   * To validate the tunein params are of expected dataType and
   * adhere to the range mentioned in TuningParameters
   * @param jobParameters - TuneIn model which may be modified by the user
   */
  private static void validateJobParameters(JsonNode jobParameters) {
    for(JsonNode parameter: jobParameters) {
      int paramId = parameter.path(PARAM_ID).asInt();
      String paramName = parameter.path(NAME).asText();
      TuningParameter tuningParam = getTuningParameter(paramId);
      Double userSuggestedParamValue;
      try {
        userSuggestedParamValue = Double.parseDouble(parameter.path(USER_SUGGESTED_PARAM_VALUE).asText());
      } catch (NumberFormatException nfe){
        throw new IllegalArgumentException(String.format
            ("Param value for \"%s\" doesn't adhere to expected Datatype", paramName));
      }
      if ((userSuggestedParamValue) < tuningParam.minValue ||
          (userSuggestedParamValue > tuningParam.maxValue) ) {
        throw new IllegalArgumentException(String.format("Param value for \"%s\" is not in the range [%.2f, %.2f]",
            paramName, tuningParam.minValue, tuningParam.maxValue));
      }
    }
  }

  /**
   * Fetch the TuningParamter for a respective id
   * @param paramId - Id of the parameter
   */
  private static TuningParameter getTuningParameter(int paramId) {
    TuningParameter tuningParameter = TuningParameter.find.select("*")
        .where()
        .eq(TuningParameter.TABLE.id, paramId)
        .findUnique();
    return tuningParameter;
  }

}
