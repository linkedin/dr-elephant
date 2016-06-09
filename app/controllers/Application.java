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
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import play.api.Play;
import play.api.templates.Html;
import play.data.DynamicForm;
import play.data.Form;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import views.html.page.comparePage;
import views.html.page.flowHistoryPage;
import views.html.page.helpPage;
import views.html.page.homePage;
import views.html.page.jobHistoryPage;
import views.html.page.searchPage;
import views.html.results.compareResults;
import views.html.results.flowDetails;
import views.html.results.flowHistoryResults;
import views.html.results.jobDetails;
import views.html.results.jobHistoryResults;
import views.html.results.searchResults;
import com.google.gson.*;


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
  public static final String USERNAME = "username";
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

  private static long _lastFetch = 0;
  private static int _numJobsAnalyzed = 0;
  private static int _numJobsCritical = 0;
  private static int _numJobsSevere = 0;

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
      _lastFetch = now;
    }

    // Fetch only required fields for jobs analysed in the last 24 hours up to a max of 50 jobs
    List<AppResult> results = AppResult.find
        .select(AppResult.getSearchFields())
        .where()
        .gt(AppResult.TABLE.FINISH_TIME, finishDate)
        .order().desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(50)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        .findList();

    return ok(homePage.render(_numJobsAnalyzed, _numJobsSevere, _numJobsCritical,
        searchResults.render("Latest analysis", results)));
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
    String flowExecId = form.get(FLOW_EXEC_ID);
    flowExecId = (flowExecId != null) ? flowExecId.trim() : null;

    // Search and display job details when job id or flow execution url is provided.
    if (!appId.isEmpty()) {
      AppResult result = AppResult.find.select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
              + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
          .where()
          .idEq(appId).findUnique();
      return ok(searchPage.render(null, jobDetails.render(result)));
    } else if (flowExecId != null && !flowExecId.isEmpty()) {
      List<AppResult> results = AppResult.find
          .select(AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_EXEC_ID)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId)
          .findList();
      Map<IdUrlPair, List<AppResult>> map = groupJobs(results, GroupBy.JOB_EXECUTION_ID);
      return ok(searchPage.render(null, flowDetails.render(flowExecId, map)));
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
    List<AppResult> results = query
        .setFirstRow((paginationBarStartIndex - 1) * pageLength)
        .setMaxRows((paginationStats.getPageBarLength() - 1) * pageLength + 1)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        .findList();
    paginationStats.setQueryString(getQueryString());
    if (results.isEmpty() || currentPage > paginationStats.computePaginationBarEndIndex(results.size())) {
      return ok(searchPage.render(null, jobDetails.render(null)));
    } else {
      return ok(searchPage.render(paginationStats,
          searchResults.render("Results",
              results.subList((currentPage - paginationBarStartIndex) * pageLength, Math.min(results.size(),
                  (currentPage - paginationBarStartIndex + 1) * pageLength)))));
    }
  }

  /**
   * Parses the request for the queryString
   *
   * @return URL Encoded String of Parameter Value Pair
   */
  private static String getQueryString() {
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

  private static Map<String, String> getSearchParams() {
    Map<String, String> searchParams = new HashMap<String, String>();

    DynamicForm form = Form.form().bindFromRequest(request());
    String username = form.get(USERNAME);
    username = username != null ? username.trim().toLowerCase() : null;
    searchParams.put(USERNAME, username);
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
    if (Utils.isSet(username)) {
      query = query.eq(AppResult.TABLE.USERNAME, username);
    }
    String jobType = searchParams.get(JOB_TYPE);
    if (Utils.isSet(jobType)) {
      query = query.eq(AppResult.TABLE.JOB_TYPE, jobType);
    }
    String severity = searchParams.get(SEVERITY);
    if (Utils.isSet(severity)) {
      String analysis = searchParams.get(ANALYSIS);
      if (Utils.isSet(analysis)) {
        query = query.eq(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.HEURISTIC_NAME, analysis)
            .ge(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.SEVERITY, severity);
      } else {
        query = query.ge(AppResult.TABLE.SEVERITY, severity);
      }
    }

    // Time Predicates. Both the startedTimeBegin and startedTimeEnd are inclusive in the filter
    String startedTimeBegin = searchParams.get(STARTED_TIME_BEGIN);
    if (Utils.isSet(startedTimeBegin)) {
      long time = parseTime(startedTimeBegin);
      if (time > 0) {
        query = query.ge(AppResult.TABLE.START_TIME, time);
      }
    }
    String startedTimeEnd = searchParams.get(STARTED_TIME_END);
    if (Utils.isSet(startedTimeEnd)) {
      long time = parseTime(startedTimeEnd);
      if (time > 0) {
        query = query.le(AppResult.TABLE.START_TIME, time);
      }
    }

    String finishedTimeBegin = searchParams.get(FINISHED_TIME_BEGIN);
    if (Utils.isSet(finishedTimeBegin)) {
      long time = parseTime(finishedTimeBegin);
      if (time > 0) {
        query = query.ge(AppResult.TABLE.FINISH_TIME, time);
      }
    }
    String finishedTimeEnd = searchParams.get(FINISHED_TIME_END);
    if (Utils.isSet(finishedTimeEnd)) {
      long time = parseTime(finishedTimeEnd);
      if (time > 0) {
        query = query.le(AppResult.TABLE.FINISH_TIME, time);
      }
    }

    // If queried by start time then sort the results by start time.
    if (Utils.isSet(startedTimeBegin) || Utils.isSet(startedTimeEnd)) {
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
    String flowExecId1 = form.get(COMPARE_FLOW_ID1);
    flowExecId1 = (flowExecId1 != null) ? flowExecId1.trim() : null;
    String flowExecId2 = form.get(COMPARE_FLOW_ID2);
    flowExecId2 = (flowExecId2 != null) ? flowExecId2.trim() : null;

    List<AppResult> results1 = null;
    List<AppResult> results2 = null;
    if (flowExecId1 != null && !flowExecId1.isEmpty() && flowExecId2 != null && !flowExecId2.isEmpty()) {
       results1 = AppResult.find
          .select(AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL
              + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId1).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .findList();
       results2 = AppResult.find
          .select(
              AppResult.getSearchFields() + "," + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL + ","
                  + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId2).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
          .findList();
    }
    return ok(comparePage.render(compareResults.render(compareFlows(results1, results2))));
  }

  /**
   * Helper Method for the compare controller.
   * This Compares 2 flow executions at job level.
   *
   * @param results1 The list of jobs under flow execution 1
   * @param results2 The list of jobs under flow execution 2
   * @return A map of Job Urls to the list of jobs corresponding to the 2 flow execution urls
   */
  private static Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> compareFlows(List<AppResult> results1,
      List<AppResult> results2) {
    Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> jobDefMap = new HashMap<IdUrlPair, Map<IdUrlPair, List<AppResult>>>();

    if (results1 != null && !results1.isEmpty() && results2 != null && !results2.isEmpty()) {

      IdUrlPair flow1 = new IdUrlPair(results1.get(0).flowExecId, results1.get(0).flowExecUrl);
      IdUrlPair flow2 = new IdUrlPair(results2.get(0).flowExecId, results2.get(0).flowExecUrl);

      Map<IdUrlPair, List<AppResult>> map1 = groupJobs(results1, GroupBy.JOB_DEFINITION_ID);
      Map<IdUrlPair, List<AppResult>> map2 = groupJobs(results2, GroupBy.JOB_DEFINITION_ID);

      final Set<IdUrlPair> group1 = new TreeSet<IdUrlPair>(new Comparator<IdUrlPair>(){
        public int compare(final IdUrlPair o1, final IdUrlPair o2){
          return o1.getId().compareToIgnoreCase(o2.getId());
        }
      } );
      group1.addAll(map1.keySet());
      final Set<IdUrlPair> group2 = new TreeSet<IdUrlPair>(new Comparator<IdUrlPair>(){
        public int compare(final IdUrlPair o1, final IdUrlPair o2){
          return o1.getId().compareToIgnoreCase(o2.getId());
        }
      } );
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
   * Controls the flow history. Displays max MAX_HISTORY_LIMIT executions
   */
  public static Result flowHistory() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String flowDefId = form.get(FLOW_DEF_ID);
    flowDefId = (flowDefId != null) ? flowDefId.trim() : null;
    if (flowDefId == null || flowDefId.isEmpty()) {
      return ok(flowHistoryPage.render(flowHistoryResults.render(null, null, null, null)));
    }

    // Fetch available flow executions with latest JOB_HISTORY_LIMIT mr jobs.
    List<AppResult> results = AppResult.find
        .select(
                AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL + ","
                        + AppResult.TABLE.JOB_DEF_ID + "," + AppResult.TABLE.JOB_DEF_URL + "," + AppResult.TABLE.JOB_NAME)
        .where().eq(AppResult.TABLE.FLOW_DEF_ID, flowDefId)
        .order().desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(JOB_HISTORY_LIMIT)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        .findList();
    if (results.size() == 0) {
      return notFound("Unable to find record on flow url: " + flowDefId);
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute flow execution data
    List<AppResult> filteredResults = new ArrayList<AppResult>();     // All jobs starting from latest execution
    List<Long> flowExecTimeList = new ArrayList<Long>();         // To map executions to resp execution time
    Map<IdUrlPair, Map<IdUrlPair, List<AppResult>>> executionMap =
        new LinkedHashMap<IdUrlPair, Map<IdUrlPair, List<AppResult>>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry: flowExecIdToJobsMap.entrySet()) {

      // Reverse the list content from desc order of finish time to increasing order so that when grouping we get
      // the job list in the order of completion.
      List<AppResult> mrJobsList = Lists.reverse(entry.getValue());

      // Flow exec time is the finish time of the last mr job in the flow
      flowExecTimeList.add(mrJobsList.get(mrJobsList.size() - 1).finishTime);

      filteredResults.addAll(mrJobsList);
      executionMap.put(entry.getKey(), groupJobs(mrJobsList, GroupBy.JOB_DEFINITION_ID));
    }

    // Calculate unique list of jobs (job def url) to maintain order across executions. List will contain job def urls
    // from latest execution first followed by any other extra job def url that may appear in previous executions.
    Map<IdUrlPair, String> idPairToJobNameMap = new HashMap<IdUrlPair, String>();
    Map<IdUrlPair, List<AppResult>> filteredMap = groupJobs(filteredResults, GroupBy.JOB_DEFINITION_ID);
    for (Map.Entry<IdUrlPair, List<AppResult>> entry: filteredMap.entrySet()) {
      idPairToJobNameMap.put(entry.getKey(), filteredMap.get(entry.getKey()).get(0).jobName);
    }

    return ok(flowHistoryPage.render(flowHistoryResults.render(flowDefId, executionMap, idPairToJobNameMap,
        flowExecTimeList)));
  }

  /**
   * Controls Job History. Displays at max MAX_HISTORY_LIMIT executions
   */
  public static Result jobHistory() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String jobDefId = form.get(JOB_DEF_ID);
    jobDefId = (jobDefId != null) ? jobDefId.trim() : null;
    if (jobDefId == null || jobDefId.isEmpty()) {
      return ok(jobHistoryPage.render(jobHistoryResults.render(null, null, -1, null)));
    }

    // Fetch all job executions
    List<AppResult> results = AppResult.find
        .select(AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
        .where().eq(AppResult.TABLE.JOB_DEF_ID, jobDefId)
        .order().desc(AppResult.TABLE.FINISH_TIME).setMaxRows(JOB_HISTORY_LIMIT)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "." + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .findList();
    if (results.size() == 0) {
      return notFound("Unable to find record on job url: " + jobDefId);
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =
        limitHistoryResults(groupJobs(results, GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute job execution data
    List<Long> flowExecTimeList = new ArrayList<Long>();
    int maxStages = 0;
    Map<IdUrlPair, List<AppResult>> executionMap = new LinkedHashMap<IdUrlPair, List<AppResult>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry: flowExecIdToJobsMap.entrySet()) {

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

    return ok(jobHistoryPage.render(jobHistoryResults.render(jobDefId, executionMap, maxStages, flowExecTimeList)));
  }

  /**
   * Applies a limit on the number of executions to be displayed after trying to maximize the correctness.
   *
   * Correctness:
   * When the number of jobs are less than the JOB_HISTORY_LIMIT, we can show all the executions correctly. However,
   * when the number of jobs are greater than the JOB_HISTORY_LIMIT, we cannot simply prune the jobs at that point and
   * show the history because we may skip some jobs which belong to the last flow execution. For the flow executions
   * we display, we want to ensure we show all the jobs belonging to that flow.
   *
   * So, when the number of executions are less than 10, we skip the last execution and when the number of executions
   * are greater than 10, we skip the last 3 executions just to maximise the correctness.
   *
   * @param map The results map to be pruned.
   * @param size Total number of jobs in the map
   * @param execLimit The upper limit on the number of executions to be displayed.
   * @return A map after applying the limit.
   */
  private static Map<IdUrlPair, List<AppResult>> limitHistoryResults(Map<IdUrlPair, List<AppResult>> map,
      int size, int execLimit) {
    Map<IdUrlPair, List<AppResult>> resultMap = new LinkedHashMap<IdUrlPair, List<AppResult>>();

    int limit;
    if (size < JOB_HISTORY_LIMIT) {
      // No pruning needed. 100% correct.
      limit = execLimit;
    } else {
      Set<IdUrlPair> keySet = map.keySet();
      if (keySet.size() > 10) {
        // Prune last 3 executions
        limit = keySet.size() > (execLimit + 3) ? execLimit : keySet.size() - 3;
      } else {
        // Prune the last execution
        limit = keySet.size() - 1;
      }
    }

    // Filtered results
    int i = 1;
    for (Map.Entry<IdUrlPair, List<AppResult>> entry : map.entrySet()) {
      if (i > limit) {
        break;
      }
      resultMap.put(entry.getKey(), entry.getValue());
      i++;
    }

    return resultMap;
  }

  /**
   * Controls the Help Page
   */
  public static Result help() {
    DynamicForm form = Form.form().bindFromRequest(request());
    String topic = form.get("topic");
    Html page = null;
    String title = "Help";
    if (topic != null && !topic.isEmpty()) {
      page = ElephantContext.instance().getHeuristicToView().get(topic);
      if (page != null) {
        title = topic;
      }
    }
    return ok(helpPage.render(title, page));
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
        .idEq(id).findUnique();

    if (result != null) {
      return ok(Json.toJson(result));
    } else {
      return notFound("Unable to find record on id: " + id);
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
        .where().eq(AppResult.TABLE.JOB_EXEC_ID, jobExecId)
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
        .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId)
        .findList();

    if (results.size() == 0) {
      return notFound("Unable to find record on flow exec url: " + flowExecId);
    }

    Map<IdUrlPair, List<AppResult>> groupMap = groupJobs(results, GroupBy.JOB_EXECUTION_ID);

    Map<String, List<AppResult>> resMap = new HashMap<String, List<AppResult>>();
    for (Map.Entry<IdUrlPair, List<AppResult>> entry : groupMap.entrySet()) {
      IdUrlPair jobExecPair = entry.getKey();
      List<AppResult> value = entry.getValue();
      resMap.put(jobExecPair.getId(), value);
    }

    return ok(Json.toJson(resMap));
  }

  static enum GroupBy {
    JOB_EXECUTION_ID,
    JOB_DEFINITION_ID,
    FLOW_EXECUTION_ID
  }

  /**
   * Grouping a list of AppResult by GroupBy enum.
   *
   * @param results The list of jobs of type AppResult to be grouped.
   * @param groupBy The field by which the results have to be grouped.
   * @return A map with the grouped field as the key and the list of jobs as the value.
   */
  private static Map<IdUrlPair, List<AppResult>> groupJobs(List<AppResult> results, GroupBy groupBy) {
    Map<String, List<AppResult>> groupMap = new LinkedHashMap<String, List<AppResult>>();
    Map<String, String> idUrlMap = new HashMap<String, String>();

    for (AppResult result : results) {
      String idField = null;
      String urlField = null;
      switch (groupBy) {
        case JOB_EXECUTION_ID:
          idField = result.jobExecId;
          urlField = result.jobExecUrl;
          break;
        case JOB_DEFINITION_ID:
          idField = result.jobDefId;
          urlField = result.jobDefUrl;
          break;
        case FLOW_EXECUTION_ID:
          idField = result.flowExecId;
          urlField = result.flowExecUrl;
          break;
      }
      if (!idUrlMap.containsKey(idField)) {
        idUrlMap.put(idField, urlField);
      }

      if (groupMap.containsKey(idField)) {
        groupMap.get(idField).add(result);
      } else {
        List<AppResult> list = new ArrayList<AppResult>();
        list.add(result);
        groupMap.put(idField, list);
      }
    }

    // Construct the final result map with the key as a (id, url) pair.
    Map<IdUrlPair, List<AppResult>> resultMap = new LinkedHashMap<IdUrlPair, List<AppResult>>();
    for (Map.Entry<String, List<AppResult>> entry : groupMap.entrySet()) {
      String key = entry.getKey();
      List<AppResult> value = entry.getValue();
      resultMap.put(new IdUrlPair(key, idUrlMap.get(key)), value);
    }

    return resultMap;
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
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
              + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
          .where()
          .idEq(appId).findUnique();
      if (result != null) {
        return ok(Json.toJson(result));
      } else {
        return notFound("Unable to find record on id: " + appId);
      }
    } else if (flowExecId != null && !flowExecId.isEmpty()) {
      List<AppResult> results = AppResult.find
          .select("*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
              + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
          .where().eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId)
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
    List<AppResult> results = query
        .setFirstRow((page - 1) * REST_PAGE_LENGTH)
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
      results1 = AppResult.find
          .select("*").where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId1).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
              + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
          .findList();
      results2 = AppResult.find
          .select("*").where()
          .eq(AppResult.TABLE.FLOW_EXEC_ID, flowExecId2).setMaxRows(100)
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
          .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
              + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
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
    List<AppResult> results = AppResult.find
        .select("*")
        .where().eq(AppResult.TABLE.FLOW_DEF_ID, flowDefId)
        .order().desc(AppResult.TABLE.FINISH_TIME)
        .setMaxRows(JOB_HISTORY_LIMIT)
        // The 2nd and 3rd table are not required for plotting the graph
        //.fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, AppHeuristicResult.getSearchFields())
        //.fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS + "."
        //    + AppHeuristicResult.TABLE.APP_HEURISTIC_RESULT_DETAILS, "*")
        .findList();
    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for(int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int flowPerfScore = 0;
      JsonArray jobScores = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));
      Map<IdUrlPair, List<AppResult>> jobDefIdToJobsMap = groupJobs(mrJobsList, GroupBy.JOB_DEFINITION_ID);

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
      dataset.addProperty("flowtime", mrJobsList.get(mrJobsList.size() - 1).finishTime);
      dataset.addProperty("score", flowPerfScore);
      dataset.add("jobscores", jobScores);

      datasets.add(dataset);
    }

    return ok(new Gson().toJson(datasets));
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
    List<AppResult> results = AppResult.find
        .select(AppResult.getSearchFields() + "," + AppResult.TABLE.FLOW_EXEC_ID + "," + AppResult.TABLE.FLOW_EXEC_URL)
        .where().eq(AppResult.TABLE.JOB_DEF_ID, jobDefId)
        .order().desc(AppResult.TABLE.FINISH_TIME).setMaxRows(JOB_HISTORY_LIMIT)
        .fetch(AppResult.TABLE.APP_HEURISTIC_RESULTS, "*")
        .findList();
    if (results.size() == 0) {
      logger.info("No results for Job url");
    }
    Map<IdUrlPair, List<AppResult>> flowExecIdToJobsMap =  limitHistoryResults(
        groupJobs(results, GroupBy.FLOW_EXECUTION_ID), results.size(), MAX_HISTORY_LIMIT);

    // Compute the graph data starting from the earliest available execution to latest
    List<IdUrlPair> keyList = new ArrayList<IdUrlPair>(flowExecIdToJobsMap.keySet());
    for(int i = keyList.size() - 1; i >= 0; i--) {
      IdUrlPair flowExecPair = keyList.get(i);
      int jobPerfScore = 0;
      JsonArray stageScores = new JsonArray();
      List<AppResult> mrJobsList = Lists.reverse(flowExecIdToJobsMap.get(flowExecPair));
      for (AppResult appResult : flowExecIdToJobsMap.get(flowExecPair)) {

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
      dataset.addProperty("flowtime", mrJobsList.get(mrJobsList.size() - 1).finishTime);
      dataset.addProperty("score", jobPerfScore);
      dataset.add("stagescores", stageScores);

      datasets.add(dataset);
    }

    return ok(new Gson().toJson(datasets));
  }
}