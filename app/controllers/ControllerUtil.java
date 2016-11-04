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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import models.AppResult;


public class ControllerUtil {

 private static final int JOB_HISTORY_LIMIT = 5000;

  public static enum GroupBy {
    JOB_EXECUTION_ID,
    JOB_DEFINITION_ID,
    FLOW_EXECUTION_ID
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
  public static Map<IdUrlPair, List<AppResult>> limitHistoryResults(Map<IdUrlPair, List<AppResult>> map,int size,
      int execLimit) {

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
   * Grouping a list of AppResult by GroupBy enum.
   *
   * @param results The list of jobs of type AppResult to be grouped.
   * @param groupBy The field by which the results have to be grouped.
   * @return A map with the grouped field as the key and the list of jobs as the value.
   */
  public static Map<IdUrlPair, List<AppResult>> groupJobs(List<AppResult> results, GroupBy groupBy) {
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

}
