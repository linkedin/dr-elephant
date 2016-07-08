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

package com.linkedin.drelephant.exceptions;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/* Client to interact with job history server and get the mapreduce logs*/

public class MRClient {
  final String jhistoryAddr = new Configuration().get("mapreduce.jobhistory.webapp.address");
  private AuthenticatedURL.Token _token;
  private AuthenticatedURL _authenticatedURL;

  public MRClient() {
    _token = new AuthenticatedURL.Token();
    _authenticatedURL = new AuthenticatedURL();
  }

  /*
  * For a given rest url, fetchs and return the jsonnode
  *
  * @param rest job history server url
  * @return Json node to which the url points
  */
  private JsonNode fetchJson(URL url) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      HttpURLConnection conn = _authenticatedURL.openConnection(url, _token);
      return objectMapper.readTree(conn.getInputStream());
    } catch (AuthenticationException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /*
  * Returns the diagnostics for a given MR Job Id
  *
  * @param mrJobId   MR Job Id
  * @return Diagnostics in a string format
  */

  public String getMRJobLog(String mrJobId) {
    String mrJobHistoryURL = "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + mrJobId;
    try {
      JsonNode response = fetchJson(new URL(mrJobHistoryURL));
      if (response.get("job").get("state").toString() != "SUCCEEDED") {
        return response.get("job").get("diagnostics").getTextValue();
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
    return null;
  }

  /*
  * Returns the last task attempt diagnostic for a given failed taskId
  *
  * @param mrJobId   MR Job Id
  * @param mrTaskId  MRTask Id
  * @return Diagnostic in a string format
  */

  /* To do:
  It is a heuristic that for a failed task, the last task attempt should be look at to get the correct stack trace
  Counter example:
  http://ltx1-holdemjh01.grid.linkedin.com:19888/ws/v1/history/mapreduce/jobs/job_1466048666726_706209/tasks/task_1466048666726_706209_r_000623/attempts

  Probably we should look at the first attempt, it is a better assumption than above
  */
  public String getMRTaskLog(String mrJobId, String mrTaskId) {
    String mrTaskHistoryURL =
        "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + mrJobId + "/tasks/" + mrTaskId + "/attempts";
    ;
    try {
      JsonNode response = fetchJson(new URL(mrTaskHistoryURL));
      int attempts = response.get("taskAttempts").get("taskAttempt").size();
      int maxattempt = 0;
      int maxattemptid = 0;
      for (int i = 0; i < attempts; i++) {
        int attempt = Integer.parseInt(
            response.get("taskAttempts").get("taskAttempt").get(i).get("id").getTextValue().split("_")[5]);
        if (attempt > maxattempt) {
          maxattemptid = i;
          maxattempt = attempt;
        }
      }
      return response.get("taskAttempts").get("taskAttempt").get(maxattemptid).get("diagnostics").getTextValue();
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
