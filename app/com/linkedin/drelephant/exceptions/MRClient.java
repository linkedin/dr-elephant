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


public class MRClient {
  final String jhistoryAddr = new Configuration().get("mapreduce.jobhistory.webapp.address");
  private AuthenticatedURL.Token _token;
  private AuthenticatedURL _authenticatedURL;

  public MRClient() {
    _token = new AuthenticatedURL.Token();
    _authenticatedURL = new AuthenticatedURL();
  }

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
