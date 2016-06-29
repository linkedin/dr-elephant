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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ExceptionFinder {

  private final Logger logger = Logger.getLogger(ExceptionFinder.class);
  //private String url;
  private Map<String, List<HadoopException>> exceptions;
  private AzkabanClient _client;

  public ExceptionFinder(String url) {
    //this.url = url;
    this.exceptions = new HashMap<String, List<HadoopException>>();
    _client = new AzkabanClient(url);
    _client.azkabanLogin("", ""); //Login using Azkaban Credentials
    fetchAndAnalyzeLogs();
  }

  public void fetchAndAnalyzeLogs() {
    final String jhistoryAddr = new Configuration().get("mapreduce.jobhistory.webapp.address");

    LogAnalyzer analyzedFlowLog = analyzeAzkabanFlowLog();
    Set<String> failedAzkabanJobs = analyzedFlowLog.getFailedSubEvents();
    for (String azkabanJob : failedAzkabanJobs) {
      LogAnalyzer analyzedAzkabanJobLog = analyzeAzkabanJobLog(azkabanJob);
      List<HadoopException> exceptions = new ArrayList<HadoopException>();      // azkabanJob exceptions

      Set<String> hadoopJobs = analyzedAzkabanJobLog.getFailedSubEvents();
      for (String hadoopJob : hadoopJobs) {
        String jhsURL = "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + hadoopJob;
        LogAnalyzer AnalyzedMRLog = analyzeHadoopJobOverview(jhsURL);
        if (AnalyzedMRLog != null) {
          HadoopException mrException = new HadoopException();
          if (AnalyzedMRLog.getException().getExceptionChain() != null) {
            //mr job failed for reason other than task failure
            ExceptionLoggingEvent mrJobException = AnalyzedMRLog.getException();
            mrJobException.setType("mr");
            mrJobException.setId(hadoopJob);
            mrException.addExceptionLoggingEvent(mrJobException);
          }
          Set<String> failedHadoopTasks = AnalyzedMRLog.getFailedSubEvents();
          for (String hadoopTask : failedHadoopTasks) {
            jhsURL = "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + hadoopJob + "/tasks/" + hadoopTask
                + "/attempts";
            ExceptionLoggingEvent mrTaskException = analyzeHadoopTaskDiagnostic(jhsURL);
            mrTaskException.setType("task");
            mrTaskException.setId(hadoopTask);
            mrException.addExceptionLoggingEvent(mrTaskException);
          }
          exceptions.add(mrException);
        }
      }
      if (exceptions.isEmpty()
          && analyzedAzkabanJobLog.getException() != null) {
        // Azkaban job failed for reason other than mr job failure
        HadoopException azException = new HadoopException();
        azException.addExceptionLoggingEvent(analyzedAzkabanJobLog.getException());
        exceptions.add(azException);
      }
      this.exceptions.put(azkabanJob, exceptions);
    }
  }

  public LogAnalyzer analyzeAzkabanFlowLog() {
    String response = _client.getExecutionLog("0", "99999999");
    LogAnalyzer analyzedLog = new LogAnalyzer(response);
    return analyzedLog;
  }

  public LogAnalyzer analyzeAzkabanJobLog(String azkabanJob) {
    String response = _client.getJobLog(azkabanJob, "0", "99999999");
    LogAnalyzer analyzedLog = new LogAnalyzer(response);
    return analyzedLog;
  }

  public JsonNode readJson(URL url) {
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL authenticatedURL = new AuthenticatedURL();
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      HttpURLConnection conn = authenticatedURL.openConnection(url, token);
      return objectMapper.readTree(conn.getInputStream());
    } catch (AuthenticationException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public LogAnalyzer analyzeHadoopJobOverview(String jobURL) {
    try {
      JsonNode response = readJson(new URL(jobURL));
      if (response.get("job").get("state").toString() != "SUCCEEDED") {
        LogAnalyzer analyzedLog = new LogAnalyzer(response.get("job").get("diagnostics").getTextValue());
        return analyzedLog;
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public ExceptionLoggingEvent analyzeHadoopTaskDiagnostic(String taskURL) {
    // Analyzes the last task attempt
    try {
      JsonNode response = readJson(new URL(taskURL));
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
      LogAnalyzer analyzedLog = new LogAnalyzer(
          response.get("taskAttempts").get("taskAttempt").get(maxattemptid).get("diagnostics").getTextValue());
      return analyzedLog.getException();
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    return null;
  }

  public Map<String, List<HadoopException>> getExceptions() {
    return this.exceptions;
  }
}