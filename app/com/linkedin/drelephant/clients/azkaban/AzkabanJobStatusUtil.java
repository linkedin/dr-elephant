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

package com.linkedin.drelephant.clients.azkaban;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.drelephant.AutoTuner;
import com.linkedin.drelephant.clients.WorkflowClient;
import com.linkedin.drelephant.configurations.scheduler.SchedulerConfigurationData;
import com.linkedin.drelephant.util.InfoExtractor;


/**
 * This class is azkaban scheduler util for getting job status.
 */
public class AzkabanJobStatusUtil {

  private static final Logger logger = Logger.getLogger(AzkabanJobStatusUtil.class);
  private HashMap<String, AzkabanWorkflowClient> workflowClients = new HashMap<String, AzkabanWorkflowClient>();
  private String scheduler = "azkaban";
  private static String USERNAME = "username";
  private static String PRIVATE_KEY = "private_key";
  private static String PASSWORD = "password";
  private static final long TOKEN_UPDATE_INTERVAL = AutoTuner.ONE_MIN * 60 * 1;

  public AzkabanWorkflowClient getWorkflowClient(String url) throws MalformedURLException {
    String hostAddress = "https://" + new URL(url).getAuthority();
    AzkabanWorkflowClient workflowClient = null;
    if (workflowClients.containsKey(hostAddress)) {
      logger.debug("WorkflowClient Exist " + url + " Host Address is " + hostAddress);
      workflowClient = workflowClients.get(hostAddress);
    } else {
      logger.debug("WorkflowClient Does not Exist " + url + " Host Address is " + hostAddress);
      workflowClient = (AzkabanWorkflowClient) InfoExtractor.getWorkflowClientInstance(scheduler, url);
      workflowClients.put(hostAddress, workflowClient);
    }
    doLogin(workflowClient);
    return workflowClient;
  }

  public WorkflowClient doLogin(AzkabanWorkflowClient workflowClient) {
    Long _currentTime = System.currentTimeMillis();
    if (_currentTime - workflowClient.getSessionUpdatedTime() > TOKEN_UPDATE_INTERVAL) {
      logger.info("Creating a new session with Azkaban");

      SchedulerConfigurationData schedulerData = InfoExtractor.getSchedulerData(scheduler);

      if (schedulerData == null) {
        throw new RuntimeException(String.format("Cannot find scheduler %s for url %s", scheduler));
      }

      if (!schedulerData.getParamMap().containsKey(USERNAME)) {
        throw new RuntimeException(String.format("Cannot find username for login"));
      }

      String username = schedulerData.getParamMap().get(USERNAME);

      if (schedulerData.getParamMap().containsKey(PRIVATE_KEY)) {
        workflowClient.login(username, new File(schedulerData.getParamMap().get(PRIVATE_KEY)));
      } else if (schedulerData.getParamMap().containsKey(PASSWORD)) {
        workflowClient.login(username, schedulerData.getParamMap().get(PASSWORD));
      } else {
        throw new RuntimeException("Neither private key nor password was specified");
      }
      workflowClient.setSessionUpdatedTime(_currentTime);
    }
    return workflowClient;
  }

  /**
   * Returns the jobs from the flow
   * @param execUrl Execution url
   * @return Jobs from flow
   * @throws MalformedURLException
   * @throws URISyntaxException
   */
  public Map<String, String> getJobsFromFlow(String execUrl) throws MalformedURLException, URISyntaxException {
    AzkabanWorkflowClient workflowClient = getWorkflowClient(execUrl);
    workflowClient.setURL(execUrl);
    return workflowClient.getJobsFromFlow();
  }
}
