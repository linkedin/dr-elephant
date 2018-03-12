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

package com.linkedin.drelephant.clients;

import java.io.File;
import java.util.Map;
import java.util.Set;

import com.linkedin.drelephant.exceptions.JobState;
import com.linkedin.drelephant.exceptions.LoggingEvent;


/**
 * The interface WorkflowClient should be implemented by all the workflow client. The client should not
 * be confused with the a client of the scheduler since the context of this client is limited to a workflow
 * and it doesn't operate at a scheduler level.
 */
public interface WorkflowClient {

  /**
   * Login to the scheduler using the username and the password
   * @param username The username of the user
   * @param password The password of the user
   */
  public void login(String username, String password);

  /**
   * Login to the scheduler using the username and the private key
   * @param username The username of the user
   * @param privateKey The private key of the user
   */
  public void login(String username, File privateKey);

  /**
   * Return all the jobs in the workflow. It returns a Map<String,String> where the key \n
   * is the execution id of the job and the value is the status of the job.
   * @return Return all the jobs in the workflow
   */
  public Map<String, String> getJobsFromFlow();

  /**
   * Given a job id, this method analyzes the job
   * @param jobId The execution id of the job
   */
  public void analyzeJob(String jobId);

  /**
   * This method extracts out all the yarn applications from the job and returns the set of them.
   * @param jobId The jobid of the job.
   * @return The set of all the yarn applications spawned by the job
   */
  public Set<String> getYarnApplicationsFromJob(String jobId);

  /**
   * Returns the job state of the job.
   * @param jobId  The id of the job
   * @return Retruns the state of the job
   */
  public JobState getJobState(String jobId);

  /**
   * Get the exception, given a job id
   * @param jobId The id of the job
   * @return The exeception encountered
   */
  public LoggingEvent getJobException(String jobId);
}
