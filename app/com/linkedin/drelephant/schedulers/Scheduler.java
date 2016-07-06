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

package com.linkedin.drelephant.schedulers;


/**
 * Scheduler interface defining the
 */
public interface Scheduler {

  /**
   * Return the Scheduler Name
   *
   * @return the scheduler name
   */
  public String getSchedulerName();

  /**
   * True if the the scheduler object was not able to parse the given properties
   *
   * @return true the scheduler is empty
   */
  public boolean isEmpty();

  /**
   * Return the Job Definition Id of the job in the workflow
   *
   * @return the job definition id
   */
  public String getJobDefId();

  /**
   * Return the Job Execution Id of the job in the workflow
   *
   * @return the job execution id
   */
  public String getJobExecId();

  /**
   * Return the Flow Definition Id of the workflow
   *
   * @return the flow definition id
   */
  public String getFlowDefId();

  /**
   * Return the Flow Execution Id of the workflow
   *
   * @return the flow execution id
   */
  public String getFlowExecId();

  /**
   * Return a link to the job's definition
   *
   * @return the job definition url
   */
  public String getJobDefUrl();

  /**
   * Return a link to the job's execution
   *
   * @return the job execution url
   */
  public String getJobExecUrl();

  /**
   * Return a link to the flow's definition
   *
   * @return the flow definition url
   */
  public String getFlowDefUrl();

  /**
   * Return a link to the flow's execution
   *
   * @return the flow execution url
   */
  public String getFlowExecUrl();

  /**
   * Return the name of the Job/Action in the Flow
   *
   * @return the job/action name
   */
  public String getJobName();

  /**
   * Return the workflow depth
   *
   * @return the workflow depth
   */
  public int getWorkflowDepth();
}
