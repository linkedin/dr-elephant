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

package com.linkedin.drelephant.analysis;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;


/**
 * Provides AnalyticJobs that will yield to analysis results later. This class basically generates to-dos that could be
 * executed later.
 */
public interface AnalyticJobGenerator {

  /**
   * Configures the provider instance
   *
   * @param configuration The Hadoop configuration object
   * @throws Exception
   */
  public void configure(Configuration configuration)
      throws IOException;

  /**
   * Configures the resource manager addresses considering HA
   */
  public void updateResourceManagerAddresses();

  /**
   * Provides a list of AnalyticJobs that should be calculated
   *
   * @return a list of AnalyticJobs
   * @throws IOException
   * @throws AuthenticationException
   */
  public List<AnalyticJob> fetchAnalyticJobs()
      throws IOException, AuthenticationException;

  /**
   * Add an AnalyticJob into retry list. Those jobs will be provided again via #fetchAnalyticJobs under
   * the generator's decision.
   *
   * @param job The job to add
   */
  public void addIntoRetries(AnalyticJob job);

  /**
   * Add an AnalyticJob into the second retry list. This queue fetches jobs on greater intervals of time. Those jobs will be provided again via #fetchAnalyticJobs under
   * the generator's decision.
   *
   * @param job The job to add
   */
  public void addIntoSecondRetryQueue(AnalyticJob job);
}
