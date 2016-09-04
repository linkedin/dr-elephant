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

import org.apache.hadoop.mapreduce.Job;


/**
 * The interface to define common methods for each fetcher.
 *
 * There would be a different fetcher implementation given a different Hadoop version and a different application type.
 */
public interface ElephantFetcher<T extends HadoopApplicationData> {

  /**
   * Given an application id and optionally a Job in case of running application, fetches the data object
   *
   * @param job The job being analysed
   * @return the fetched data
   * @throws Exception
   */
  public T fetchData(AnalyticJob analyticjob, Job job)
      throws Exception;
}