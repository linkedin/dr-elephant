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

/**
 * This class contains the aggregated data of a job
 */
public class HadoopAggregatedData {

  //variable resourceUsed is actually the resource blocked on the cluster. 
  private long resourceUsed = 0;
  private long resourceWasted = 0;
  private long totalDelay = 0;

  /**
   * Returns the resource usage (in MBSeconds) of the job
   * @return The resource usage (in MBSeconds) of the job
   */
  public long getResourceUsed() {
    return resourceUsed;
  }

  /**
   * Setter for the resource usage (in MBSeconds) of the job
   * @param resourceUsed The resource usage (in MBSeconds) of the job
   */
  public void setResourceUsed(long resourceUsed) {
    this.resourceUsed = resourceUsed;
  }

  /**
   * Returns the wasted resources (in MBSeconds) of the job
   * @return The wasted resources (in MBSeconds) of the job
   */
  public long getResourceWasted() {
    return resourceWasted;
  }

  /**
   * Setter for the wasted resources (in MBSeconds)
   * @param resourceWasted The wasted resources (in MBSeconds) of the job
   */
  public void setResourceWasted(long resourceWasted) {
    this.resourceWasted = resourceWasted;
  }

  /**
   * returns the total delay of the job
   * @return The total delay of the job
   */
  public long getTotalDelay() {
    return totalDelay;
  }

  /**
   * Setter for the total delay of the job
   * @param totalDelay The total delay of the job
   */
  public void setTotalDelay(long totalDelay) {
    this.totalDelay = totalDelay;
  }

}
