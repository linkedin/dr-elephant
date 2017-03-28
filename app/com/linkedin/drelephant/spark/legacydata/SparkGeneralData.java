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

package com.linkedin.drelephant.spark.legacydata;

import java.util.Set;


/**
 * This class holds Spark application information
 */
public class SparkGeneralData {
  private Set<String> _adminAcls;
  private Set<String> _viewAcls;
  private String _applicationId;
  private String _applicationName;
  private String _sparkUser;
  private long _startTime;
  private long _endTime;

  public Set<String> getAdminAcls() {
    return _adminAcls;
  }

  public void setAdminAcls(Set<String> adminAcls) {
    _adminAcls = adminAcls;
  }

  public Set<String> getViewAcls() {
    return _viewAcls;
  }

  public void setViewAcls(Set<String> viewAcls) {
    _viewAcls = viewAcls;
  }

  public String getApplicationId() {
    return _applicationId;
  }

  public void setApplicationId(String applicationId) {
    _applicationId = applicationId;
  }

  public String getApplicationName() {
    return _applicationName;
  }

  public void setApplicationName(String applicationName) {
    _applicationName = applicationName;
  }

  public String getSparkUser() {
    return _sparkUser;
  }

  public void setSparkUser(String sparkUser) {
    _sparkUser = sparkUser;
  }

  public long getStartTime() {
    return _startTime;
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
  }
}
