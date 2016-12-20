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

package com.linkedin.drelephant.configurations.scheduler;

import java.util.Map;


/**
 * Scheduler Configuration Holder
 */
public class SchedulerConfigurationData {
  private final String _schedulerName;
  private final String _className;
  private final Map<String, String> _paramMap;

  public SchedulerConfigurationData(String schedulerName, String className, Map<String, String> paramMap) {
    _schedulerName = schedulerName;
    _className = className;
    _paramMap = paramMap;
  }

  public String getSchedulerName() {
    return _schedulerName;
  }

  public String getClassName() {
    return _className;
  }

  public Map<String, String> getParamMap() {
    return _paramMap;
  }
}
