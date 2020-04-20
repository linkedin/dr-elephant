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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * Exception categorization data
 */
@Data
public class ExceptionCategorizationData implements Comparable<ExceptionCategorizationData> {
  private String ruleName;
  private List<String> ruleTriggers;
  private String priority;
  private String category;
  private static final String RULE_TRIGGER_DELIMITER = ",";
  private static final String UNNECESSARY_DATA = "\n";

  ExceptionCategorizationData(String ruleName, String ruleTrigger, String priority, String category) {
    this.ruleName = ruleName;
    this.ruleTriggers =
        Arrays.stream(ruleTrigger.toLowerCase().replaceAll(UNNECESSARY_DATA, "").split(RULE_TRIGGER_DELIMITER))
            .map(String::trim)
            .collect(Collectors.toList());
    this.priority = priority;
    this.category = category;
  }

  @Override
  public int compareTo(ExceptionCategorizationData o) {
    return this.priority.compareTo(o.priority);
  }
}
