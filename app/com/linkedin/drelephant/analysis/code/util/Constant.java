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


package com.linkedin.drelephant.analysis.code.util;

/**
 * This class contains all the constants require by Code Analysis
 * which are shared across classes.
 */

public final class Constant {
  static final String BASE_URL_FOR_EXTRACTING_CODE_NAME = "ca.url.extract.code";
  static final String QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION_NAME = "ca.queue.name.valid.code.extraction";
  static final String THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE_NAME =
      "ca.threshold.checkpoint.actiontransformation";
  static final String SHUFFLE_OPERATIONS_IN_HIVE_NAME = "ca.hive.shuffle.operations";
  public static final String CODE_LEVEL_OPTIMIZATION_ENABLED = "code.level.optimization.enabled";
  public static final String CODE_HEURISTIC_NAME = "codeheuristic";
  public static final String CODE_HEURISTIC_VIEW_NAME = "views.html.help.mapreduce.helpCodeHeuristic";

  public enum CodeLocationJSONKey {
    PATH("paths"),
    SCM("scm"),
    FILE_PATH("filePath"),
    REPONAME("repoName"),
    FILE_INFO("fileInfo"),
    SOURCE_CODE("sourceCode");
    private final String keyName;

    CodeLocationJSONKey(String keyName) {
      this.keyName = keyName;
    }

    public String getJSONKey() {
      return keyName;
    }
  }

  public enum ShuffleOperationThreshold {
    FIRST_THRESHOLD(1), SECOND_THRESHOLD(2);
    private final int keyName;

    ShuffleOperationThreshold(int keyName) {
      this.keyName = keyName;
    }

    public int getValue() {
      return keyName;
    }
  }

  public enum NodeComplexity {
    LOW(1), MEDIUM(2), HIGH(3);
    private final int keyName;

    NodeComplexity(int keyName) {
      this.keyName = keyName;
    }

    public int getValue() {
      return keyName;
    }
  }
}
