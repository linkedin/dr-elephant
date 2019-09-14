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

package com.linkedin.drelephant.analysis.code;

import com.linkedin.drelephant.analysis.code.dataset.JobCodeInfoDataSet;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;
import java.io.IOException;
import java.net.MalformedURLException;
import models.AppResult;
import org.codehaus.jettison.json.JSONException;


/**
 * Interface for CodeExtractor , one of the implementation is
 * AzkabanJarvisCodeExtractor
 */
public interface CodeExtractor {

  /**
   * Check if prerequistes are matched to extract the codee
   * @param appResult : Information about the application executed on cluster
   * @return true or false
   */
  boolean arePrerequisiteMatched(AppResult appResult);

  /**
   *
   * @param appResult : Information about the application executed on cluster
   * @return : Code File Name
   * @throws CodeAnalyzerException : Throws custom exception
   */
  String getCodeFileName(AppResult appResult) throws CodeAnalyzerException;

  /**
   *
   * @param appResult: Information about the application executed on cluster
   * @return : DataSet which contains source code and all the relevant information about code
   * @throws CodeAnalyzerException : Throws custom exception
   */
  JobCodeInfoDataSet execute(AppResult appResult) throws CodeAnalyzerException;

  /**
   *
   * @param codeInformation : Contains code Information
   * @throws IOException
   * @throws JSONException
   */
  void processCodeLocationInformation(String codeInformation) throws IOException, JSONException;

  /**
   * Get JobCodeInfoDataSet ,contains information about JobCode
   */
  JobCodeInfoDataSet getJobCodeInfoDataSet();
}