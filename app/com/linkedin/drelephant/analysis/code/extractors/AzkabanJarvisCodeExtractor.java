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

package com.linkedin.drelephant.analysis.code.extractors;

import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.optimizers.CodeOptimizerFactory;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;
import com.linkedin.drelephant.analysis.code.CodeExtractor;
import com.linkedin.drelephant.analysis.code.dataset.JobCodeInfoDataSet;
import com.linkedin.drelephant.analysis.code.util.Constant;
import com.linkedin.drelephant.analysis.code.util.Helper;
import com.linkedin.drelephant.clients.azkaban.AzkabanJobStatusUtil;
import com.linkedin.drelephant.clients.azkaban.AzkabanWorkflowClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import models.AppResult;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 * This class is used to extract the code information from Azkaban(job has to be schduled through Azkaban)
 * and then code information from Jarvis .
 */

public class AzkabanJarvisCodeExtractor implements CodeExtractor {
  private static final Logger logger = Logger.getLogger(AzkabanJarvisCodeExtractor.class);
  private static AzkabanJobStatusUtil azkabanJobStatusUtil = null;
  private final String BASE_URL = Helper.ConfigurationBuilder.BASE_URL_FOR_EXTRACTING_CODE.getValue();
  private final String COMPLETE_URL_TO_GET_LOCATION = BASE_URL + "filepaths?query=%s";
  private final String COMPLETE_URL_TO_GET_CODE = BASE_URL + "file/%1$s/%2$s/%3$s";
  private JobCodeInfoDataSet _jobCodeInfoDataSet = null;
  private static final int TOP_RANK_INDEX_FOR_SEARCH_RESULT = 0;

  static {
    azkabanJobStatusUtil = new AzkabanJobStatusUtil();
    logger.info(" Intialized Azkaban Job status Util to query azkaban for Code analysis ");
  }

  /**
   *
   * @param appResult: Information about the application executed on cluster
   * @return JobCodeInfoDataSet :Source code and information about its location
   * @throws CodeAnalyzerException : If things doesn't work as expected throws exception.
   * It catches Blanket Exception , since if unknown error comes , system should not halt.
   */
  @Override
  public JobCodeInfoDataSet execute(AppResult appResult) throws CodeAnalyzerException {
    try {
      if (arePrerequisiteMatched(appResult)) {
        this._jobCodeInfoDataSet = new JobCodeInfoDataSet();
        _jobCodeInfoDataSet.setJobExecutionID(appResult.jobExecId);
        String codeFileName = getCodeFileName(appResult);
        if (codeFileName != null) {
          logger.info(" Code file name is " + codeFileName);
          extractCode(codeFileName);
          if (_jobCodeInfoDataSet != null) {
            logger.info(" Successfully extracted code for following job " + appResult.jobDefId);
          }
        }
      }
    } catch (IOException | JSONException e) {
      logger.info(" Exception while processing code extractor ", e);
      throw new CodeAnalyzerException(e);
    } catch (Exception e) {
      logger.info(" Exception while processing code extractor ", e);
      throw new CodeAnalyzerException(e);
    }
    return _jobCodeInfoDataSet;
  }

  /**
   * Check if the app result is correct and if the queue/project name is correct.
   * @param appResult : Information about the application executed on cluster
   * @return
   */
  @Override
  public boolean arePrerequisiteMatched(AppResult appResult) {
    if (appResult == null || appResult.jobDefId == null || appResult.queueName == null) {
      logger.info("Insufficient information to extract code information");
      return false;
    } else if (!isCodeNameExtractable(appResult)) {
      logger.info(" Queuename is not correct " + appResult.queueName);
      return false;
    }
    return true;
  }

  /**
   * Given code fileName, this method will query two times to jarvis
   * to get information about code (Scm, fileName,RepoName) and then
   * get code
   * @param codeFileName
   * @throws IOException
   * @throws JSONException
   */
  private void extractCode(String codeFileName) throws IOException, JSONException {
    CodeOptimizer codeOptimizer = CodeOptimizerFactory.getCodeOptimizer(codeFileName);
    if (codeOptimizer != null) {
      logger.info("Optimizer is available for following code " + codeFileName + " hence getting complete code ");
      String urlToGetJobLocation = String.format(COMPLETE_URL_TO_GET_LOCATION, codeFileName.replaceAll("src/", ""));
      _jobCodeInfoDataSet.getMetaData().put("URL TO GET CODE LOCATION", urlToGetJobLocation);
      logger.info(" Final URL to get Job Location " + urlToGetJobLocation);
      processCodeLocationInformation(urlToGetJobLocation);
      if (_jobCodeInfoDataSet.getFileName() != null && _jobCodeInfoDataSet.getFileName().length() > 0) {
        processInformationForSourceCode();
        _jobCodeInfoDataSet.setCodeOptimizer(codeOptimizer);
      }
    }
  }

  private boolean isCodeNameExtractable(AppResult appResult) {
    if (appResult != null) {
      for (String validNames : Helper.ConfigurationBuilder.QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION.getValue()) {
        logger.debug(" Valid queue names are " + validNames);
        if (appResult.queueName != null && appResult.queueName.toUpperCase().equals(validNames.toUpperCase())) {
          logger.info(" Extracting source code file name of the  following job " + appResult.jobDefId + " "
              + appResult.queueName);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String getCodeFileName(AppResult appResult) throws CodeAnalyzerException {
    try {
      if (appResult != null) {
        String modifiedURL = appResult.jobDefId.replaceAll("&job=", "&jobName=").replaceAll("&flow=", "&flowName=");
        logger.info("Query following url to azkaban " + modifiedURL);
        if (_jobCodeInfoDataSet == null) {
          _jobCodeInfoDataSet = new JobCodeInfoDataSet();
        }
        _jobCodeInfoDataSet.getMetaData().put("URL TO GET SCRIPT NAME", modifiedURL);
        return getCodeFileNameFromSchduler(appResult, modifiedURL);
      }
    } catch (MalformedURLException e) {
      throw new CodeAnalyzerException(e);
    }
    return null;
  }

  protected String getCodeFileNameFromSchduler(AppResult appResult, String modifiedURL) throws MalformedURLException {
    AzkabanWorkflowClient azkabanWorkflowClient = azkabanJobStatusUtil.getWorkflowClient(appResult.flowExecId);
    return azkabanWorkflowClient.getCodePathfromJob(modifiedURL);
  }

  @Override
  public void processCodeLocationInformation(String codeInformation) throws IOException, JSONException {
    if (codeInformation != null) {
      JSONObject jsonJobInfo = parseURL(codeInformation);
      JSONArray paths = jsonJobInfo.getJSONArray(Constant.CodeLocationJSONKey.PATH.getJSONKey());
      JSONObject information = paths.getJSONObject(TOP_RANK_INDEX_FOR_SEARCH_RESULT);
      if (_jobCodeInfoDataSet == null) {
        _jobCodeInfoDataSet = new JobCodeInfoDataSet();
      }
      _jobCodeInfoDataSet.setFileName(
          URLEncoder.encode(information.getString(Constant.CodeLocationJSONKey.FILE_PATH.getJSONKey()), "UTF-8"));
      _jobCodeInfoDataSet.setScmType(
          URLEncoder.encode(information.getString(Constant.CodeLocationJSONKey.SCM.getJSONKey()), "UTF-8"));
      _jobCodeInfoDataSet.setRepoName(
          URLEncoder.encode(information.getString(Constant.CodeLocationJSONKey.REPONAME.getJSONKey()), "UTF-8"));
    }
  }

  @Override
  public JobCodeInfoDataSet getJobCodeInfoDataSet() {
    return _jobCodeInfoDataSet;
  }

  private void processInformationForSourceCode() throws IOException, JSONException {
    String urlToGetSourceCode =
        String.format(COMPLETE_URL_TO_GET_CODE, _jobCodeInfoDataSet.getScmType(), _jobCodeInfoDataSet.getRepoName(),
            _jobCodeInfoDataSet.getFileName());
    logger.info(" Final URL to get source code " + urlToGetSourceCode);
    _jobCodeInfoDataSet.getMetaData().put("URL TO GET SOURCE CODE", urlToGetSourceCode);
    JSONObject jsonJobInfo = parseURL(urlToGetSourceCode);
    String sourceCode = getSourceCode(jsonJobInfo);
    logger.debug(" Following source code " + sourceCode);
    _jobCodeInfoDataSet.setSourceCode(sourceCode);
  }

  protected String getSourceCode(JSONObject jsonJobInfo) throws IOException, JSONException {
    return jsonJobInfo.getJSONObject(Constant.CodeLocationJSONKey.FILE_INFO.getJSONKey())
        .getString(Constant.CodeLocationJSONKey.SOURCE_CODE.getJSONKey());
  }

  protected JSONObject parseURL(String urlName) throws IOException, JSONException {
    URL url = new URL(urlName);
    BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
    String data = "";
    StringBuilder sb = new StringBuilder();
    while ((data = br.readLine()) != null) {
      sb.append(data);
    }
    br.close();
    return new JSONObject(sb.toString());
  }
}
