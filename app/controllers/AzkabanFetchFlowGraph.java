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

package controllers;

/**
 * Created by skakker on 08/06/16.
 */

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class AzkabanFetchFlowGraph {

  private static final Logger logger = Logger.getLogger(AzkabanFetchFlowGraph.class);
  private final SSLSocketFactory sslsf;
  AzkabanProject azkProject;

  public AzkabanFetchFlowGraph()
      throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException, UnrecoverableKeyException {
    this.sslsf = new SSLSocketFactory(new TrustStrategy() {
      @Override
      public boolean isTrusted(final X509Certificate[] chain, String authType)
          throws CertificateException {
        return true;
      }
    });
  }

  /**
   * Mehtod for fetching innodes infomation for a given project name and flow name.
   *
   * @param flowExecId Flow exec id of the project
   * @param projName Project name
   * @param flowName Flow name
   * @return a jsonArray containing flow information for the given project and flow name
   * @throws JSONException
   */

  public JSONArray fetch(String flowExecId, String projName, String flowName)
      throws JSONException {
    return fetchFlowGraph(readSession(flowExecId), flowExecId, projName, flowName);
  }

  /**
   * Helper method for fetching the flow information.
   *
   * @param sessionId
   * @param flowExecId
   * @param projName
   * @param flowName
   * @return a jsonArray containing flow information for the given project and flow name
   * @throws JSONException
   */
  public JSONArray fetchFlowGraph(String sessionId, String flowExecId, String projName, String flowName)
      throws JSONException {

    azkProject = new AzkabanProject();
    if (flowExecId != null) {
      if (flowExecId.length() >= 46) {
        azkProject.azkabanUrl = flowExecId.substring(0, flowExecId.indexOf("/executor"));
      }
    }

    azkProject.azkabanProjName = projName;

    // If no previous session is available, obtain a session id from server by sending login credentials.
    if (sessionId == null) {
      logger.info("No previous session found. Logging into Azkaban:");
      sessionId = azkabanLogin(azkProject.azkabanUrl, azkProject.azkabanUserName, azkProject.azkabanPassword);
    } else {
      logger.info("Resuming previous Azkaban session");
    }
    if (azkProject.azkabanUrl == null || azkProject.azkabanUrl.isEmpty()) {
      return null;
    }
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchflowgraph"));
    urlParameters.add(new BasicNameValuePair("project", azkProject.azkabanProjName));
    urlParameters.add(new BasicNameValuePair("flow", flowName));

    String paramString = URLEncodedUtils.format(urlParameters, "utf-8");
    HttpGet httpPost = new HttpGet(azkProject.azkabanUrl + "/manager?" + paramString);

    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Cookie", "azkaban.browser.session.id=" + sessionId);

    HttpClient httpClient = new DefaultHttpClient();

    Scheme scheme = new Scheme("https", sslsf, 443);
    httpClient.getConnectionManager().getSchemeRegistry().register(scheme);
    HttpResponse response = null;
    try {
      response = httpClient.execute(httpPost);
    } catch (IOException e) {
      logger.error("Error connecting to Azkaban: " + e);
    }

    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      throw new RuntimeException(
          "Login attempt failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: "
              + response.getStatusLine().getStatusCode());
    }

    String result = null;
    JSONArray jarr = null;

    try {
      result = parseContent(response.getEntity().getContent());
      JSONObject jsonObj = new JSONObject(result);

      if (jsonObj.has("error")) {
        String temp;
        temp = (String) jsonObj.get("error");
        if (temp.equals("session")) {
          //sessionId has expired. New sessionId generated
          logger.info("Generating new session ID");
          File dir = new File(System.getProperty("user.home") + "/.azkaban");
          if (!dir.exists() && !dir.mkdirs()) {
            logger.debug("Unable to create directory: " + dir.toString());
          }

          File file = new File(dir, "session_" + new URL(flowExecId).getHost());
          if (file.exists() && !file.delete()) {
            logger.debug("Unable to delete the existing file at: " + file.toString());
          }
          sessionId = azkabanLogin(azkProject.azkabanUrl, azkProject.azkabanUserName, azkProject.azkabanPassword);
          logger.debug("Refetching information from azkaban");
          urlParameters = new ArrayList<NameValuePair>();
          urlParameters.add(new BasicNameValuePair("session.id", sessionId));
          urlParameters.add(new BasicNameValuePair("ajax", "fetchflowgraph"));
          urlParameters.add(new BasicNameValuePair("project", azkProject.azkabanProjName));
          urlParameters.add(new BasicNameValuePair("flow", flowName));

          paramString = URLEncodedUtils.format(urlParameters, "utf-8");
          httpPost = new HttpGet(azkProject.azkabanUrl + "/manager?" + paramString);

          httpPost.setHeader("Accept", "*/*");
          httpPost.setHeader("Cookie", "azkaban.browser.session.id=" + sessionId);

          httpClient = new DefaultHttpClient();

          scheme = new Scheme("https", sslsf, 443);
          httpClient.getConnectionManager().getSchemeRegistry().register(scheme);
          response = null;
          try {
            response = httpClient.execute(httpPost);
          } catch (IOException e) {
            e.printStackTrace();
          }

          if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException(
                "Login attempt failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: "
                    + response.getStatusLine().getStatusCode());
          }
          result = null;
          jarr = null;
          jsonObj = new JSONObject(result);
        }
      }

      jarr = (JSONArray) jsonObj.get("nodes");
    } catch (IOException e) {
      e.printStackTrace();
    }
    return jarr;
  }

  /**
   * Helper method to make a login request to Azkaban.
   *
   * @param azkabanUrl The Azkaban server URL
   * @param userName The username
   * @param password The password
   * @return The session id from the response
   */

  public String azkabanLogin(String azkabanUrl, String userName, String password)
      throws JSONException {

    String sessionId = null;
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("action", "login"));
    urlParameters.add(new BasicNameValuePair("username", userName));
    urlParameters.add(new BasicNameValuePair("password", password));

    if (azkabanUrl == null || azkabanUrl.isEmpty()) {
      return null;
    }
    HttpPost httpPost = new HttpPost(azkabanUrl);
    try {
      httpPost.setEntity(new UrlEncodedFormEntity(urlParameters, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

    HttpClient httpClient = new DefaultHttpClient();
    try {
      Scheme scheme = new Scheme("https", sslsf, 443);
      httpClient.getConnectionManager().getSchemeRegistry().register(scheme);

      HttpResponse response = httpClient.execute(httpPost);

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new RuntimeException(
            "Login attempt failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: "
                + response.getStatusLine().getStatusCode());
      }

      String result = parseContent(response.getEntity().getContent());

      JSONObject jsonObj = new JSONObject(result);

      if (jsonObj.has("error")) {
        throw new RuntimeException(jsonObj.get("error").toString());
      }

      if (!jsonObj.has("session.id")) {
        throw new RuntimeException("Login attempt failed. The session ID could not be obtained.");
      }

      sessionId = jsonObj.get("session.id").toString();
      saveSession(sessionId, azkabanUrl);
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      httpClient.getConnectionManager().shutdown();
    }
    return sessionId;
  }

  /**
   * Gets the content from the HTTP response.
   *
   * @param response
   * @return The content from the response
   * @throws IOException
   */
  String parseContent(InputStream response)
      throws IOException {
    BufferedReader reader = null;
    StringBuilder result = new StringBuilder();
    try {
      reader = new BufferedReader(new InputStreamReader(response));

      String line = null;
      while ((line = reader.readLine()) != null) {
        result.append(line);
      }
      return result.toString();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return result.toString();
  }

  /**
   * Format the response String to remove HTML tags. This will improve the readability of warning
   * messages returned from Azkaban.
   *
   * @param response The HttpResponse from the server
   * @return The parsed response
   */
  String parseResponse(String response) {
    String newline = System.getProperty("line.separator");
    return HtmlUtil.toText(response).replaceAll("azkaban.failure.message=", newline + newline);
  }

  /**
   * Reads the session id from session.file in the user's ~/.azkaban directory.
   * @param flowExecId
   * @return sessionId the saved session id
   */
  String readSession(String flowExecId) {
    // TODO: Handle synchronization
    if(flowExecId==null || flowExecId.isEmpty())
      return null;
    String fileName = null;
    try {
      fileName = System.getProperty("user.home") + ".azkaban/session_" + new URL(flowExecId).getHost();
    } catch (MalformedURLException e) {
      logger.error("Error reading from session file " + fileName + "\n" + e);
    }

    File file = new File(fileName);
    String sessionId = null;
    if (file.exists()) {
      try {
        FileInputStream inputStream = new FileInputStream(fileName);
        String everything = IOUtils.toString(inputStream);
        sessionId = everything;
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return sessionId;
  }

  /**
   * Stores the session id in a file under ~/.azkaban.
   *
   * @param sessionId The session id
   */

  void saveSession(String sessionId, String url) {
    if (sessionId==null || sessionId.isEmpty()) {
      throw new RuntimeException("No session ID obtained to save");
    }

    File dir = new File(System.getProperty("user.home") + "/.azkaban");
    if (!dir.exists() && !dir.mkdirs()) {
      logger.debug("Unable to create directory: " + dir.toString());
      return;
    }

    File file = null;
    try {
      file = new File(dir, "session_" + new URL(url).getHost());
    } catch (MalformedURLException e) {
      logger.error("Error saving session id: " + e);
    }
    if (file.exists() && !file.delete()) {
      logger.debug("Unable to delete the existing file at: " + file.toString());
      return;
    }

    try {
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(sessionId);
      bw.close();
    } catch (IOException ex) {
      logger.debug("Unable to store session ID to file: " + file.toString() + "\n" + ex.toString());
    }
  }
}
