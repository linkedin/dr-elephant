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

package com.linkedin.drelephant.clients.azkaban;

import com.linkedin.drelephant.clients.WorkflowClient;
import com.linkedin.drelephant.exceptions.JobState;
import com.linkedin.drelephant.exceptions.LoggingEvent;
import com.linkedin.drelephant.exceptions.azkaban.AzkabanJobLogAnalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 Client to interact with azkaban and get information about the workflow
 */
public class AzkabanWorkflowClient implements WorkflowClient {

  private final Logger logger = Logger.getLogger(AzkabanWorkflowClient.class);

  private String _workflowExecutionUrl;
  private String _azkabanUrl;
  private String _executionId;
  private String _sessionId;
  private String _username;
  private String _password;
  private long _sessionUpdatedTime = 0;

  private String AZKABAN_LOG_OFFSET = "0";
  private String AZKABAN_LOG_LENGTH_LIMIT = "9999999"; // limit the log limit to 10 mb

  Map<String, AzkabanJobLogAnalyzer> jobIdToLog;

  /**
   * Constructor for AzkabanWorkflowClient
   * @param url The url of the workflow
   * @throws URISyntaxException
   * @throws MalformedURLException
   */
  public AzkabanWorkflowClient(String url)
      throws URISyntaxException, MalformedURLException {
    if (url == null || url.isEmpty()) {
      throw new MalformedURLException("The Azkaban url is malformed");
    }
    this.setAzkabanServerUrl(url);
    this.setExecutionId(url);
    this._workflowExecutionUrl = url;
    this.jobIdToLog = new HashMap<String, AzkabanJobLogAnalyzer>();
  }

  /**
   * Making this client more usable by allowing to setURL runtime and get the status
   * @param url
   * @throws URISyntaxException
   * @throws MalformedURLException
   */
  public void setURL(String url)
      throws URISyntaxException, MalformedURLException {
    if (url == null || url.isEmpty()) {
      throw new MalformedURLException("The Azkaban url is malformed");
    }
    this.setAzkabanServerUrl(url);
    this.setExecutionId(url);
    this._workflowExecutionUrl = url;
  }

  /**
   * Sets the azkaban server url given the azkaban workflow url
   * @param azkabanWorkflowUrl The azkaban workflow url
   * @throws MalformedURLException
   * @throws URISyntaxException
   */
  private void setAzkabanServerUrl(String azkabanWorkflowUrl)
      throws MalformedURLException, URISyntaxException {
    this._azkabanUrl = "https://" + new URL(azkabanWorkflowUrl).getAuthority();
  }

  /**
   * Sets the workflow execution id given the azkaban workflow url
   * @param azkabanWorkflowUrl The url of the azkaban workflow
   * @throws MalformedURLException
   * @throws URISyntaxException
   */
  private void setExecutionId(String azkabanWorkflowUrl)
      throws MalformedURLException, URISyntaxException {
    List<NameValuePair> params = URLEncodedUtils.parse(new URI(azkabanWorkflowUrl), "UTF-8");
    for (NameValuePair param : params) {
      if (param.getName() == "execid") {
        this._executionId = param.getValue();
      }
    }
  }

  /**
   * Login using a private key
   * @param username The username of the user
   * @param _privateKey The path of the private key of the user
   */
  @Override
  public void login(String username, File _privateKey) {
    String headlessChallenge = null;
    String decodedPwd = null;
    try {
      headlessChallenge = getHeadlessChallenge(username);
      decodedPwd = decodeHeadlessChallenge(headlessChallenge, _privateKey);
    } catch (Exception e) {
      logger.error("Unexpected error encountered while decoding headless challenge " + headlessChallenge + e.toString());
    }
    login(username, decodedPwd);
  }

  public long getSessionUpdatedTime() {
    return _sessionUpdatedTime;
  }

  public void setSessionUpdatedTime(long sessionUpdatedTime) {
    _sessionUpdatedTime = sessionUpdatedTime;
  }

  /**
   * Authenticates Dr. Elephant in Azkaban and sets the sessionId
   *
   * @param userName The username of the user
   * @param password The password of the user
   */
  @Override
  public void login(String userName, String password) {
    this._username = userName;
    this._password = password;
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("action", "login"));
    urlParameters.add(new BasicNameValuePair("username", userName));
    urlParameters.add(new BasicNameValuePair("password", password));

    try {
      JSONObject jsonObject = fetchJson(urlParameters, _workflowExecutionUrl);
      if (!jsonObject.has("session.id")) {
        throw new RuntimeException("Login attempt failed. The session ID could not be obtained.");
      }
      this._sessionId = jsonObject.get("session.id").toString();
      logger.debug("Session ID is " + this._sessionId);
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  /**
   * Makes REST API Call for given url parameters and returns the json object
   *
   * @param urlParameters
   * @return Json Object in the response body
   */
  private JSONObject fetchJson(List<NameValuePair> urlParameters, String azkabanUrl) {
    HttpPost httpPost = new HttpPost(azkabanUrl);
    try {
      httpPost.setEntity(new UrlEncodedFormEntity(urlParameters, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

    HttpClient httpClient = new DefaultHttpClient();
    JSONObject jsonObj = null;
    try {
      SSLSocketFactory socketFactory = new SSLSocketFactory(new TrustStrategy() {
        @Override
        public boolean isTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
          return true;
        }
      });

      Scheme scheme = new Scheme("https", 443, socketFactory);
      httpClient.getConnectionManager().getSchemeRegistry().register(scheme);
      HttpResponse response = httpClient.execute(httpPost);

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new RuntimeException(
            response.getStatusLine().toString() + "\nStatus code: " + response.getStatusLine().getStatusCode());
      }

      String result = parseContent(response.getEntity().getContent());
      try {
        jsonObj = new JSONObject(result);
        if (jsonObj.has("error")) {
          throw new RuntimeException(jsonObj.get("error").toString());
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (UnrecoverableKeyException e) {
      e.printStackTrace();
    } catch (KeyManagementException e) {
      e.printStackTrace();
    } catch (KeyStoreException e) {
      e.printStackTrace();
    } finally {
      httpClient.getConnectionManager().shutdown();
    }
    return jsonObj;
  }

  /**
   * Parses the content given in the form of input stream to String
   * @param response the inputstream
   * @return The string parsed from the given inputstream
   * @throws IOException Throws IOException if the inputstream cannot be parsed to the string
   */
  private String parseContent(InputStream response)
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
   * @param username The username of the user
   * @return Encoded password of the user
   * @throws IOException private String getHeadlessChallenge(String username) throws IOException {
   */

  private String getHeadlessChallenge(String username)
      throws IOException {

    CloseableHttpClient httpClient = HttpClientBuilder.create().build(); //Use this instead
    String encodedPassword = null;

    try {
      logger.debug("Azkaban URL is " + _azkabanUrl);
      logger.debug("Username  " + username);
      String userUrl = _azkabanUrl + "/restli/liuser?action=headlessChallenge";
      HttpPost request = new HttpPost(userUrl);
      StringEntity params = new StringEntity("{\"username\":\"" + username + "\"}");
      request.addHeader("content-type", "application/json");
      request.setEntity(params);
      HttpResponse response = httpClient.execute(request);
      logger.debug("Response is " + response);
      String responseString = EntityUtils.toString(response.getEntity());
      JSONObject jobject = new JSONObject(responseString);
      encodedPassword = jobject.getString("value");
    } catch (Exception ex) {
      throw new RuntimeException("Unexpected exception in decoding headless account " + ex.toString());
    } finally {
      httpClient.close();
      return encodedPassword;
    }
  }

  /**
   * Decodes the encoded password using the _privateKey
   * @param encodedPassword
   * @param _privateKey
   * @return The decoded password
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   * @throws NoSuchPaddingException
   * @throws InvalidKeyException
   * @throws IllegalBlockSizeException
   * @throws BadPaddingException
   */
  private String decodeHeadlessChallenge(String encodedPassword, File _privateKey)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException,
             InvalidKeyException, IllegalBlockSizeException, BadPaddingException {

    final String RSA = "RSA";
    final String ASCII = "US-ASCII";

    // Read private key from file
    FileInputStream fstream = new FileInputStream(_privateKey);
    byte[] sshPrivateKey = IOUtils.toByteArray(fstream);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(sshPrivateKey);
    KeyFactory kf = KeyFactory.getInstance(RSA);
    PrivateKey privateKey = kf.generatePrivate(keySpec);

    // Init RSA decrypter with private key
    Cipher decryptCipher = Cipher.getInstance(RSA);
    decryptCipher.init(2, privateKey);

    // Convert base 64 password string to raw bytes
    byte[] rawBytes = org.apache.commons.codec.binary.Base64.decodeBase64(encodedPassword.getBytes(ASCII));

    // Decrypt the encoded raw bytes using decrypter
    byte[] decodedBytes = decryptCipher.doFinal(rawBytes);

    // Return decoded bytes as string
    return new String(decodedBytes, ASCII);
  }

  /**
   * Returns the jobs from the flow
   * @return The jobs from the flow
   */
  public Map<String, String> getJobsFromFlow() {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", _sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchexecflow"));
    urlParameters.add(new BasicNameValuePair("execid", _executionId));

    try {
      JSONObject jsonObject = fetchJson(urlParameters, _workflowExecutionUrl);
      JSONArray jobs = jsonObject.getJSONArray("nodes");
      Map<String, String> jobMap = new HashMap<String, String>();
      for (int i = 0; i < jobs.length(); i++) {
        JSONObject job = jobs.getJSONObject(i);
        jobMap.put(job.get("id").toString(), job.get("status").toString());
      }
      return jobMap;
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Returns the azkaban flow log
   * @param offset The offset from which logs should be found
   * @param maximumlLogLengthLimit The maximum log length limit
   * @return The azkaban flow logs
   */
  public String getAzkabanFlowLog(String offset, String maximumlLogLengthLimit) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", _sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchExecFlowLogs"));
    urlParameters.add(new BasicNameValuePair("execid", _executionId));
    urlParameters.add(new BasicNameValuePair("offset", offset));
    urlParameters.add(new BasicNameValuePair("length", maximumlLogLengthLimit));

    try {
      JSONObject jsonObject = fetchJson(urlParameters, _workflowExecutionUrl);
      if (jsonObject.getLong("length") == 0) {
        throw new RuntimeException("No log found for given execution url!.");
      }
      return jsonObject.get("data").toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void analyzeJob(String jobId) {
    String rawAzkabanJobLog = getAzkabanJobLog(jobId, AZKABAN_LOG_OFFSET, AZKABAN_LOG_LENGTH_LIMIT);
    AzkabanJobLogAnalyzer analyzedLog = new AzkabanJobLogAnalyzer(rawAzkabanJobLog);
    jobIdToLog.put(jobId, analyzedLog);
  }

  @Override
  public Set<String> getYarnApplicationsFromJob(String jobId) {
    if (!jobIdToLog.containsKey(jobId)) {
      throw new RuntimeException("No job with id " + jobId + " found");
    }
    return jobIdToLog.get(jobId).getSubEvents();
  }

  @Override
  public JobState getJobState(String jobId) {
    if (!jobIdToLog.containsKey(jobId)) {
      throw new RuntimeException("No job with id " + jobId + " found");
    }
    return jobIdToLog.get(jobId).getState();
  }

  @Override
  public LoggingEvent getJobException(String jobId) {
    if (!jobIdToLog.containsKey(jobId)) {
      throw new RuntimeException("No job with id " + jobId + " found");
    }
    return jobIdToLog.get(jobId).getException();
  }

  /**
   * Returns the Azkaban Job log for given Azkaban job id.
   *
   * @param jobId  Azkaban job id
   * @param offset Offset of log from the start
   * @param length Maximum limit on length of log
   * @return Azkaban job log in the form of string
   */
  public String getAzkabanJobLog(String jobId, String offset, String length) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", _sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchExecJobLogs"));
    urlParameters.add(new BasicNameValuePair("execid", _executionId));
    urlParameters.add(new BasicNameValuePair("jobId", jobId));
    urlParameters.add(new BasicNameValuePair("offset", offset));
    urlParameters.add(new BasicNameValuePair("length", length));
    try {
      JSONObject jsonObject = fetchJson(urlParameters, _workflowExecutionUrl);
      if (jsonObject.getLong("length") == 0) { // To do: If length ==0 throw exception
        logger.info("No log found for azkaban job" + jobId);
      }
      return jsonObject.get("data").toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }
}
