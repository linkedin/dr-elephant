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

import controllers.HtmlUtil;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

import org.apache.http.NameValuePair;



public class AzkabanClient {

  private final Logger logger = Logger.getLogger(AzkabanClient.class);
  private String _azkabanUrl;
  private String _executionId;
  private String _sessionId;

  public AzkabanClient(String url) {
    if (url == null || url.isEmpty()) {
      logger.info("Empty Azkaban URL");
    }
    this._azkabanUrl = url;
    try {
      this._executionId = new URL(url).getQuery().toString().substring(7);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
  }

  public JSONObject fetchJson(List<NameValuePair> urlParameters) {

    HttpPost httpPost = new HttpPost(_azkabanUrl);
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
            "Login attempt failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: "
                + response.getStatusLine().getStatusCode());
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

  public void azkabanLogin(String userName, String password) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("action", "login"));
    urlParameters.add(new BasicNameValuePair("username", userName));
    urlParameters.add(new BasicNameValuePair("password", password));

    try {
      JSONObject jsonObject = fetchJson(urlParameters);

      if (!jsonObject.has("session.id")) {
        throw new RuntimeException("Login attempt failed. The session ID could not be obtained.");
      }

      this._sessionId = jsonObject.get("session.id").toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  String parseResponse(String response) {
    String newline = System.getProperty("line.separator");
    return HtmlUtil.toText(response).replaceAll("azkaban.failure.message=", newline + newline);
  }

  /**
   * Gets the content from the HTTP response.
   *
   * @return The content from the response
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

  public String getExecutionLog(String offset, String length) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", _sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchExecFlowLogs"));
    urlParameters.add(new BasicNameValuePair("execid", _executionId));
    urlParameters.add(new BasicNameValuePair("offset", offset));
    urlParameters.add(new BasicNameValuePair("length", length));

    try {
      JSONObject jsonObject = fetchJson(urlParameters);

      if (jsonObject.get("length").toString() == "0") {
        throw new RuntimeException("No log found for given execution url!.");
      }
      return jsonObject.get("data").toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  public String getJobLog(String jobId, String offset, String length) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("session.id", _sessionId));
    urlParameters.add(new BasicNameValuePair("ajax", "fetchExecJobLogs"));
    urlParameters.add(new BasicNameValuePair("execid", _executionId));
    urlParameters.add(new BasicNameValuePair("jobId", jobId));
    urlParameters.add(new BasicNameValuePair("offset", offset));
    urlParameters.add(new BasicNameValuePair("length", length));
    try {
      JSONObject jsonObject = fetchJson(urlParameters);

      if (jsonObject.get("length").toString() == "0") {
        logger.error("No log found for azkaban job" + jobId);
      }
      return jsonObject.get("data").toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }
}
