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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import play.libs.WS;
import play.test.FakeApplication;

import static common.TestConstants.*;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;
import static play.test.Helpers.testServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Tests the class {@link MetricsController}.
 */
public class MetricsControllerTest {

  private static FakeApplication fakeApp;

  @Before
  public void setup() {
    Map<String, String> additionalConfig = new HashMap<String, String>();
    additionalConfig.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    additionalConfig.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    additionalConfig.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    additionalConfig.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);
    additionalConfig.put(METRICS_ENABLE_KEY, "true");
    fakeApp = fakeApplication(additionalConfig);
  }

  /**
   * Test fetching metrics for queue sizes from Dr.Elephant's "/metrics" endpoint.
   */
  @Test
  public void testQueueSizeMetrics() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        // Initialize the metrics and set queue sizes for the main job queue, first retry queue and
        // second retry queue.
        MetricsController.init();
        MetricsController.setQueueSize(6);
        MetricsController.setRetryQueueSize(4);
        MetricsController.setSecondRetryQueueSize(2);

        // Initiate a request to metrics endpoint and verify the response.
        JsonNode jsonResponse = getMetricsEndpointResponse();
        JsonNode metricsNode = getAndVerifyJsonNode(jsonResponse, "metrics");
        assertMetricsIntValue(
            metricsNode, "AnalyticJob.jobQueue.size", 6);
        assertMetricsIntValue(
            metricsNode, "AnalyticJob.retryQueue.size", 4);
        assertMetricsIntValue(
            metricsNode, "AnalyticJob.secondRetryQueue.size", 2);
      }
    });
  }

  private static JsonNode getMetricsEndpointResponse() {
    WS.Response response = WS.url(BASE_URL + METRICS_ENDPOINT).
        get().get(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    return response.asJson();
  }

  private static JsonNode getAndVerifyJsonNode(JsonNode rootNode, String nodeName) {
    JsonNode node = rootNode.path(nodeName);
    assertNotNull(nodeName + " node should have been returned", node);
    return node;
  }

  private static void assertMetricsIntValue(JsonNode metricsNode, String metricName,
      int expectedValue) {
    JsonNode metricNameNode = getAndVerifyJsonNode(metricsNode, metricName);
    JsonNode valueNode = metricNameNode.path("value");
    assertNotNull("value node inside " +  metricName + " node should have "
        + "been returned", valueNode);
    assertEquals(expectedValue, valueNode.asInt());
  }
}
