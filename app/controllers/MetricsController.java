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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.jvm.ThreadDeadlockHealthCheck;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.metrics.CustomGarbageCollectorMetricSet;
import org.apache.log4j.Logger;
import play.Configuration;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * This class enables the use of <a href="http://metrics.dropwizard.io">Dropwizard</a>
 * metrics for the application.
 *
 * <br><br>
 * The following endpoints are exposed.
 * <ul>/ping - Reports application status if up</ul>
 * <ul>/healthcheck - Returns status in Json format from all the implemented healthchecks</ul>
 * <ul>/metrics - Returns all the metrics in Json format</ul>
 */
public class MetricsController extends Controller {
  private static final Logger LOGGER = Logger.getLogger(MetricsController.class);

  private static MetricRegistry metricRegistry = null;
  private static HealthCheckRegistry healthCheckRegistry = null;

  private static final String METRICS_NOT_ENABLED = "Metrics not enabled";
  private static final String HEALTHCHECK_NOT_ENABLED = "Healthcheck not enabled";
  private static final String UNINITIALIZED_MESSAGE =
      "Metrics should be initialized before use.";

  private static int _queueSize = -1;
  private static int _retryQueueSize = -1;
  private static Meter skippedJobs;

  /**
   * Initializer method for the metrics registry. Call this method before registering
   * new metrics with the registry.
   */
  public static void init() {
    // Metrics registries will be initialized only if enabled
    if(!Configuration.root().getBoolean("metrics")) {
      LOGGER.debug("Metrics not enabled in the conf file.");
      return;
    }

    // Metrics & healthcheck registries will be initialized only once
    if(metricRegistry != null) {
      LOGGER.debug("Metric registries already initialized.");
      return;
    }

    metricRegistry = new MetricRegistry();

    String className = AnalyticJob.class.getSimpleName();

    metricRegistry.meter(name(className, "skippedJobs", "count"));

    metricRegistry.register(name(className, "jobQueue", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _queueSize;
      }
    });

    metricRegistry.register(name(className, "retryQueue", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _retryQueueSize;
      }
    });

    metricRegistry.registerAll(new CustomGarbageCollectorMetricSet());
    metricRegistry.registerAll(new MemoryUsageGaugeSet());

    JmxReporter.forRegistry(metricRegistry).build().start();

    healthCheckRegistry = new HealthCheckRegistry();

    healthCheckRegistry.register("ThreadDeadlockHealthCheck",
        new ThreadDeadlockHealthCheck());
  }

  /**
   *
   * @param name to be used while registering the timer.
   * @return Returns <code> Timer.Context </code> if metrics is enabled
   * and <code>null</code> otherwise.
   */
  public static Timer.Context startTimer(String name) {
    if(metricRegistry != null) {
      return metricRegistry.timer(name).time();
    } else {
      throw new NullPointerException(UNINITIALIZED_MESSAGE);
    }
  }

  /**
   *
   * @return The <code>MetricRegistry</code> if initialized.
   */
  public static MetricRegistry getMetricRegistry() {
    if (metricRegistry != null) {
      return metricRegistry;
    } else {
      throw new NullPointerException(UNINITIALIZED_MESSAGE);
    }
  }

  /**
   * Set the current job queue size in the metric registry.
   * @param size
   */
  public static void setQueueSize(int size) {
    _queueSize = size;
  }

  /**
   * Set the retry job queue size in the metric registry.
   * @param retryQueueSize
   */
  public static void setRetryQueueSize(int retryQueueSize) {
    _retryQueueSize = retryQueueSize;
  }

  /**
   * A meter for marking skipped jobs.
   * Jobs which doesn't have any data or which exceeds the set number of
   * retries can be marked as skipped.
   */
  public static void markSkippedJob() {
    skippedJobs.mark();
  }

  /**
   * The endpoint /ping
   * Ping will respond with the message 'alive' if the application is running.
   *
   * @return Will return 'alive' if Dr. Elephant is Up.
   */
  public static Result ping() {
    return ok(Json.toJson("alive"));
  }

  /**
   * The endpoint /metrics
   * Endpoint can be queried if metrics is enabled.
   *
   * @return Will return all the metrics in Json format.
   */
  public static Result index() {
    if (metricRegistry != null) {
      return ok(Json.toJson(metricRegistry));
    } else {
      return ok(Json.toJson(METRICS_NOT_ENABLED));
    }
  }

  /**
   * The endpoint /healthcheck
   * Endpoint can be queried if metrics is enabled.
   *
   * @return Will return all the healthcheck metrics in Json format.
   */
  public static Result healthcheck() {
    if (healthCheckRegistry != null) {
      return ok(Json.toJson(healthCheckRegistry.runHealthChecks()));
    } else {
      return ok(Json.toJson(HEALTHCHECK_NOT_ENABLED));
    }
  }
}
