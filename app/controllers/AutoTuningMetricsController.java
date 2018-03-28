package controllers;

import static com.codahale.metrics.MetricRegistry.name;

import org.apache.log4j.Logger;

import play.Configuration;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.linkedin.drelephant.AutoTuner;


public class AutoTuningMetricsController extends Controller {

  private static final String METRICS_NOT_ENABLED = "Metrics not enabled";

  private static MetricRegistry _metricRegistry = null;
  private static final Logger logger = Logger.getLogger(AutoTuningMetricsController.class);

  private static int _fitnessComputeWaitExecutions = -1;
  private static int _baselineComputeWaitJobs = -1;
  private static int _azkabanStatusUpdateWaitExecutions = -1;
  private static int _paramSetGenerateWaitJobs = -1;

  private static Meter _getCurrentRunParametersFailures;
  private static Meter _fitnessComputedExecutions;
  private static Meter _successfulExecutions;
  private static Meter _failedExecutions;
  private static Meter _paramSetGenerated;
  private static Meter _baselineComputed;
  private static Meter _paramSetNotFound;
  private static Meter _newAutoTuningJob;

  private static Timer _getCurrentRunParametersTimer;

  public static void init() {

    // Metrics registries will be initialized only if enabled
    if (!Configuration.root().getBoolean("metrics", false)) {
      logger.debug("Metrics not enabled in the conf file.");
      return;
    }

    // Metrics & healthcheck registries will be initialized only once
    if (_metricRegistry != null) {
      logger.debug("Metric registries already initialized.");
      return;
    }

    _metricRegistry = new MetricRegistry();

    String autoTunerClassName = AutoTuner.class.getSimpleName();
    String apiClassName = Application.class.getSimpleName();

    //API timer and failed counts
    _getCurrentRunParametersTimer = _metricRegistry.timer(name(apiClassName, "getCurrentRunParametersResponses"));
    _getCurrentRunParametersFailures =
        _metricRegistry.meter(name(apiClassName, "getCurrentRunParametersFailures", "count"));

    //Daemon counters
    _fitnessComputedExecutions = _metricRegistry.meter(name(autoTunerClassName, "fitnessComputedExecutions", "count"));
    _successfulExecutions = _metricRegistry.meter(name(autoTunerClassName, "successfulExecutions", "count"));
    _failedExecutions = _metricRegistry.meter(name(autoTunerClassName, "failedExecutions", "count"));
    _paramSetGenerated = _metricRegistry.meter(name(autoTunerClassName, "paramSetGenerated", "count"));
    _baselineComputed = _metricRegistry.meter(name(autoTunerClassName, "baselineComputed", "count"));
    _paramSetNotFound = _metricRegistry.meter(name(autoTunerClassName, "paramSetNotFound", "count"));
    _newAutoTuningJob = _metricRegistry.meter(name(autoTunerClassName, "newAutoTuningJob", "count"));

    _metricRegistry.register(name(autoTunerClassName, "fitnessComputeWaitExecutions", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _fitnessComputeWaitExecutions;
      }
    });

    _metricRegistry.register(name(autoTunerClassName, "baselineComputeWaitJobs", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _baselineComputeWaitJobs;
      }
    });

    _metricRegistry.register(name(autoTunerClassName, "azkabanStatusUpdateWaitExecutions", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _azkabanStatusUpdateWaitExecutions;
      }
    });

    _metricRegistry.register(name(autoTunerClassName, "paramSetGenerateWaitJobs", "size"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return _paramSetGenerateWaitJobs;
      }
    });
  }
  public static void setFitnessComputeWaitJobs(int fitnessComputeWaitJobs) {
    _fitnessComputeWaitExecutions = fitnessComputeWaitJobs;
  }

  public static void setBaselineComputeWaitJobs(int baselineComputeWaitJobs) {
    _baselineComputeWaitJobs = baselineComputeWaitJobs;
  }

  public static void setAzkabanStatusUpdateWaitJobs(int azkabanStatusUpdateWaitJobs) {
    _azkabanStatusUpdateWaitExecutions = azkabanStatusUpdateWaitJobs;
  }

  public static void setParamSetGenerateWaitJobs(int paramSetGenerateWaitJobs) {
    _paramSetGenerateWaitJobs = paramSetGenerateWaitJobs;
  }

  public static void markSuccessfulJobs() {
    if (_successfulExecutions != null) {
      _successfulExecutions.mark();
    }
  }
  public static void markNewAutoTuningJob() {
    if (_newAutoTuningJob != null) {
      _newAutoTuningJob.mark();
    }
  }
  public static void markParamSetNotFound() {
    if (_paramSetNotFound != null) {
      _paramSetNotFound.mark();
    }
  }

  public static void markFailedJobs() {
    if (_failedExecutions != null) {
      _failedExecutions.mark();
    }
  }

  public static void markParamSetGenerated() {
    if (_paramSetGenerated != null) {
      _paramSetGenerated.mark();
    }
  }

  public static void markFitnessComputedJobs() {
    if (_fitnessComputedExecutions != null) {
      _fitnessComputedExecutions.mark();
    }
  }

  public static void markBaselineComputed() {
    if (_baselineComputed != null) {
      _baselineComputed.mark();
    }
  }

  public static void markGetCurrentRunParametersFailures() {
    if (_getCurrentRunParametersFailures != null) {
      _getCurrentRunParametersFailures.mark();
    }
  }

  public static Context getCurrentRunParametersTimerContext() {
    if(_getCurrentRunParametersTimer!=null)
    {
      return _getCurrentRunParametersTimer.time();
    }else
    {
      return null;
    }
  }

  /**
   * The endpoint /metrics
   * Endpoint can be queried if metrics is enabled.
   *
   * @return Will return all the metrics in Json format.
   */
  public static Result index() {
    if (_metricRegistry != null) {
      return ok(Json.toJson(_metricRegistry));
    } else {
      return ok(Json.toJson(METRICS_NOT_ENABLED));
    }
  }
}
