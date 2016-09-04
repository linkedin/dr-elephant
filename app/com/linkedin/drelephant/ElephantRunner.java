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

package com.linkedin.drelephant;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.AnalyticJobGenerator;
import com.linkedin.drelephant.analysis.AnalyticJobGeneratorHadoop2;
import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import models.AppResult;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.log4j.Logger;


/**
 * The class that runs the Dr. Elephant daemon
 */
public class ElephantRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);

  private static final String SPARK_APP_TYPE = "SPARK";
  private static final String RUNNING_JOB_POOL_SIZE_KEY = "drelephant.analysis.realtime.thread.count";
  private static final String COMPLETED_JOB_POOL_SIZE_KEY = "drelephant.analysis.completed.thread.count";
  private static final String FETCH_INTERVAL_KEY = "drelephant.analysis.fetch.interval";
  private static final String FETCH_LAG_KEY = "drelephant.analysis.fetch.lag";
  private static final String RUNNING_JOB_UPDATE_INTERVAL_KEY = "drelephant.analysis.realtime.update.interval";

  // Job States
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final String KILLED = "KILLED";
  private static final String UNDEFINED = "UNDEFINED";
  private static final String RUNNING = "RUNNING";
  private static final String PREP = "PREP";

  // Default interval between fetches
  private static final long DEFAULT_FETCH_INTERVAL = Statistics.MINUTE_IN_MS;
  // Default frequency with which running jobs should be analysed
  private static final long DEFAULT_RUNNING_JOB_UPDATE_INTERVAL = Statistics.MINUTE_IN_MS;
  // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
  private static final long DEFAULT_INITIAL_FETCH_LAG = Statistics.MINUTE_IN_MS;
  // The default number of executor threads to analyse completed jobs
  private static final long DEFAULT_COMPLETED_JOB_POOL_SIZE = 5;
  // The default number of executor threads to analyse running jobs
  private static final long DEFAULT_RUNNING_JOB_POOL_SIZE = 5;

  private final HadoopSecurity _hadoopSecurity;
  private final Configuration _configuration;
  private long _completedExecutorCount = DEFAULT_COMPLETED_JOB_POOL_SIZE;
  private long _runningExecutorCount = DEFAULT_RUNNING_JOB_POOL_SIZE;
  private long _fetchInterval = DEFAULT_FETCH_INTERVAL;
  private long _runningJobUpdateInterval = DEFAULT_RUNNING_JOB_UPDATE_INTERVAL;
  private long _fetchLag = DEFAULT_INITIAL_FETCH_LAG;

  // Executor for SUCCEEDED and FAILED jobs
  private ThreadPoolExecutor _completedJobThreadPoolExecutor;
  // Executor for Jobs in Undefined state (RUNNING, PREP)
  private ScheduledThreadPoolExecutor _undefinedJobThreadPoolExecutor;

  private Cluster _cluster;
  private AnalyticJobGenerator _analyticJobGenerator;
  private long _lastTime = 0;
  private long _currentTime = 0;
  private AtomicBoolean _running = new AtomicBoolean(true);
  private Map<String, ScheduledFuture> _analyticJobScheduledFutureMap = new HashMap<String, ScheduledFuture>();

  public ElephantRunner() {
    _hadoopSecurity = new HadoopSecurity();
    _configuration = new Configuration();
    _analyticJobGenerator = new AnalyticJobGeneratorHadoop2();
  }

  @Override
  public void run() {
    try {
      logger.info("Dr.elephant has started");
      initialize();
      _cluster = new Cluster(_configuration);
      _hadoopSecurity.login();
      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          while (_running.get() && !Thread.currentThread().isInterrupted()) {
            _currentTime = System.currentTimeMillis();
            _analyticJobGenerator.updateResourceManagerAddresses();

            // Kerberos Authentation
            try {
              _hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              waitInterval(_fetchInterval);
              continue;
            }

            fetchApplications(_lastTime + 1, _currentTime);
            waitInterval(_fetchInterval);
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (Exception e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));
      _completedJobThreadPoolExecutor.shutdown();
      _undefinedJobThreadPoolExecutor.shutdown();
    }
  }

  private void initialize() {
    if (!HadoopSystemContext.isHadoop2Env()) {
      throw new RuntimeException("Unsupported Hadoop major version detected. It is not 2.x.");
    }

    try {
      _analyticJobGenerator.configure(_configuration);
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }

    HDFSContext.load();
    ElephantContext.init();
    loadGeneralConfiguration();
    loadExecutorThreads();
  }

  /**
   * Load all the properties from GeneralConf.xml
   */
  private void loadGeneralConfiguration() {
    _configuration.addResource(this.getClass().getClassLoader().getResourceAsStream("GeneralConf.xml"));

    _completedExecutorCount =
        getLongFromConf(_configuration, COMPLETED_JOB_POOL_SIZE_KEY, DEFAULT_COMPLETED_JOB_POOL_SIZE);
    _runningExecutorCount = getLongFromConf(_configuration, RUNNING_JOB_POOL_SIZE_KEY, DEFAULT_RUNNING_JOB_POOL_SIZE);

    _fetchInterval = getLongFromConf(_configuration, FETCH_INTERVAL_KEY, DEFAULT_FETCH_INTERVAL);
    _fetchLag = getLongFromConf(_configuration, FETCH_LAG_KEY, DEFAULT_INITIAL_FETCH_LAG);

    _runningJobUpdateInterval =
        getLongFromConf(_configuration, RUNNING_JOB_UPDATE_INTERVAL_KEY, DEFAULT_RUNNING_JOB_UPDATE_INTERVAL);
  }

  /**
   * Extract a long value from the configuration
   *
   * @param conf The configuration object
   * @param key The key to be extracted
   * @param defaultValue The default value to use when key is not found
   * @return the extracted value
   */
  private long getLongFromConf(Configuration conf, String key, long defaultValue) {
    long result = defaultValue;
    try {
      result = conf.getLong(key, defaultValue);
    } catch (NumberFormatException e) {
      logger.error("Invalid configuration " + key + " in GeneralConf.xml. Value: " + conf.get(key)
          + ". Resetting it to default value: " + defaultValue);
    }
    return result;
  }

  /**
   * Create a thread pool and load all the executor threads for running and completed jobs
   */
  private void loadExecutorThreads() {
    if (_completedExecutorCount < 1) {
      throw new RuntimeException("Must have at least 1 worker thread for analyzing completed jobs.");
    }

    logger.info("The number of threads analysing completed jobs is " + _completedExecutorCount);
    ThreadFactory completedJobfactory =
        new ThreadFactoryBuilder().setNameFormat("dr-completed-executor-thread-%d").build();
    _completedJobThreadPoolExecutor =
        new ScheduledThreadPoolExecutor((int)_completedExecutorCount, completedJobfactory);

    if (_runningExecutorCount < 0) {
      _runningExecutorCount = 0;
    }
    logger.info("The number of threads analysing running jobs is " + _runningExecutorCount);
    ThreadFactory runningJobFactory = new ThreadFactoryBuilder().setNameFormat("dr-running-executor-thread-%d").build();
    _undefinedJobThreadPoolExecutor = new ScheduledThreadPoolExecutor((int) _runningExecutorCount, runningJobFactory);
    _undefinedJobThreadPoolExecutor.setRemoveOnCancelPolicy(true);
  }

  /**
   * Fetch all the completed jobs and jobs in <i>undefined</i> state from the resource manager.
   *
   * Ideally we want to fetch only the completed and running jobs but since the resource manager
   * api doesn't allow filtering by running job, we will fetch all the jobs in <i>undefined</i>
   * state and then poll them.
   *
   * @param from Lower limit on the start time of the job in millisec
   * @param to Upper limit on the start time of the job ib millisec
   */
  private void fetchApplications(long from, long to) {
    logger.info("Fetching all the analytic apps which started between " + from + " and " + to);
    logger.info("No of active completed threads: " + _completedJobThreadPoolExecutor.getActiveCount());
    logger.info("No of active running threads: " + _undefinedJobThreadPoolExecutor.getActiveCount());

    List<AnalyticJob> undefinedJobs = new ArrayList<AnalyticJob>();
    List<AnalyticJob> completedJobs = new ArrayList<AnalyticJob>();
    try {
      _analyticJobGenerator.updateAuthToken(to);

      // Since the running jobs complete at some stage, we want at least one completed executor thread
      if (_runningExecutorCount > 0 && _completedExecutorCount > 0) {
        undefinedJobs = _analyticJobGenerator.fetchUndefinedAnalyticJobs(from, to);
      }
      if (_completedExecutorCount > 0) {
        // Because of the delay in completed jobs moving to hdfs, we will fetch completed jobs before the lag.
        completedJobs = _analyticJobGenerator.fetchCompletedAnalyticJobs(Math.max(from - _fetchLag, 0), to - _fetchLag);
      }
    } catch (Exception e) {
      logger.error("Error fetching job list. Try again later...", e);
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
      }
      return;
    }

    logger.info("Completed Job queue size is " + (getCompletedThreadPoolQueueSize() + completedJobs.size()));
    for (AnalyticJob analyticJob : completedJobs) {
      _completedJobThreadPoolExecutor.submit(new CompletedExecutorJob(analyticJob));
    }

    logger.info("Undefined Job queue size is " + (getUndefinedThreadPoolQueueSize() + undefinedJobs.size()));
    for (AnalyticJob analyticJob : undefinedJobs) {
      ScheduledFuture<?> future =
          _undefinedJobThreadPoolExecutor.scheduleAtFixedRate(new UndefinedExecutorJob(analyticJob), 0,
              _runningJobUpdateInterval, TimeUnit.MILLISECONDS);
      _analyticJobScheduledFutureMap.put(analyticJob.getAppId(), future);
    }

    _lastTime = to;
  }

  /**
   * Add analytic job into the retry queue or drop it depending the number of retries.
   *
   * @param analyticJob the job being analyzed
   */
  private void retryOrDrop(AnalyticJob analyticJob) {
    if (analyticJob != null && analyticJob.retry()) {
      logger.error("Add analytic job id [" + analyticJob.getAppId() + "] into the retry list.");
      _analyticJobGenerator.addIntoRetries(analyticJob);
    } else {
      if (analyticJob != null) {
        logger.error(
            "Drop the analytic job. Reason: reached the max retries for application id = [" + analyticJob.getAppId()
                + "].");
      }
    }
  }

  /**
   * Sleep for <i>interval</i> time
   *
   * @param interval the time to wait/sleep
   */
  private void waitInterval(long interval) {
    // Wait for long enough
    long nextRun = _lastTime + interval;
    long waitTime = nextRun - System.currentTimeMillis();

    if (waitTime <= 0) {
      return;
    }

    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void stopFurtherAnalysis(String appId) {
    if (_analyticJobScheduledFutureMap.containsKey(appId)) {
      _analyticJobScheduledFutureMap.remove(appId).cancel(false);
    }
  }

  public int getCompletedThreadPoolQueueSize() {
    return _completedJobThreadPoolExecutor.getQueue().size();
  }

  public int getUndefinedThreadPoolQueueSize() {
    return _undefinedJobThreadPoolExecutor.getQueue().size();
  }

  private void saveOrUpdate(AppResult result) {
    AppResult jobInDb = AppResult.find.byId(result.id);
    if (jobInDb == null) {
      result.save();
    } else {
      // Delete and save. Prevents Optimistic Locking
      jobInDb.delete();
      result.save();
    }
  }

  /**
   * Kill the Dr. Elephant daemon
   */
  public void kill() {
    _running.set(false);
    if (_completedJobThreadPoolExecutor != null) {
      _completedJobThreadPoolExecutor.shutdownNow();
    }
    if (_undefinedJobThreadPoolExecutor != null) {
      _undefinedJobThreadPoolExecutor.shutdownNow();
    }
  }

  public boolean isKilled() {
    if (_completedJobThreadPoolExecutor != null && _undefinedJobThreadPoolExecutor != null) {
      return !_running.get() && _completedJobThreadPoolExecutor.isShutdown() &&
          _undefinedJobThreadPoolExecutor.isShutdown();
    }
    return true;
  }

  /**
   * Analyze Completed jobs
   */
  private class CompletedExecutorJob implements Runnable {

    private AnalyticJob _analyticJob;

    CompletedExecutorJob(AnalyticJob analyticJob) {
      this._analyticJob = analyticJob;
      _analyticJob.setAnalysisStartTime(System.currentTimeMillis());
    }

    @Override
    public void run() {
      try {
        logger.info(String.format("Analyzing %s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId()));
        String finalStatus = _analyticJob.getFinalStatus();
        if (finalStatus.equals(SUCCEEDED) || finalStatus.equals(FAILED)) {
          // The finalStatus obtained from the Resource Manager Rest api is inconsistent with the final
          // jobStatus as obtained from the mapreduce client api. Making them same will make it easier
          // for us to handle the different cases(completed and running).
          _analyticJob.setJobStatus(finalStatus);
        }
        String appState = _analyticJob.getJobStatus();
        String analysisName =
            String.format("%s %s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId(), appState);
        logger.info(String.format("%s %s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId(), appState));

        if (appState.equals(SUCCEEDED) || appState.equals(FAILED) || appState.equals(KILLED)) {
          AppResult result = _analyticJob.getAnalysis(null);
          stopFurtherAnalysis(_analyticJob.getAppId());
          saveOrUpdate(result);
          logger.info(String.format("Analysis of %s took %sms", analysisName,
              System.currentTimeMillis() - _analyticJob.getAnalysisStartTime()));
        } else {
          // Should not reach here. We consider only COMPLETED JOBs in _completedJobQueue
          throw new RuntimeException(_analyticJob.getAppId() + " is in " + appState + " state and final status is " +
              _analyticJob.getFinalStatus() + ". Please debug this issue. We consider only Succeeded and Failed jobs in"
              + " Completed Jobs Queue");
        }
      } catch (InterruptedException e) {
        logger.info("Thread interrupted");
        logger.info(e.getMessage());
        logger.info(ExceptionUtils.getStackTrace(e));
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error(e.getMessage());
        logger.error(ExceptionUtils.getStackTrace(e));
        retryOrDrop(_analyticJob);
      }
    }
  }

  /**
   * Analyze Running jobs
   */
  private class UndefinedExecutorJob implements Runnable {

    private AnalyticJob _analyticJob;

    UndefinedExecutorJob(AnalyticJob analyticJob) {
      this._analyticJob = analyticJob;
    }

    @Override
    public void run() {
      logger.info(String.format("Analyzing %s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId()));
      if (_analyticJob.getAppType().getName().toUpperCase().equals(SPARK_APP_TYPE)) {
        stopFurtherAnalysis(_analyticJob.getAppId());
        return;
      }

      Job job = null;
      try {
        job = _cluster.getJob(JobID.forName(Utils.getJobIdFromApplicationId(_analyticJob.getAppId())));
        if (job == null) {
          throw new RuntimeException(_analyticJob.getAppId() + " cannot be found.");
        } else {
          _analyticJob.setJobStatus(job.getJobState().name());
        }
      } catch (Exception e) {
        logger.error(e.getMessage());
        logger.info("Removing " + _analyticJob.getAppId() + " from the undefined job queue.");
        stopFurtherAnalysis(_analyticJob.getAppId());
        return;
      }
      logger.info(String.format("%s %s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId(),
          _analyticJob.getJobStatus()));

      try {
        String appState = _analyticJob.getJobStatus();
        if (appState.equals(SUCCEEDED) || appState.equals(FAILED) || appState.equals(KILLED)) {
          stopFurtherAnalysis(_analyticJob.getAppId());
        } else if (appState.equals(RUNNING)) {
          AppResult result = _analyticJob.getAnalysis(job);
          saveOrUpdate(result);
        } else if (appState.equals(PREP) || appState.equals(UNDEFINED)) {
          // Ignore Job State and continue
        } else {
          logger.error(_analyticJob + " is in an unknown state. Removing it from undefined queue.");
          stopFurtherAnalysis(_analyticJob.getAppId());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error(e.getMessage());
        logger.error(ExceptionUtils.getStackTrace(e));
      }
    }
  }
}
