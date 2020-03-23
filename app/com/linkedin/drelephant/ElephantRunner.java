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

import com.avaje.ebean.Ebean;
import com.avaje.ebean.TxRunnable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.AnalyticJobGenerator;
import com.linkedin.drelephant.analysis.AnalyticJobGeneratorHadoop2;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.ElephantBackfillFetcher;
import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.priorityexecutor.Priority;
import com.linkedin.drelephant.priorityexecutor.PriorityBasedThreadPoolExecutor;
import com.linkedin.drelephant.priorityexecutor.RunnableWithPriority;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import controllers.MetricsController;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import models.AppResult;
import models.BackfillInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Logger;


/**
 * The class that runs the Dr. Elephant daemon
 */
public class ElephantRunner implements Runnable {
  private static final Logger logger = Logger.getLogger(ElephantRunner.class);

  private static final long FETCH_INTERVAL = 60 * 1000;     // Interval between fetches
  private static final long RETRY_INTERVAL = 60 * 1000;     // Interval between retries
  private static final long BACKFILL_RETRY_INTERVAL = 60 * 1000;     // Interval between retries for backfill
  // Buffer time of 10 seconds deducted from backfill ts retrieved from DB to account for time
  // taken to report job finish notification to RM.
  @VisibleForTesting
  static final long BACKFILL_BUFFER_TIME = 10 * 1000;
  // Max wait time on query with job prioritization flag.
  private static final long PRIORITIZATION_ON_JOB_QUERY_MAX_WAIT_INTERVAL = 60 * 1000;
  private static final int EXECUTOR_NUM = 5;                // The number of executor threads to analyse the jobs

  private static final String FETCH_INTERVAL_KEY = "drelephant.analysis.fetch.interval";
  private static final String RETRY_INTERVAL_KEY = "drelephant.analysis.retry.interval";
  private static final String BACKFILL_ENABLED_KEY = "drelephant.analysis.backfill.enabled";
  private static final String BACKFILL_RETRY_INTERVAL_KEY = "drelephant.analysis.backfill.retry.interval";
  private static final String PRIORITIZATION_ON_JOB_QUERY_MAX_WAIT_INTERVAL_KEY =
      "drelephant.analysis.prioritization-on-job-query.max-wait-interval";
  private static final String FETCH_INITIAL_WINDOW_MS = "drelephant.analysis.fetch.initial.windowMillis";
  private static final String EXECUTOR_NUM_KEY = "drelephant.analysis.thread.count";
  private static final String SUBMIT_BACKFILL_JOB_WITH_LOW_PRIORITY_KEY =
      "drelephant.analysis.submit-backfill-job.with.low-priority";
  private static final SimpleDateFormat DATE_FORMAT_GMT;
  static {
    DATE_FORMAT_GMT = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS' GMT'");
    DATE_FORMAT_GMT.setCalendar(Calendar.getInstance(new SimpleTimeZone(0, "GMT")));
  }

  private AtomicBoolean _running = new AtomicBoolean(true);
  private long lastRun;
  private long _fetchInterval;
  private long _retryInterval;
  private long _backfillRetryInterval;
  private long _prioritizationMaxInterval;
  private long _initialFetchWindow;
  private int _executorNum;
  private boolean _submitBackfillJobWithLowPriority = true;
  private HadoopSecurity _hadoopSecurity;
  private ThreadPoolExecutor _threadPoolExecutor;
  private AnalyticJobGenerator _analyticJobGenerator;
  private boolean _backfillEnabled = false;
  // Thread which is used for backfilling applications.
  private Thread _backFillThread = null;
  // Stores a unmodifiable view of Backfill info table, fetched at startup.
  private volatile Map<ApplicationType, Long> _appTypeToBackfillTs;
  // Mapping of application ID and Analytic Job (including the Future returned after job submission).
  private Map<String, AnalyticJob> _appToAnalyticJobMap = new ConcurrentHashMap<String, AnalyticJob>();
  private Object _appsLock = new Object();
  // Helper maps and variables used for populating backfill_info table.
  private Map<String, FinishTimeInfo> _appTypeToFinishTimeInfo = new HashMap<String, FinishTimeInfo>();

  private void loadGeneralConfiguration() {
    Configuration configuration = ElephantContext.instance().getGeneralConf();

    _executorNum = Utils.getNonNegativeInt(configuration, EXECUTOR_NUM_KEY, EXECUTOR_NUM);
    _fetchInterval = Utils.getNonNegativeLong(configuration, FETCH_INTERVAL_KEY, FETCH_INTERVAL);
    _retryInterval = Utils.getNonNegativeLong(configuration, RETRY_INTERVAL_KEY, RETRY_INTERVAL);
    _backfillEnabled = configuration.getBoolean(BACKFILL_ENABLED_KEY, false);
    logger.info("Backfill is " + (_backfillEnabled ? "" : "not") + " enabled");
    _backfillRetryInterval = Utils.getNonNegativeLong(
        configuration, BACKFILL_RETRY_INTERVAL_KEY, BACKFILL_RETRY_INTERVAL);
    _prioritizationMaxInterval = Utils.getNonNegativeLong(configuration,
        PRIORITIZATION_ON_JOB_QUERY_MAX_WAIT_INTERVAL_KEY,
        PRIORITIZATION_ON_JOB_QUERY_MAX_WAIT_INTERVAL);
    _initialFetchWindow = Utils.getNonNegativeLong(configuration, FETCH_INITIAL_WINDOW_MS, 0);
    _submitBackfillJobWithLowPriority = configuration.getBoolean(SUBMIT_BACKFILL_JOB_WITH_LOW_PRIORITY_KEY, true);
  }

  @VisibleForTesting
  public AnalyticJobGenerator getAnalyticJobGenerator() {
    if (HadoopSystemContext.isHadoop2Env()) {
      return new AnalyticJobGeneratorHadoop2();
    } else {
      throw new RuntimeException("Unsupported Hadoop major version detected. It is not 2.x.");
    }
  }

  private void loadAnalyticJobGenerator() {
    _analyticJobGenerator = getAnalyticJobGenerator();
    try {
      _analyticJobGenerator.configure(ElephantContext.instance().getGeneralConf());
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    logger.info("Dr.elephant has started");
    try {
      _hadoopSecurity = HadoopSecurity.getInstance();
      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          HDFSContext.load();
          loadGeneralConfiguration();
          loadAnalyticJobGenerator();
          ElephantContext.init();

          // Initialize the metrics registries.
          MetricsController.init();

          logger.info("executor num is " + _executorNum);
          if (_executorNum < 1) {
            throw new RuntimeException("Must have at least 1 worker thread.");
          }
          ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("dr-el-executor-thread-%d").build();
          _threadPoolExecutor = new PriorityBasedThreadPoolExecutor(_executorNum, _executorNum, 0L,
              TimeUnit.MILLISECONDS, factory);

          boolean firstRun = true;
          while (_running.get() && !Thread.currentThread().isInterrupted()) {
            _analyticJobGenerator.updateResourceManagerAddresses();
            lastRun = System.currentTimeMillis();

            logger.info("Fetching analytic job list...");

            try {
              _hadoopSecurity.checkLogin();
            } catch (IOException e) {
              logger.info("Error with hadoop kerberos login", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            List<AnalyticJob> todos;
            try {
              todos = _analyticJobGenerator.fetchAnalyticJobs();
            } catch (Exception e) {
              logger.error("Error fetching job list. Try again later...", e);
              //Wait for a while before retry
              waitInterval(_retryInterval);
              continue;
            }

            // Submit jobs fetched from RM and get the lowest finish time amongst the apps fetched from RM on first run.
            long lowestFinishTime = Long.MAX_VALUE;
            String appId = "";
            for (AnalyticJob analyticJob : todos) {
              if (firstRun && analyticJob.getFinishTime() < lowestFinishTime) {
                lowestFinishTime = analyticJob.getFinishTime();
                appId = analyticJob.getAppId();
              }
              synchronized (_appsLock) {
                Future<?> future = _threadPoolExecutor.submit(RunnableWithPriority.get(new ExecutorJob(analyticJob)));
                analyticJob.setJobFuture(future);
                addToFinishTimesMap(analyticJob.getAppType().getName(), analyticJob.getFinishTime());
                _appToAnalyticJobMap.put(analyticJob.getAppId(), analyticJob);
              }
            }

            // On first run, if backfill is enabled, launch a backfill thread to get and submit a list of apps to be
            // backfilled.
            if (firstRun && _backfillEnabled) {
              logger.info("Lowest finish time retrieved from RM is " +
                  DATE_FORMAT_GMT.format(new Date(lowestFinishTime)) + " for " + appId);
              List<BackfillData> backfillDataList = populateAndGetBackfillInfoData(lowestFinishTime);
              if (backfillDataList != null && !backfillDataList.isEmpty()) {
                _backFillThread = new Thread(new BackfillThread(backfillDataList, lowestFinishTime), "Backfill Thread");
                _backFillThread.start();
              } else {
                logger.info("Backfill is not required.");
              }
            }
            firstRun = false;
            int queueSize = _threadPoolExecutor.getQueue().size();
            MetricsController.setQueueSize(queueSize);
            logger.info("Job queue size is " + queueSize);

            //Wait for a while before next fetch
            waitInterval(_fetchInterval);
          }
          logger.info("Main thread is terminated.");
          return null;
        }
      });
    } catch (Exception e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));
    }
  }

  @VisibleForTesting
  Map<String, AnalyticJob> getAppToAnalyticJobMap() {
    return _appToAnalyticJobMap;
  }

  @VisibleForTesting
  FinishTimeInfo getFinishTimeInfo(String appType) {
    return _appTypeToFinishTimeInfo.get(appType);
  }

  @VisibleForTesting
  public ElephantBackfillFetcher getBackfillFetcher(ApplicationType appType) {
    return ElephantContext.instance().getBackfillFetcherForApplicationType(appType);
  }

  @VisibleForTesting
  public ApplicationType getAppTypeForName(String appType) {
    return ElephantContext.instance().getApplicationTypeForName(appType);
  }

  private List<BackfillData> populateAndGetBackfillInfoData(long lowestFinishTime) {
    List<BackfillInfo> backfillInfos = BackfillInfo.find.select("*").findList();
    // No data in backfill_info table.
    if (backfillInfos == null) {
      return null;
    }
    Map<ApplicationType, Long> tempBackfillMap = new HashMap<ApplicationType, Long>(backfillInfos.size());
    List<BackfillData> backfillDataList = new ArrayList<BackfillData>(backfillInfos.size());
    long currentTs = System.currentTimeMillis();
    // As number of app types would be limited, we do not expect this operation to be very expensive.
    for (BackfillInfo info : backfillInfos) {
      ApplicationType appType = getAppTypeForName(info.appType);
      long backfillTs = info.backfillTs;
      if (_initialFetchWindow > 0) {
        // Limit the apps to be backfilled based on initial fetch window. Initial fetch window configuration is used
        // to limit apps loaded from RM on Dr.Elephant startup. Hence, we need not backfill apps beyond the initial
        // fetch window as well.
        if (backfillTs < currentTs - _initialFetchWindow) {
          backfillTs = currentTs - _initialFetchWindow;
        }
      }
      tempBackfillMap.put(appType, backfillTs);
      ElephantBackfillFetcher backfillFetcher = getBackfillFetcher(appType);
      if (backfillFetcher == null) {
        logger.info("Fetcher configured for appType=" + appType.getName() + " does not support backfill.");
        continue;
      }
      if (lowestFinishTime < backfillTs) {
        continue;
      }
      backfillDataList.add(new BackfillData(appType, backfillTs, backfillFetcher));
    }
    _appTypeToBackfillTs = Collections.unmodifiableMap(tempBackfillMap);
    return backfillDataList;
  }

  private void addToFinishTimesMap(String appType, long finishTime) {
    FinishTimeInfo info = _appTypeToFinishTimeInfo.get(appType);
    if (info == null) {
      info = new FinishTimeInfo();
    }
    Integer count = info._appFinishTimesMap.get(finishTime);
    if (count == null) {
      count = 0;
    }
    info._appFinishTimesMap.put(finishTime, count + 1);
    _appTypeToFinishTimeInfo.put(appType, info);
  }

  // Called from a synchronized block.
  private void removeFromFinishTimesMap(Map<Long, Integer> finishTimesMap, long finishTime) {
    if (finishTimesMap != null && !finishTimesMap.isEmpty()) {
      Integer count = finishTimesMap.get(finishTime);
      if (count != null) {
        if (count == 1) {
          finishTimesMap.remove(finishTime);
        } else {
          finishTimesMap.put(finishTime, count - 1);
        }
      }
    }
  }

  /**
   * Get applicable finish time used while storing in app finish times map. This would typically be the app finish
   * time for apps fetched from RM. But for backfill jobs, if finish time could not be determined at the time of
   * fetching jobs for backfill, we will use the backfill ts fetched from backfill_info table instead (0 will be used
   * if an entry in backfill_info table could not be found as well).
   *
   * @param analyticJob Job for which applicable finish time needs to be found.
   * @return applicable finish time.
   */
  private long getApplicableFinishTime(AnalyticJob analyticJob) {
    long applicableFinishTime = analyticJob.getFinishTime();
    // Was backfilled with no finish time available at the time. Use value from backfill_info table instead.
    if (analyticJob.getIsBackfilledWithNoFinishTime()) {
      Long backfillTs = _appTypeToBackfillTs.get(analyticJob.getAppType());
      applicableFinishTime = ((backfillTs == null) ? 0 : backfillTs);
    }
    return applicableFinishTime;
  }

  /**
   * Class which holds information about the finish times to ensure correct backfill timestamp is updated in
   * backfill_info table. The class contains the following:
   *
   * 1. App finish times sorted map which stores the mapping between finish times for the apps and the reference count
   *    corresponding to the finish time. Reference count to handle the case where multiple apps have the same finish
   *    time. This map is required to keep track of the lowest finish time which is then used to update the
   *    backfill_info table. Finish times are stored in ascending order.
   * 2. Last Backfill ts saved in the backfill_info table. This allows us to keep track of the last backfill_ts updated
   *    in DB and hence ensures not sending an update unnecessarily if the same timestamp already exists in DB.
   * 3. Max finish time is used to keep track of maximum finish time amongst all the apps saved in DB thus far. This is
   *    also used to update the backfill_ts in the rare scenario when all the apps have been processed and no further
   *    apps are to be processed by Dr.Elephant. In this case the largest finish time amongst all the apps processed
   *    thus far needs to be updated in DB instead of the lowest finish time amongst the apps to be processed.
   */
  @VisibleForTesting
  static class FinishTimeInfo {
    NavigableMap<Long, Integer> _appFinishTimesMap = new TreeMap<Long, Integer>();
    private long _lastBackfillTsSaved = -1;
    private long _maxFinishTime = -1;
  }

  /**
   * Job which is run for analysis.
   */
  private class ExecutorJob implements Runnable {

    private AnalyticJob _analyticJob;

    ExecutorJob(AnalyticJob analyticJob) {
      _analyticJob = analyticJob;
    }

    @Override
    public void run() {
      String appType = _analyticJob.getAppType().getName();
      String analysisName = String.format("%s %s", appType, _analyticJob.getAppId());
      long analysisStartTimeMillis = System.currentTimeMillis();
      logger.info(String.format("Analyzing %s", analysisName));
      long applicableFinishTime = -1;
      long jobFinishTime = -1;
      FinishTimeInfo finishTimeInfo = null;
      try {
        final AppResult result = _analyticJob.getAnalysis();
        applicableFinishTime = getApplicableFinishTime(_analyticJob);
        synchronized (_appsLock) {
          finishTimeInfo = _appTypeToFinishTimeInfo.get(appType);
          final BackfillInfo backfillInfo = getBackfillInfoForSave(finishTimeInfo, appType, applicableFinishTime);
          jobFinishTime = result.finishTime;
          // Execute as a transaction.
          Ebean.execute(new TxRunnable() {
            public void run() {
              result.save();
              if (backfillInfo != null) {
                backfillInfo.save();
              }
            }
          });
          _appToAnalyticJobMap.remove(_analyticJob.getAppId());
          if (finishTimeInfo !=  null) {
            updateFinishTimeInfo(finishTimeInfo, backfillInfo, result.finishTime);
            removeFromFinishTimesMap(finishTimeInfo._appFinishTimesMap, applicableFinishTime);
          }
        }
        long processingTime = System.currentTimeMillis() - analysisStartTimeMillis;
        logger.info(String.format("Analysis of %s took %sms", analysisName, processingTime));
        MetricsController.setJobProcessingTime(processingTime);
        MetricsController.markProcessedJobs();

      } catch (InterruptedException e) {
        logger.info("Thread interrupted");
        logger.info(e.getMessage());
        logger.info(ExceptionUtils.getStackTrace(e));

        Thread.currentThread().interrupt();
      } catch (TimeoutException e){
        logger.warn("Timed out while fetching data. Exception message is: " + e.getMessage());
        jobFate(finishTimeInfo, appType, applicableFinishTime, jobFinishTime);
      } catch (Exception e) {
        logger.error(String.format("Failed to analyze %s", analysisName), e);
        jobFate(finishTimeInfo, appType, applicableFinishTime, jobFinishTime);
      }
    }

    /**
     * Get applicable finish time used while storing in app finish times map. This would typically be the app finish
     * time for apps fetched from RM. But for backfill jobs, if finish time could not be determined at the time of
     * fetching jobs for backfill, we will use the backfill ts fetched from backfill_info table instead (0 will be used
     * if an entry in backfill_info table could not be found as well).
     *
     * @return applicable finish time.
     */
    /*private long getApplicableFinishTime() {
      long applicableFinishTime = _analyticJob.getFinishTime();
      // Was backfilled with no finish time available at the time. Use value from backfill_info table instead.
      if (_analyticJob.getIsBackfilledWithNoFinishTime()) {
        Long backfillTs = _appTypeToBackfillTs.get(_analyticJob.getAppType());
        applicableFinishTime = ((backfillTs == null) ? 0 : backfillTs);
      }
      return applicableFinishTime;
    }*/

    /*
     * Presumed to be called under a synchronized block.
     */
    private BackfillInfo getBackfillInfoForSave(FinishTimeInfo finishTimeInfo, String appType,
        long applicableFinishTime) {
      BackfillInfo info = null;
      if (finishTimeInfo != null && !finishTimeInfo._appFinishTimesMap.isEmpty()) {
        Map.Entry<Long, Integer> lowestTsEntry = finishTimeInfo._appFinishTimesMap.firstEntry();
        long backfillTsToBeStored = lowestTsEntry.getKey();
        // If the first entry is the last entry as well and it will be removed at the end of this cycle,
        // this means that all the jobs upto the current max finish time have been stored. So store the max finish time
        // instead (only if max finish time > finish time for this app).
        if (finishTimeInfo._appFinishTimesMap.size() == 1 && lowestTsEntry.getValue() == 1 &&
            lowestTsEntry.getKey() == applicableFinishTime && backfillTsToBeStored < finishTimeInfo._maxFinishTime) {
          backfillTsToBeStored = finishTimeInfo._maxFinishTime;
        }
        if (finishTimeInfo._lastBackfillTsSaved != backfillTsToBeStored) {
          // Get hold of the record to update, if available, otherwise create a new one.
          info = BackfillInfo.find.byId(appType);
          if (info == null) {
            info = new BackfillInfo();
          }
          info.appType = appType;
          info.backfillTs = backfillTsToBeStored;
        }
      }

      return info;
    }

    private void updateLastBackfillTsSaved(FinishTimeInfo finishTimeInfo, BackfillInfo info) {
      // Update last timestamp if something has been saved in DB.
      if (info != null) {
        finishTimeInfo._lastBackfillTsSaved = info.backfillTs;
      }
    }

    private void updateFinishTimeInfo(FinishTimeInfo finishTimeInfo, BackfillInfo info, long jobFinishTime) {
      updateLastBackfillTsSaved(finishTimeInfo, info);
      // Update max finish time, if required.
      if (finishTimeInfo._maxFinishTime < jobFinishTime) {
        finishTimeInfo._maxFinishTime = jobFinishTime;
      }
    }

    private void jobFate (FinishTimeInfo finishTimeInfo, String appType, long applicableFinishTime,
        long jobFinishTime) {
      if (_analyticJob != null && _analyticJob.retry()) {
        logger.warn("Add analytic job id [" + _analyticJob.getAppId() + "] into the retry list.");
        _analyticJobGenerator.addIntoRetries(_analyticJob);
      } else if (_analyticJob != null && _analyticJob.isSecondPhaseRetry()) {
        //Putting the job into a second retry queue which fetches jobs after some interval. Some spark jobs may need
        // more time than usual to process, hence the queue.
        logger.warn("Add analytic job id [" + _analyticJob.getAppId() + "] into the second retry list.");
        _analyticJobGenerator.addIntoSecondRetryQueue(_analyticJob);
      } else {
        if (_analyticJob != null) {
          MetricsController.markSkippedJob();
          logger.error("Drop the analytic job. Reason: reached the max retries for application id = ["
                  + _analyticJob.getAppId() + "].");
          // Job is being dropped. Clean it up from corresponding maps.
          _appToAnalyticJobMap.remove(_analyticJob.getAppId());
          if (finishTimeInfo != null) {
            synchronized (_appsLock) {
              boolean saveSucceeded = false;
              long finishTime = (applicableFinishTime == -1) ?
                  getApplicableFinishTime(_analyticJob) : applicableFinishTime;
              // As this job has failed, its possible that this may have been the one with lowest finish time, hence
              // backfill_info table may have to be updated.
              BackfillInfo backfillInfo = null;
              try {
                backfillInfo = getBackfillInfoForSave(finishTimeInfo, appType, finishTime);
                // We do not retry currently, if DB operation fails while saving backfill_info.
                if (backfillInfo != null) {
                  backfillInfo.save();
                }
                saveSucceeded = true;
              } catch(Exception e) {
                logger.warn("Exception thrown while saving to backfill_info" + e.getMessage());
                logger.warn(ExceptionUtils.getStackTrace(e));
              }
              if (saveSucceeded) {
                // As the job analysis failed, no need to update max finish time.
                updateLastBackfillTsSaved(finishTimeInfo, backfillInfo);
              }
              // Remove from finish times map irrespective of failure during save. Rely on backfill ts to be updated
              // correctly on next run of analysis job.
              removeFromFinishTimesMap(finishTimeInfo._appFinishTimesMap, finishTime);
            }
          }
        }
      }
    }
  }

  private static class BackfillData {
    private ApplicationType _appType;
    private long _backfillTs;
    private ElephantBackfillFetcher _backfillFetcher;
    private BackfillData(ApplicationType appType, long backfillTs, ElephantBackfillFetcher backfillFetcher) {
      _appType = appType;
      _backfillTs = backfillTs;
      _backfillFetcher = backfillFetcher;
    }
  }

  /**
   * Thread which contacts different fetchers for the given application types to backfill applications based on timesamp
   * stored in backfill_info table and the lowest finish time amongst applications fetched from RM.
   */
  private class BackfillThread implements Runnable {
    private long _lowestFinishTimeFromRM;
    private List<BackfillData> _backfillDataList;
    //private final SimpleDateFormat _dateFormat;
    private BackfillThread(List<BackfillData> backfillDataList, long lowestFinishTimeFromRM) {
      _lowestFinishTimeFromRM = lowestFinishTimeFromRM;
      _backfillDataList = backfillDataList;
      //_dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS' GMT'");
      //_dateFormat.setCalendar(Calendar.getInstance(new SimpleTimeZone(0, "GMT")));
    }

    /**
     * Sort the backfill jobs, by finish time, in an ascending order, so that the jobs can be submitted for analysis in
     * that order.
     *
     * @param backfillJobs Backfill jobs to be sorted.
     */
    private void sortBackfillJobs(List<AnalyticJob> backfillJobs) {
      Collections.sort(backfillJobs, new Comparator<AnalyticJob>() {
        @Override
        public int compare(AnalyticJob job1, AnalyticJob job2) {
          // Push jobs which have finish time as -1, i.e. undetermined, to the end of list.
          if (job1.getFinishTime() == -1) {
            return ((job2.getFinishTime() == -1) ? 0 : 1);
          }
          long diff = job1.getFinishTime() - job2.getFinishTime();
          return (diff > 0) ? 1 : (diff < 0) ? -1 : 0;
        }
      });
    }

    private void augmentBackfillJob(AnalyticJob job) {
      // Set whatever fields we can based on information available.
      if (job.getFinishTime() <= 0) {
        job.setIsBackfilledWithNoFinishTime();
      }
      // Generate tracking URL based on RM address if tracking URL could not be populated while retrieving jobs for
      // backfill.
      if (StringUtils.isEmpty(job.getTrackingUrl())) {
        job.setTrackingUrl(StringUtils.join("http://",
            _analyticJobGenerator.getEffectiveResourceManagerAddress(), "/proxy/", job.getAppId()));
      }
    }

    private void fetchJobsForBackfill(List<AnalyticJob> backfillTodos, BackfillData data) throws Exception {
      List<AnalyticJob> jobsFetched = data._backfillFetcher.fetchJobsForBackfill(
          data._backfillTs - BACKFILL_BUFFER_TIME, _lowestFinishTimeFromRM);
      if (!jobsFetched.isEmpty()) {
        if (jobsFetched.get(0).getAppType() != null) {
          backfillTodos.addAll(jobsFetched);
        } else {
          // Set the app type as it has not been set by the backfill fetcher.
          for (AnalyticJob job : jobsFetched) {
            backfillTodos.add(job.setAppType(data._appType));
          }
        }
      }
    }

    private void submitBackfillJob(AnalyticJob job) {
      // Submit backfill job with low/normal priority depending on config.
      Priority jobPriority = _submitBackfillJobWithLowPriority ? Priority.LOW : Priority.NORMAL;
      Future<?> future = _threadPoolExecutor.submit(
          RunnableWithPriority.get(new ExecutorJob(job), jobPriority));
      job.setJobFuture(future).setJobExecutionPriority(jobPriority);
      _appToAnalyticJobMap.put(job.getAppId(), job);
      addToFinishTimesMap(job.getAppType().getName(), getApplicableFinishTime(job));
    }

    @Override
    public void run() {
      if (_backfillDataList == null || _backfillDataList.isEmpty()) {
        return;
      }
      logger.info("Backfilling starts...");
      long beginTime = Time.monotonicNow();
      int numOfJobsBackfilled = 0;
      while (!Thread.currentThread().isInterrupted()) {
        Iterator<BackfillData> iterator = _backfillDataList.iterator();
        List<AnalyticJob> backfillTodos = new ArrayList<AnalyticJob>();
        while (iterator.hasNext()) {
          BackfillData data = iterator.next();
          try {
            fetchJobsForBackfill(backfillTodos, data);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          } catch (Exception e) {
            logger.error("Error fetching jobs of appType=" + data._appType.getName() + " for backfill. " +
                "Will try again later...", e);
            continue;
          }
          logger.info("Jobs fetched for backfill for app type " + data._appType.getName() + " and time range: " +
              DATE_FORMAT_GMT.format(new Date(data._backfillTs - BACKFILL_BUFFER_TIME)) + " to " +
              DATE_FORMAT_GMT.format(new Date(_lowestFinishTimeFromRM)));

          iterator.remove();
        }
        if (!backfillTodos.isEmpty()) {
          sortBackfillJobs(backfillTodos);
          for (AnalyticJob job : backfillTodos) {
            // This application has already been picked up for analysis. No need to submit again to executor.
            if (_appToAnalyticJobMap.containsKey(job.getAppId())) {
              continue;
            }
            // This application has already been analysed, no need to analyse again.
            if (AppResult.find.byId(job.getAppId()) != null) {
              continue;
            }
            // Set whatever fields we can based on information available.
            augmentBackfillJob(job);
            synchronized (_appsLock) {
              submitBackfillJob(job);
              numOfJobsBackfilled++;
            }
          }
        }
        if (_backfillDataList.isEmpty()) {
          long endTime = Time.monotonicNow();
          logger.info("Finished backfilling jobs for analysis... " + numOfJobsBackfilled + " jobs backfilled. Took " +
              (endTime - beginTime) + " ms.");
          break;
        }
        // If for some application types, jobs to be backfilled could not be fetched, wait for backfill retry interval
        // before retrying.
        try {
          Thread.sleep(_backfillRetryInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @VisibleForTesting
  Future<?> prioritizeExecutionAndReturnFuture(String appId) {
    AnalyticJob job = _appToAnalyticJobMap.get(appId);
    if (job == null) {
      return null;
    }
    // Do not allow same job to be processed at the same time  because of parallel REST calls.
    synchronized (job) {
      Future<?> future = job.getJobFuture();
      if (job.getJobExecutionPriority() != Priority.HIGH && future != null) {
        boolean cancelled =  future.cancel(false);
        if (cancelled) {
          future = _threadPoolExecutor.submit(RunnableWithPriority.get(new ExecutorJob(job), Priority.HIGH));
          job.setJobFuture(future).setJobExecutionPriority(Priority.HIGH);
        }
      }
      return future;
    }
  }

  @VisibleForTesting
  void waitTillPrioritizedJobAnalysisFinishes(Future<?> future) {
    // Wait for the analysis task to finish.
    if (_prioritizationMaxInterval > 0) {
      long loopTimes = _prioritizationMaxInterval / 100;
      while (!future.isDone() && loopTimes > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        loopTimes--;
      }
    }
  }

  public boolean prioritizeExecutionAndWait(String appId) {
    Future<?> future = prioritizeExecutionAndReturnFuture(appId);
    if (future == null) {
      // App wasn't found.
      return false;
    }
    waitTillPrioritizedJobAnalysisFinishes(future);
    return true;
  }

  private void waitInterval(long interval) {
    // Wait for long enough
    long nextRun = lastRun + interval;
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

  public void kill() {
    _running.set(false);
    if (_threadPoolExecutor != null) {
      _threadPoolExecutor.shutdownNow();
    }
    // Stop the backfill thread by sending an interrupt.
    if (_backFillThread != null) {
      _backFillThread.interrupt();
      try {
        _backFillThread.join();
      } catch (InterruptedException e) {
        // Ignore as Dr.Elephant is going down.
      }
    }
  }
}