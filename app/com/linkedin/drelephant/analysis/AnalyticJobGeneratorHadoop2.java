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

package com.linkedin.drelephant.analysis;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.math.Statistics;
import controllers.MetricsController;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import models.AppResult;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.io.LZ4CompressionCodec;
import org.apache.spark.io.SnappyCompressionCodec;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.Lz4Codec;



/**
 * This class provides a list of analysis promises to be generated under Hadoop YARN environment
 */
public class AnalyticJobGeneratorHadoop2 implements AnalyticJobGenerator {
    private static final Logger logger = Logger.getLogger(AnalyticJobGeneratorHadoop2.class);
    private static final String RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address";
    private static final String IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled";
    private static final String RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids";
    private static final String RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info";
    private static final String FETCH_INITIAL_WINDOW_MS = "drelephant.analysis.fetch.initial.windowMillis";

    private Configuration configuration;

    // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
    private static final long FETCH_DELAY = 60000;

    // Generate a token update interval with a random deviation so that it does not update the token exactly at the same
    // time with other token updaters (e.g. ElephantFetchers).
    private static final long TOKEN_UPDATE_INTERVAL =
            Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS);

    private String _resourceManagerAddress;
    private long _lastTime = 0;
    private long _fetchStartTime = 0;
    private long _currentTime = 0;
    private long _tokenUpdatedTime = 0;
    private AuthenticatedURL.Token _token;
    private AuthenticatedURL _authenticatedURL;
    private final ObjectMapper _objectMapper = new ObjectMapper();

    private final Queue<AnalyticJob> _firstRetryQueue = new ConcurrentLinkedQueue<AnalyticJob>();

    private final List<AnalyticJob> _secondRetryQueue = new LinkedList<AnalyticJob>();

    public void updateResourceManagerAddresses() {
        if (Boolean.valueOf(configuration.get(IS_RM_HA_ENABLED))) {
            String resourceManagers = configuration.get(RESOURCE_MANAGER_IDS);
            if (resourceManagers != null) {
                logger.info("The list of RM IDs are " + resourceManagers);
                List<String> ids = Arrays.asList(resourceManagers.split(","));
                _currentTime = System.currentTimeMillis();
                updateAuthToken();
                for (String id : ids) {
                    try {
                        String resourceManager = configuration.get(RESOURCE_MANAGER_ADDRESS + "." + id);
                        String resourceManagerURL = String.format(RM_NODE_STATE_URL, resourceManager);
                        logger.info("Checking RM URL: " + resourceManagerURL);
                        JsonNode rootNode = readJsonNode(new URL(resourceManagerURL));
                        String status = rootNode.path("clusterInfo").path("haState").getValueAsText();
                        if (status.equals("ACTIVE")) {
                            logger.info(resourceManager + " is ACTIVE");
                            _resourceManagerAddress = resourceManager;
                            break;
                        } else {
                            logger.info(resourceManager + " is STANDBY");
                        }
                    } catch (AuthenticationException e) {
                        logger.info("Error fetching resource manager " + id + " state " + e.getMessage());
                    } catch (IOException e) {
                        logger.info("Error fetching Json for resource manager " + id + " status " + e.getMessage());
                    }
                }
            }
        } else {
            _resourceManagerAddress = configuration.get(RESOURCE_MANAGER_ADDRESS);
        }
        if (_resourceManagerAddress == null) {
            throw new RuntimeException(
                    "Cannot get YARN resource manager address from Hadoop Configuration property: [" + RESOURCE_MANAGER_ADDRESS + "].");
        }
    }

    @Override
    public String getEffectiveResourceManagerAddress() {
        return _resourceManagerAddress;
    }

    @Override
    public long getFetchStartTime() {
        return _fetchStartTime;
    }

    @Override
    public void configure(Configuration configuration)
            throws IOException {
        this.configuration = configuration;
        String initialFetchWindowString = configuration.get(FETCH_INITIAL_WINDOW_MS);
        if (initialFetchWindowString != null) {
            long initialFetchWindow = Long.parseLong(initialFetchWindowString);
            _lastTime = System.currentTimeMillis() - FETCH_DELAY - initialFetchWindow;
            _fetchStartTime = _lastTime;
        }
        updateResourceManagerAddresses();
    }

    /**
     * Fetch all the succeeded and failed applications/analytic jobs from the resource manager.
     *
     * @return a list of {@link AnalyticJob} objects based on applications fetched from RM.
     * @throws IOException
     * @throws AuthenticationException
     */

    private List<AnalyticJob> fetchAnalyticsJobsFromYarn()
            throws IOException, AuthenticationException {
        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();


        logger.info("Fetching recent finished application runs between last time: " + (_lastTime + 1)
                + ", and current time: " + _currentTime);

        // Fetch all succeeded apps
        URL succeededAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format(
                "/ws/v1/cluster/apps?finalStatus=SUCCEEDED&finishedTimeBegin=%s&finishedTimeEnd=%s",
                String.valueOf(_lastTime + 1), String.valueOf(_currentTime)));
        logger.info("The succeeded apps URL is " + succeededAppsURL);
        List<AnalyticJob> succeededApps = readApps(succeededAppsURL);
        appList.addAll(succeededApps);

        // Fetch all failed apps
        // state: Application Master State
        // finalStatus: Status of the Application as reported by the Application Master
        URL failedAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format(
                "/ws/v1/cluster/apps?finalStatus=FAILED&state=FINISHED&finishedTimeBegin=%s&finishedTimeEnd=%s",
                String.valueOf(_lastTime + 1), String.valueOf(_currentTime)));
        List<AnalyticJob> failedApps = readApps(failedAppsURL);
        logger.info("The failed apps URL is " + failedAppsURL);
        appList.addAll(failedApps);
        return appList;
    }
    private InputStream getRequiredCodecInputStream(Path filePath,InputStream inputStream){
        String extension = FilenameUtils.getExtension(filePath.getName().replaceFirst("\\.inprogress$", ""));
        switch (extension){
            case "snappy":
                return new SnappyCompressionCodec(new SparkConf()).compressedInputStream(inputStream);
            case "lz4":
                return new LZ4CompressionCodec(new SparkConf()).compressedInputStream(inputStream);
            default:
                return inputStream;
        }
    }
    private void fetchAnalyticsJobsFromEventLogsUtill(Path filePath, List<AnalyticJob> appList) {
        AnalyticJob analyticJob = new AnalyticJob();
        try {
            Configuration conf = new Configuration();
            FileSystem fs = filePath.getFileSystem(conf);

            logger.info("Processing Event log : " + filePath );
            try (InputStream inputStream = fs.open(filePath);
                 InputStream inputStreamDecompressed = getRequiredCodecInputStream(filePath,inputStream);
                 BufferedReader br = (inputStreamDecompressed != null) ? new BufferedReader(new InputStreamReader(inputStreamDecompressed)) : new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                String line;
                boolean appEndFound=false;
                boolean appStartFound=false;
                while ((line = br.readLine()) != null && ! (appStartFound && appEndFound) ) {
                    // Parse the JSON in the line
                    JSONObject log = new JSONObject(line);
                    if(log.has("Event")&& log.getString("Event").equals("SparkListenerLogStart"))
                        logger.info("log file spark version : "+ log.getString("Spark Version"));
                    // Check if the log has 'Completion' event and print the needed info
                    if (log.has("Event") && log.getString("Event").equals("SparkListenerApplicationEnd")) {
                        analyticJob.setFinishTime(log.getLong("Timestamp")); // Set finish time to 0 initially
                        appEndFound=true;
                    }

                    // Check if the log has the 'Start' event and print the needed info
                    if (log.has("Event") && log.getString("Event").equals("SparkListenerApplicationStart")) {
                        String appId = log.getString("App ID");
                        String appName = log.getString("App Name");
                        long startTime = log.getLong("Timestamp");
                        String sparkUser = log.getString("User");
                        ApplicationType applicationType = ElephantContext.instance().getApplicationTypeForName("SPARK");
                        analyticJob.setAppId(appId)
                                .setName(appName)
                                .setStartTime(startTime)
                                .setUser(sparkUser)
                                .setTrackingUrl("http://localhost:18080/history/" + appId + "/jobs/")
                                .setAppType(applicationType);
                        appStartFound=true;
                    }
                }
            }
            appList.add(analyticJob);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<AnalyticJob> fetchAnalyticsJobsFromEventLogs()
            throws IOException, AuthenticationException {
        String eventLogsDirectory = FetcherConfParser.extractEventLogLocationUri();

        if (eventLogsDirectory != null) {
            System.out.println("Event Log Location URI: " + eventLogsDirectory);
        } else {
            System.out.println("Event Log Location URI not found.");
        }

        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

        logger.info("Fetching recent finished application runs between last time: " + (_lastTime + 1)
                + ", and current time: " + _currentTime);

        logger.info("Event log directory " + eventLogsDirectory);
        try {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(eventLogsDirectory), false);

                while (fileStatusIterator.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatusIterator.next();
                    long modificationTime = fileStatus.getModificationTime();
                    if(!fileStatus.getPath().getName().startsWith(".") && !fileStatus.getPath().getName().endsWith(".inprogress")
                            && modificationTime >= _lastTime && modificationTime <= _currentTime)
                        fetchAnalyticsJobsFromEventLogsUtill(fileStatus.getPath(), appList);
                }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return appList;
    }

    @Override
    public List<AnalyticJob> fetchAnalyticJobs()
            throws IOException, AuthenticationException {
        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

        // There is a lag of job data from AM/NM to JobHistoryServer HDFS, we shouldn't use the current time, since there
        // might be new jobs arriving after we fetch jobs. We provide one minute delay to address this lag.
        _currentTime = System.currentTimeMillis() - FETCH_DELAY;
        updateAuthToken();

        String useYarn = System.getenv("USE_YARN");
        if (useYarn != null && useYarn.equalsIgnoreCase("true")) {
            appList = fetchAnalyticsJobsFromYarn();
        } else {
            appList = fetchAnalyticsJobsFromEventLogs();
        }
        // Append promises from the retry queue at the end of the list

        while (!_firstRetryQueue.isEmpty()) {
            appList.add(_firstRetryQueue.poll());
        }

        // Fetch jobs from second retry queue which are ready for second retry and
        // add to app list.
        fetchJobsFromSecondRetryQueue(appList);

        _lastTime = _currentTime;
        return appList;
    }

    @VisibleForTesting
    void fetchJobsFromSecondRetryQueue(List<AnalyticJob> appList) {
        synchronized (_secondRetryQueue) {
            Iterator iteratorSecondRetry = _secondRetryQueue.iterator();
            while (iteratorSecondRetry.hasNext()) {
                AnalyticJob job = (AnalyticJob) iteratorSecondRetry.next();
                if (job.readyForSecondRetry()) {
                    appList.add(job);
                    iteratorSecondRetry.remove();
                }
            }
        }
    }

    @Override
    public void addIntoRetries(AnalyticJob promise) {
        _firstRetryQueue.add(promise);
        int retryQueueSize = _firstRetryQueue.size();
        MetricsController.setRetryQueueSize(retryQueueSize);
        logger.info("Retry queue size is " + retryQueueSize);
    }

    @Override
    public void addIntoSecondRetryQueue(AnalyticJob job) {
        int secondRetryQueueSize;
        synchronized (_secondRetryQueue) {
            _secondRetryQueue.add(job.setTimeToSecondRetry());
            secondRetryQueueSize = _secondRetryQueue.size();
        }
        MetricsController.setSecondRetryQueueSize(secondRetryQueueSize);
        logger.info("Second Retry queue size is " + secondRetryQueueSize);
    }

    /**
     * Authenticate and update the token
     */
    private void updateAuthToken() {
        if (_currentTime - _tokenUpdatedTime > TOKEN_UPDATE_INTERVAL) {
            logger.info("AnalysisProvider updating its Authenticate Token...");
            _token = new AuthenticatedURL.Token();
            _authenticatedURL = new AuthenticatedURL();
            _tokenUpdatedTime = _currentTime;
        }
    }

    /**
     * Connect to url using token and return the JsonNode
     *
     * @param url The url to connect to
     * @return
     * @throws IOException             Unable to get the stream
     * @throws AuthenticationException Authencation problem
     */
    private JsonNode readJsonNode(URL url)
            throws IOException, AuthenticationException {
        return _objectMapper.readTree(url.openStream());
    }

    /**
     * Parse the returned json from Resource manager
     *
     * @param url The REST call
     * @return
     * @throws IOException
     * @throws AuthenticationException Problem authenticating to resource manager
     */
    private List<AnalyticJob> readApps(URL url) throws IOException, AuthenticationException {
        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

        JsonNode rootNode = readJsonNode(url);
        JsonNode apps = rootNode.path("apps").path("app");

        for (JsonNode app : apps) {
            String appId = app.get("id").getValueAsText();

            // When called first time after launch, hit the DB and avoid duplicated analytic jobs that have been analyzed
            // before.
            if (_lastTime > _fetchStartTime || (_lastTime == _fetchStartTime && AppResult.find.byId(appId) == null)) {
                String user = app.get("user").getValueAsText();
                String name = app.get("name").getValueAsText();
                String queueName = app.get("queue").getValueAsText();
                String trackingUrl = app.get("trackingUrl") != null ? app.get("trackingUrl").getValueAsText() : null;
                long startTime = app.get("startedTime").getLongValue();
                long finishTime = app.get("finishedTime").getLongValue();

                ApplicationType type =
                        ElephantContext.instance().getApplicationTypeForName(app.get("applicationType").getValueAsText());

                // If the application type is supported
                if (type != null) {
                    AnalyticJob analyticJob = new AnalyticJob();
                    analyticJob.setAppId(appId).setAppType(type).setUser(user).setName(name).setQueueName(queueName)
                            .setTrackingUrl(trackingUrl).setStartTime(startTime).setFinishTime(finishTime);

                    appList.add(analyticJob);
                }
            }
        }
        return appList;
    }
}
