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

package common;

import scala.Int;


public class TestConstants {

  // Test data constants
  public static final int TEST_SERVER_PORT = 9001;
  public static final String BASE_URL = "http://localhost:" + TEST_SERVER_PORT;
  public static final String TEST_DATA_FILE = "test/resources/test-init.sql";
  public static final String TEST_IPSO_DATA_FILE = "test/resources/test-init-ipso.sql";
  public static final String TEST_BASELINE_DATA_FILE = "test/resources/test-init-baseline.sql";
  public static final String TEST_JOB_STATUS_DATA_FILE = "test/resources/test-job-status-data.sql";
  public static final String TEST_AUTO_TUNING_DATA_FILE1 = "test/resources/tunein-test1.sql";
  public static final String TEST_FITNESS_CALCULATION_DATA_FILE = "test/resources/test-fitness-calculation-data.sql";
  public static final String TEST_PARAM_GENERATE_DATA_FILE = "test/resources/test-param-generate-data.sql";
  public static final String TEST_SPARK_HBT_PARAM_RECOMMENDER = "test/resources/test-SparkHBTParamRecommender.sql";

  public static final int RESPONSE_TIMEOUT = 3000; // milliseconds

  public static final String TEST_JOB_ID1 = "application_1458194917883_1453361";
  public static final String TEST_JOB_ID2 = "application_1458194917883_1453362";
  public static final String TEST_JOB_NAME = "Email Overwriter";
  public static final String TEST_JOB_TYPE = "HadoopJava";
  public static final String TEST_APP_TYPE = "HadoopJava";
  public static final String TEST_USERNAME = "growth";
  public static final String TEST_DEFAULT_QUEUE_NAME = "default";

  public static final String TEST_JOB_EXEC_ID1 =
      "https://elephant.linkedin.com:8443/executor?execid=1654676&job=overwriter-reminder2&attempt=0";
  public static final String TEST_JOB_EXEC_ID2 =
      "https://elephant.linkedin.com:8443/executor?execid=1654677&job=overwriter-reminder2&attempt=0";

  public static final String TEST_FLOW_EXEC_ID1 =
      "https://elephant.linkedin.com:8443/executor?execid=1654676";
  public static final String TEST_FLOW_EXEC_ID2 =
      "https://elephant.linkedin.com:8443/executor?execid=1654677";
  public static final String TEST_FLOW_DEF_ID1 =
      "https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder";
  public static final String TEST_JOB_DEF_ID1 =
      "https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder&job=overwriter-reminder2";

  public static final String TEST_START_TIME1 = "2016-04-18";
  public static final String TEST_END_TIME1 = "2016-04-19";
  // DB connection strings
  public static final String DB_DEFAULT_DRIVER_KEY = "db.default.driver";
  public static final String DB_DEFAULT_DRIVER_VALUE = "org.h2.Driver";
  public static final String DB_DEFAULT_URL_KEY = "db.default.url";
  public static final String DB_DEFAULT_URL_VALUE = "jdbc:h2:mem:test;MODE=MySQL;";
  public static final String EVOLUTION_PLUGIN_KEY = "evolutionplugin";
  public static final String EVOLUTION_PLUGIN_VALUE = "enabled";
  public static final String APPLY_EVOLUTIONS_DEFAULT_KEY = "applyEvolutions.default";
  public static final String APPLY_EVOLUTIONS_DEFAULT_VALUE = "true";

  // Paths to the rest end-points
  public static final String REST_APP_RESULT_PATH = "/rest/job";
  public static final String REST_JOB_EXEC_RESULT_PATH = "/rest/jobexec";
  public static final String REST_FLOW_EXEC_RESULT_PATH = "/rest/flowexec";
  public static final String REST_SEARCH_PATH = "/rest/search";
  public static final String REST_COMPARE_PATH = "/rest/compare";
  public static final String REST_FLOW_GRAPH_DATA_PATH = "/rest/flowgraphdata";
  public static final String REST_JOB_GRAPH_DATA_PATH = "/rest/jobgraphdata";
  public static final String REST_JOB_METRICS_GRAPH_DATA_PATH = "/rest/jobmetricsgraphdata";
  public static final String REST_FLOW_METRICS_GRAPH_DATA_PATH = "/rest/flowmetricsgraphdata";
  public static final String REST_USER_RESOURCE_USAGE_PATH = "/rest/resourceusagedatabyuser";
  public static final String REST_GET_CURRENT_RUN_PARAMETERS = "/rest/getCurrentRunParameters";
  public static final String REST_LOGIN_ENDPOINT = "/rest/login";
  public static final String REST_USER_AUTHORIZATION_API_ENDPOINT = "/rest/userAuthorization";

  public static final String DEFAULT_ENCODING = "UTF-8";

  //Paths to the UI rest end points
  public static final String REST_WORKFLOW_SUMMARIES_PATH = "/rest/workflow-summaries";
  public static final String REST_JOB_SUMMARIES_PATH = "/rest/job-summaries";
  public static final String REST_APPLICATION_SUMMARIES_PATH = "/rest/application-summaries";
  public static final String REST_WORKFLOWS_PATH = "/rest/workflows";
  public static final String REST_JOBS_PATH = "/rest/jobs";
  public static final String REST_APPLICATIONS_PATH = "/rest/applications";
  public static final String REST_SEARCH_RESULTS = "/rest/search-results";

  // Sample mapreduce constants
  public static final String FILENAME_JOBCOUNTER = "mrdata/sampleJobCounter.properties";
  public static final String FILENAME_MAPPERTASK = "mrdata/mapperTaskCounter$.properties";
  public static final String FILENAME_REDUCERTASK = "mrdata/reducerTaskCounter$.properties";
  public static final String FILENAME_JOBCONF = "mrdata/sampleJobConf.properties";

  public static final String TEST_TRACKING_URL = "http://hostname/jobhistory/job/job_1460381439677_0001";
  public static final String TEST_JOBCONF_NAME = "mapred.child.java.opts";
  public static final String TEST_JOBCONF_PATTERN = ".*.";
  public static final String TEST_SCHEDULER = "azkaban";
  public static final long TEST_SCORE = 0;
  public static final String TEST_SEVERITY = "NONE";

  // Tuning constant
  public static final String JSON_CURRENT_POPULATION_KEY = "current_population";
  public static final String JSON_PREVIOUS_POPULATION_KEY = "prev_population";
  public static final String JSON_ARCHIVE_KEY = "archive";
  public static final String JSON_RND_STATE_KEY = "rnd_state";
  public static final int SWARM_SIZE = 3;
  public static final String JSON_PARTICLE_CANDIDATE_KEY = "_candidate";
  public static final String JSON_PARTICLE_BIRTHDATE_KEY = "birthdate";
  public static final String JSON_PARTICLE_MAXIMIZE_KEY = "maximize";
  public static final String JSON_PARTICLE_FITNESS_KEY = "fitness";

  //Mock Scheduler server constants
  public static final String MOCK_SCHEDULER_HOST = "localhost";
  public static final int MOCK_SCHEDULER_PORT = 9191;


  //Authuntication and Authorization Constants
  public static final String FAKE_SCHEDULER_URL = "http://localhost:9191";
  public static final String SCHEDULER_URL_KEY = "schedulerUrl";
  public static final String AZKABAN_LOGIN_URL_SUFFIX = "/?action=login";
  public static final String AZKABAN_USER_AUTHORIZATION_URL_SUFFIX = "/manager";
  public static final String LOGIN_CREDENTIAL_ERROR_MESSAGE = "Incorrect Login. Username/Password+VIP not found.";
  public static final String SUCCESS_KEY = "success";
  public static final String SESSION_ID = "session_id";
  public static final String JOB_DEFITION_ID = "jobDefId";
  public static final String TEST_PROJECT_NAME = "auth_test_project";
  public static final String TEST_SESSION_ID1 = "48fe6ae4-a3fc-4e6f-bc0a-6f6ac3165673";
  public static final String TEST_SESSION_ID2 = "8bf3bfe1-5a22-4b52-8ced-544ff4b1e9ae";
  public static final String TEST_JOB_DEF_ID2 =
      "https://localhost:9191/manager?project=auth_test_project&flow=test_flow&job=sample_job";
  public static final String INVALID_JOB_DEF_ID2 =
      "https://localhost:9191/manager?flow=test_flow&job=sample_job";


}
