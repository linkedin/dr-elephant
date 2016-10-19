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

package models;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.linkedin.drelephant.analysis.Severity;

import com.linkedin.drelephant.util.Utils;
import java.util.Date;
import play.db.ebean.Model;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;


@Entity
@Table(name = "yarn_app_result")
public class AppResult extends Model {

  private static final long serialVersionUID = 1L;

  public static final int ID_LIMIT = 50;
  public static final int USERNAME_LIMIT = 50;
  public static final int QUEUE_NAME_LIMIT = 50;
  public static final int APP_NAME_LIMIT = 100;
  public static final int JOB_NAME_LIMIT = 255;
  public static final int TRACKING_URL_LIMIT = 255;
  public static final int JOBTYPE_LIMIT = 20;
  public static final int SCHEDULER_LIMIT = 20;
  public static final int URL_LEN_LIMIT = 800;
  public static final int FLOW_EXEC_ID_LIMIT = 255;

  // Note that the Table column constants are actually the java variable names defined in this model.
  // This is because ebean operations require the model variable names to be passed as strings.
  public static class TABLE {
    public static final String TABLE_NAME = "yarn_app_result";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String USERNAME = "username";
      public static final String QUEUE_NAME = "queueName";
    public static final String START_TIME = "startTime";
    public static final String FINISH_TIME = "finishTime";
    public static final String TRACKING_URL = "trackingUrl";
    public static final String JOB_TYPE = "jobType";
    public static final String SEVERITY = "severity";
    public static final String SCORE = "score";
    public static final String WORKFLOW_DEPTH = "workflowDepth";
    public static final String SCHEDULER = "scheduler";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_EXEC_ID = "jobExecId";
    public static final String FLOW_EXEC_ID = "flowExecId";
    public static final String JOB_DEF_ID = "jobDefId";
    public static final String FLOW_DEF_ID = "flowDefId";
    public static final String JOB_EXEC_URL = "jobExecUrl";
    public static final String FLOW_EXEC_URL = "flowExecUrl";
    public static final String JOB_DEF_URL = "jobDefUrl";
    public static final String FLOW_DEF_URL = "flowDefUrl";
    public static final String APP_HEURISTIC_RESULTS = "yarnAppHeuristicResults";
    public static final String RESOURCE_USAGE = "resourceUsed";
    public static final String WASTED_RESOURCES = "resourceWasted";
    public static final String TOTAL_DELAY = "totalDelay";
  }

  public static String getSearchFields() {
    return Utils.commaSeparated(AppResult.TABLE.NAME, AppResult.TABLE.USERNAME, TABLE.QUEUE_NAME, AppResult.TABLE.JOB_TYPE,
        AppResult.TABLE.SEVERITY, AppResult.TABLE.FINISH_TIME);
  }

  @Id
  @Column(length = ID_LIMIT, unique = true, nullable = false)
  public String id;

  @Column(length = APP_NAME_LIMIT, nullable = false)
  public String name;

  @Column(length = USERNAME_LIMIT, nullable = false)
  public String username;

  @Column(length = QUEUE_NAME_LIMIT, nullable = false)
  public String queueName;

  @Column(nullable = false)
  public long startTime;

  @Column(nullable = false)
  public long finishTime;

  @Column(length = TRACKING_URL_LIMIT, nullable = false)
  public String trackingUrl;

  @Column(length = JOBTYPE_LIMIT, nullable = false)
  public String jobType;

  @Column(nullable = false)
  public Severity severity;

  @Column(nullable = false)
  public int score;

  @Column(nullable = false)
  public int workflowDepth;

  @Column(length = SCHEDULER_LIMIT, nullable = true)
  public String scheduler;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String jobName;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String jobExecId;

  @Column(length = FLOW_EXEC_ID_LIMIT, nullable = false)
  public String flowExecId;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String jobDefId;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String flowDefId;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String jobExecUrl;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String flowExecUrl;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String jobDefUrl;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String flowDefUrl;

  @Column(nullable = true)
  public long resourceUsed;

  @Column(nullable = true)
  public long resourceWasted;

  @Column(nullable = true)
  public long totalDelay;

  @JsonManagedReference
  @OneToMany(cascade = CascadeType.ALL, mappedBy = "yarnAppResult")
  public List<AppHeuristicResult> yarnAppHeuristicResults;

  public static Finder<String, AppResult> find = new Finder<String, AppResult>(String.class, AppResult.class);
}
