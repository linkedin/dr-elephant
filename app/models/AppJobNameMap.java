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

import play.db.ebean.Model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;


@Entity
@Table(name = "yarn_app_job_name_map")
public class AppJobNameMap extends Model {

  private static final long serialVersionUID = 1L;

  public static final int FLOW_EXEC_ID_LIMIT = 255;
  public static final int JOB_NAME_LIMIT = 255;
  public static final int JOB_IN_NODES_LIMIT = 255;

  // Note that the Table column constants are actually the java variable names defined in this model.
  // This is because ebean operations require the model variable names to be passed as strings.
  public static class TABLE {
    public static final String TABLE_NAME = "yarn_app_job_name_map";
    public static final String FLOW_EXEC_ID = "flowExecId";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_NAME_ID = "jobNameId";
    public static final String JOB_IN_NODES = "jobInnodes";
  }

  @Column(length = FLOW_EXEC_ID_LIMIT, nullable = false)
  public String flowExecId;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String jobName;

  @Column(nullable = false)
  public int jobNameId;

  @Column (length = JOB_IN_NODES_LIMIT, nullable = true)
  public String jobInnodes;

  public static Finder<String, AppJobNameMap> find = new Finder<String, AppJobNameMap>(String.class, AppJobNameMap.class);
}
