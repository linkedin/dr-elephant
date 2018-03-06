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

import java.sql.Timestamp;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.avaje.ebean.annotation.UpdatedTimestamp;

import play.db.ebean.Model;


@Entity
@Table(name = "job_definition")
public class JobDefinition extends Model {

  private static final long serialVersionUID = 1L;
  public static final int USERNAME_LIMIT = 50;
  public static final int JOB_NAME_LIMIT = 1000;

  public static class TABLE {
    public static final String TABLE_NAME = "job_definition";
    public static final String id = "id";
    public static final String jobDefId = "jobDefId";
    public static final String scheduler = "scheduler";
    public static final String username = "username";
    public static final String jobName = "jobName";
    public static final String jobDefUrl = "jobDefUrl";
    public static final String flowDefinitionId = "flowDefinitionId";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Integer id;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String jobDefId;

  @Column(length = USERNAME_LIMIT, nullable = false)
  public String scheduler;

  @Column(length = USERNAME_LIMIT, nullable = false)
  public String username;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String jobName;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String jobDefUrl;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "flow_definition", joinColumns = {@JoinColumn(name = "flow_definition_id", referencedColumnName = "id")})
  public FlowDefinition flowDefinition;

  @Column(nullable = true)
  public Timestamp createdTs;

  @Column(nullable = true)
  @UpdatedTimestamp
  public Timestamp updatedTs;

  public static Finder<Integer, JobDefinition> find =
      new Finder<Integer, JobDefinition>(Integer.class, JobDefinition.class);
}
