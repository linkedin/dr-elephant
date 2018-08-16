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
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
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
@Table(name = "job_execution")
public class JobExecution extends Model {

  private static final long serialVersionUID = 1L;

  public enum ExecutionState {
    NOT_STARTED, IN_PROGRESS, SUCCEEDED, FAILED, CANCELLED
  }

  public static class TABLE {
    public static final String TABLE_NAME = "job_execution";
    public static final String id = "id";
    public static final String jobExecId = "jobExecId";
    public static final String executionState = "executionState";
    public static final String resourceUsage = "resourceUsage";
    public static final String executionTime = "executionTime";
    public static final String inputSizeInBytes = "inputSizeInBytes";
    public static final String jobExecUrl = "jobExecUrl";
    public static final String jobDefinition = "jobDefinition";
    public static final String flowExecution = "flowExecution";
    public static final String job = "job";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  public String jobExecId;

  public String jobExecUrl;

  @Enumerated(EnumType.STRING)
  public ExecutionState executionState;

  public Double resourceUsage;

  public Double executionTime;

  public Double inputSizeInBytes;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "flow_execution", joinColumns = {@JoinColumn(name = "flow_execution_id", referencedColumnName = "id")})
  public FlowExecution flowExecution;

  @Column(nullable = false)
  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_definition", joinColumns = {@JoinColumn(name = "job_definition_id", referencedColumnName = "id")})
  public JobDefinition job;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;

  @Override
  public void save() {
    this.updatedTs = new Timestamp(System.currentTimeMillis());
    super.save();
  }

  @Override
  public void update() {
    this.updatedTs = new Timestamp(System.currentTimeMillis());
    super.update();
  }

  public static Finder<Long, JobExecution> find = new Finder<Long, JobExecution>(Long.class, JobExecution.class);
}
