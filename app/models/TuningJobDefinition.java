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
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.avaje.ebean.annotation.UpdatedTimestamp;

import play.db.ebean.Model;


@Entity
@Table(name = "tuning_job_definition")
public class TuningJobDefinition extends Model {

  private static final long serialVersionUID = 1L;

  public static final int JOB_NAME_LIMIT = 1000;

  public static class TABLE {
    public static final String TABLE_NAME = "tuning_job_definition";
    public static final String client = "client";
    public static final String tuningAlgorithmId = "tuningAlgorithmId";
    public static final String tuningEnabled = "tuningEnabled";
    public static final String averageResourceUsage = "averageResourceUsage";
    public static final String averageExecutionTime = "averageExecutionTime";
    public static final String averageInputSizeInBytes = "averageInputSizeInBytes";
    public static final String allowedMaxResourceUsagePercent = "allowedMaxResourceUsagePercent";
    public static final String allowedMaxExecutionTimePercent = "allowedMaxExecutionTimePercent";
    public static final String job = "job";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_definition", joinColumns = {@JoinColumn(name = "job_definition_id", referencedColumnName = "id")})
  public JobDefinition job;

  @Column(length = JOB_NAME_LIMIT, nullable = false)
  public String client;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_algorithm", joinColumns = {@JoinColumn(name = "tuning_algorithm_id", referencedColumnName = "id")})
  public TuningAlgorithm tuningAlgorithm;

  @Column(nullable = false)
  public int tuningEnabled;

  @Column(nullable = true)
  public Double averageResourceUsage;

  @Column(nullable = true)
  public Double averageExecutionTime;

  @Column(nullable = true)
  public Long averageInputSizeInBytes;

  @Column(nullable = true)
  public Double allowedMaxResourceUsagePercent;

  @Column(nullable = true)
  public Double allowedMaxExecutionTimePercent;

  public Double getAverageInputSizeInGB() {
    if (averageInputSizeInBytes != null) {
      return averageInputSizeInBytes * 1.0 / (1024 * 1024 * 1024);
    } else {
      return null;
    }
  }

  public static Model.Finder<Integer, TuningJobDefinition> find =
      new Model.Finder<Integer, TuningJobDefinition>(Integer.class, TuningJobDefinition.class);

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;
}
