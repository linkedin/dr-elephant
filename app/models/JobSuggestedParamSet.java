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

import com.avaje.ebean.annotation.UpdatedTimestamp;
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
import javax.persistence.OneToOne;
import javax.persistence.Table;

import play.db.ebean.Model;


@Entity
@Table(name = "job_suggested_param_set")
public class JobSuggestedParamSet extends Model {

  private static final long serialVersionUID = -294471313051608818L;

  public enum ParamSetStatus {
    CREATED, SENT, EXECUTED, FITNESS_COMPUTED, DISCARDED
  }

  public static class TABLE {
    public static final String TABLE_NAME = "job_suggested_param_set";
    public static final String id = "id";
    public static final String jobDefinition = "jobDefinition";
    public static final String tuningAlgorithm = "tuningAlgorithm";
    public static final String paramSetState = "paramSetState";
    public static final String isParamSetDefault = "isParamSetDefault";
    public static final String fitness = "fitness";
    public static final String fitnessJobExecution = "fitnessJobExecution";
    public static final String isParamSetBest = "isParamSetBest";
    public static final String areConstraintsViolated = "areConstraintsViolated";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";

  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  @Column(nullable = false)
  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_definition", joinColumns = {@JoinColumn(name = "job_definition_id", referencedColumnName = "id")})
  public JobDefinition jobDefinition;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_execution", joinColumns = {@JoinColumn(name = "fitness_job_execution_id", referencedColumnName = "id")})
  public JobExecution fitnessJobExecution;

  @Column(nullable = false)
  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_algorithm", joinColumns = {@JoinColumn(name = "tuning_algorithm_id", referencedColumnName = "id")})
  public TuningAlgorithm tuningAlgorithm;


  @Enumerated(EnumType.STRING)
  public ParamSetStatus paramSetState;

  @Column(nullable = false)
  public Boolean isParamSetDefault;

  public Double fitness;

  @Column(nullable = false)
  public Boolean isParamSetBest;

  @Column(nullable = false)
  public Boolean areConstraintsViolated;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;

  public static Model.Finder<Long, JobSuggestedParamSet> find =
      new Model.Finder<Long, JobSuggestedParamSet>(Long.class, JobSuggestedParamSet.class);

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
}
