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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import play.db.ebean.Model;


@Entity
@Table(name = "tuning_job_execution")
public class TuningJobExecution extends Model {

  private static final long serialVersionUID = -294471313051608818L;

  public enum ParamSetStatus {
    CREATED, SENT, EXECUTED, FITNESS_COMPUTED, DISCARDED
  }

  public static class TABLE {
    public static final String TABLE_NAME = "tuning_job_execution";
    public static final String jobExecutionId = "jobExecutionId";
    public static final String tuningAlgorithmId = "tuningAlgorithmId";
    public static final String paramSetState = "paramSetState";
    public static final String isDefaultExecution = "isDefaultExecution";
    public static final String fitness = "fitness";
    public static final String jobExecution = "jobExecution";
  }

  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_execution", joinColumns = {@JoinColumn(name = "job_execution_id", referencedColumnName = "id")})
  public JobExecution jobExecution;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_algorithm", joinColumns = {@JoinColumn(name = "tuning_algorithm_id", referencedColumnName = "id")})
  public TuningAlgorithm tuningAlgorithm;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  public ParamSetStatus paramSetState;

  public Boolean isDefaultExecution;

  public Double fitness;

  public static Model.Finder<Long, TuningJobExecution> find =
      new Model.Finder<Long, TuningJobExecution>(Long.class, TuningJobExecution.class);
}
