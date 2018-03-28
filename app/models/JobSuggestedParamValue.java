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
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import play.db.ebean.Model;


@Entity
@Table(name = "job_suggested_param_value")
public class JobSuggestedParamValue extends Model {

  private static final long serialVersionUID = 1L;

  public static class TABLE {
    public static final String TABLE_NAME = "job_suggested_param_value";
    public static final String id = "id";
    public static final String jobExecution = "jobExecution";
    public static final String tuningParameterId = "tuningParameterId";
    public static final String paramValue = "paramValue";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Integer id;
  public Double paramValue;
  public Timestamp createdTs;

  @UpdatedTimestamp
  public Timestamp updatedTs;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_execution", joinColumns = {@JoinColumn(name = "job_execution_id", referencedColumnName = "id")})
  public JobExecution jobExecution;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_parameter", joinColumns = {@JoinColumn(name = "tuning_parameter_id", referencedColumnName = "id")})
  public TuningParameter tuningParameter;

  public static Finder<Long, JobSuggestedParamValue> find =
      new Finder<Long, JobSuggestedParamValue>(Long.class, JobSuggestedParamValue.class);
}
