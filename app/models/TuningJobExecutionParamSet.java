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
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import play.db.ebean.Model;

@Entity
@Table(name = "tuning_job_execution_param_set")
public class TuningJobExecutionParamSet extends Model {

  private static final long serialVersionUID = 1L;
  public static class TABLE {
    public static final String TABLE_NAME = "tuning_job_execution_param_set";
    public static final String jobSuggestedParamSet = "jobSuggestedParamSet";
    public static final String jobExecution = "jobExecution";
    public static final String tuningEnabled = "tuningEnabled";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_suggested_param_set", joinColumns = {@JoinColumn(name = "job_suggested_param_set_id", referencedColumnName = "id")})
  public JobSuggestedParamSet jobSuggestedParamSet;

  @OneToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_execution", joinColumns = {@JoinColumn(name = "job_execution_id", referencedColumnName = "id")})
  public JobExecution jobExecution;

  public Boolean tuningEnabled;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;

  public static Finder<Long, TuningJobExecutionParamSet> find =
      new Finder<Long, TuningJobExecutionParamSet>(Long.class, TuningJobExecutionParamSet.class);

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
