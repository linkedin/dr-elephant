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

import com.linkedin.drelephant.analysis.Severity;
import java.sql.Timestamp;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GenerationType;
import javax.persistence.Table;
import javax.persistence.Id;
import com.avaje.ebean.annotation.UpdatedTimestamp;
import javax.persistence.GeneratedValue;
import play.db.ebean.Model;


/**
 * Table describe about the job which is for tuning. It have one entry per job execution.
 */
@Entity
@Table(name = "tuning_job_execution_code_recommendation")
public class TuningJobExecutionCodeRecommendation extends Model {

  private static final long serialVersionUID = 1L;


  public static class TABLE {
    public static final String TABLE_NAME = "tuning_job_execution_code_recommendation";
    public static final String id = "id";
    public static final String jobDefId = "jobDefId";
    public static final String jobExecUrl = "jobExecUrl";
    public static final String codeLocation = "codeLocation";
    public static final String recommendation = "recommendation";
    public static final String severity = "severity";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  @Column(nullable = false)
  public String jobDefId;

  @Column(nullable = false)
  public String jobExecUrl;

  @Column(nullable = false)
  public String codeLocation;

  @Column(nullable = false)
  public String recommendation;

  @Column(nullable = false)
  public String severity;

  public static Finder<Integer, TuningJobExecutionCodeRecommendation> find =
      new Finder<Integer, TuningJobExecutionCodeRecommendation>(Integer.class, TuningJobExecutionCodeRecommendation.class);

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
}
