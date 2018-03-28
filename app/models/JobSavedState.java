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

import javax.persistence.Column;

import play.db.ebean.Model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.avaje.ebean.annotation.UpdatedTimestamp;


@Entity
@Table(name = "job_saved_state")
public class JobSavedState extends Model {

  private static final long serialVersionUID = 1L;

  public static class TABLE {
    public static final String TABLE_NAME = "job_saved_state";
    public static final String jobDefinitionId = "jobDefinitionId";
    public static final String savedState = "savedState";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  public Integer jobDefinitionId;

  @Column(nullable = false)
  public byte[] savedState;

  public Timestamp createdTs;

  @UpdatedTimestamp
  public Timestamp updatedTs;

  public boolean isValid() {
    return jobDefinitionId != null && savedState != null;
  }

  public static Finder<Integer, JobSavedState> find =
      new Finder<Integer, JobSavedState>(Integer.class, JobSavedState.class);
}
