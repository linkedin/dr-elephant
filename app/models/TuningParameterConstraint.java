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

/**
 * TuningParameterConstraint refers to the table tuning_parameter_constraint.
 * This table will contain boundry constraint for each parameter. This table will be specifically
 * use in IPSO
 */
@Entity
@Table(name = "tuning_parameter_constraint")
public class TuningParameterConstraint extends Model {

  private static final long serialVersionUID = 1L;

  public enum ConstraintType {
    BOUNDARY, INTERDEPENDENT
  }

  public static class TABLE {
    public static final String TABLE_NAME = "tuning_parameter_constraint";
    public static final String id = "id";
    public static final String jobDefinition = "jobDefinition";
    public static final String constraintType = "constraintType";
    public static final String tuningParameterId = "tuningParameterId";
    public static final String lowerBound = "lowerBound";
    public static final String upperBound = "upperBound";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Integer id;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "job_definition", joinColumns = {@JoinColumn(name = "job_definition_id", referencedColumnName = "id")})
  public JobDefinition jobDefinition;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  public ConstraintType constraintType;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_parameter", joinColumns = {@JoinColumn(name = "tuning_parameter_id", referencedColumnName = "id")})
  public TuningParameter tuningParameter;

  @Column(nullable = false)
  public Double lowerBound;

  @Column(nullable = false)
  public Double upperBound;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;


  public static Finder<Integer, TuningParameterConstraint> find =
      new Finder<Integer, TuningParameterConstraint>(Integer.class, TuningParameterConstraint.class);

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
