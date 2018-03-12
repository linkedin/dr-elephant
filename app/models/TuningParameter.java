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
@Table(name = "tuning_parameter")
public class TuningParameter extends Model {

  private static final long serialVersionUID = 1L;

  public enum ParamValueType {
    INT, FLOAT, DOUBLE, BOOLEAN
  }

  public static class TABLE {
    public static final String TABLE_NAME = "tuning_parameter";
    public static final String id = "id";
    public static final String paramName = "paramName";
    public static final String tuningAlgorithmId = "tuningAlgorithmId";
    public static final String defaultValue = "defaultValue";
    public static final String minValue = "minValue";
    public static final String maxValue = "maxValue";
    public static final String stepSize = "stepSize";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
    public static final String tuningAlgorithm = "tuningAlgorithm";
    public static final String isDerived = "isDerived";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Integer id;

  @Column(nullable = false)
  public String paramName;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name = "tuning_algorithm", joinColumns = {@JoinColumn(name = "tuning_algorithm_id", referencedColumnName = "id")})
  public TuningAlgorithm tuningAlgorithm;

  @Column(nullable = false)
  public Double defaultValue;

  @Column(nullable = false)
  public Double minValue;

  @Column(nullable = false)
  public Double maxValue;

  @Column(nullable = false)
  public Double stepSize;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column(nullable = false)
  @UpdatedTimestamp
  public Timestamp updatedTs;

  @Column(nullable = false)
  public Integer isDerived;

  public static Finder<Integer, TuningParameter> find =
      new Finder<Integer, TuningParameter>(Integer.class, TuningParameter.class);
}
