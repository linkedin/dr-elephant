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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import play.db.ebean.Model;


@Entity
@Table(name = "flow_definition")
public class FlowDefinition extends Model {

  private static final long serialVersionUID = -6227998928253066437L;

  public static class TABLE {
    public static final String TABLE_NAME = "flow_definition";
    public static final String id = "id";
    public static final String flowDefId = "flowDefId";
    public static final String flowDefUrl = "flowDefUrl";
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Integer id;

  @Column(nullable = false)
  public String flowDefId;

  @Column(nullable = false)
  public String flowDefUrl;

  public static Model.Finder<Integer, FlowDefinition> find =
      new Model.Finder<Integer, FlowDefinition>(Integer.class, FlowDefinition.class);
}


