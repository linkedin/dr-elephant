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

import com.linkedin.drelephant.util.Utils;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import com.avaje.ebean.Model;

@Entity
@Table(name = "backfill_info")
public class BackfillInfo extends Model {

  private static final long serialVersionUID = 1L;

  public static final int APPTYPE_LIMIT = 20;

  // Note that the Table column constants are actually the java variable names defined in this model.
  // This is because ebean operations require the model variable names to be passed as strings.
  public static class TABLE {
    public static final String TABLE_NAME = "backfill_info";
    public static final String APP_TYPE = "app_type";
    public static final String BACKFILL_TS = "backfill_ts";
  }

  public static String getSearchFields() {
    return Utils.commaSeparated(BackfillInfo.TABLE.APP_TYPE, TABLE.BACKFILL_TS);
  }

  @Id
  @Column(length = APPTYPE_LIMIT, unique = true, nullable = false)
  public String appType;

  @Column(nullable = false)
  public long backfillTs;

  public static Finder<String, BackfillInfo> find = new Finder<String, BackfillInfo>(String.class, BackfillInfo.class);
}
