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

package common;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.io.IOUtils;
import play.db.DB;

import static common.TestConstants.TEST_DATA_FILE;


public class DBTestUtil {

  public static void initDB()
      throws IOException, SQLException {
    String query = "";
    FileInputStream inputStream = new FileInputStream(TEST_DATA_FILE);

    try {
      query = IOUtils.toString(inputStream);
    } finally {
      inputStream.close();
    }

    Connection connection = DB.getConnection();

    try {
      Statement statement = connection.createStatement();
      statement.execute(query);
    } finally {
      connection.close();
    }
  }
}
