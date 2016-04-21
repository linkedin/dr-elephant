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
