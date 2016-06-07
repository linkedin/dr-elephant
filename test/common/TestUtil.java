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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

  // private on purpose
  private TestUtil() {}

  public static Properties loadProperties(String filePath)
      throws IOException {
    Properties properties = new Properties();
    InputStream inputStream = TestUtil.class.getClassLoader().getResourceAsStream(filePath);
    if (inputStream == null) {
      logger.info("Configuation file not present in classpath. File:  " + filePath);
      throw new RuntimeException("Unable to read " + filePath);
    }
    properties.load(inputStream);
    logger.info("Configuation file loaded. File: " + filePath);
    return properties;
  }

}
