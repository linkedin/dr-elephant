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

package com.linkedin.drelephant.util;

import com.linkedin.drelephant.analysis.Severity;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import models.AppResult;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import play.Play;


/**
 * This class contains all the utility methods.
 */
public final class Utils {
  private static final Logger logger = Logger.getLogger(Utils.class);

  private static final String TRUNCATE_SUFFIX = "...";

  private Utils() {
    // do nothing
  }

  /**
   * Given a mapreduce job's application id, get its corresponding job id
   *
   * @param appId The application id of the job
   * @return the corresponding job id
   */
  public static String getJobIdFromApplicationId(String appId) {
    return appId.replaceAll("application", "job");
  }

  /**
   * Load an XML document from a file path
   *
   * @param filePath The file path to load
   * @return The loaded Document object
   */
  public static Document loadXMLDoc(String filePath) {
    InputStream instream = null;
    logger.info("Loading configuration file " + filePath);
    instream = Play.application().resourceAsStream(filePath);

    if (instream == null) {
      logger.info("Configuation file not present in classpath. File:  " + filePath);
      throw new RuntimeException("Unable to read " + filePath);
    }
    logger.info("Configuation file loaded. File: " + filePath);

    Document document = null;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      document = builder.parse(instream);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("XML Parser could not be created.", e);
    } catch (SAXException e) {
      throw new RuntimeException(filePath + " is not properly formed", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read " + filePath, e);
    }

    return document;
  }

  /**
   * Parse a java option string in the format of "-Dfoo=bar -Dfoo2=bar ..." into a {optionName -> optionValue} map.
   *
   * @param str The option string to parse
   * @return A map of options
   */
  public static Map<String, String> parseJavaOptions(String str) {
    Map<String, String> options = new HashMap<String, String>();
    String[] tokens = str.trim().split("\\s");
    for (String token : tokens) {
      if (token.isEmpty()) {
        continue;
      }
      if (!token.startsWith("-D")) {
        throw new IllegalArgumentException(
            "Cannot parse java option string [" + str + "]. Some options does not begin with -D prefix.");
      }
      String[] parts = token.substring(2).split("=", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            "Cannot parse java option string [" + str + "]. The part [" + token + "] does not contain a =.");
      }

      options.put(parts[0], parts[1]);
    }
    return options;
  }

  /**
   * Returns the configured thresholds after evaluating and verifying the levels.
   *
   * @param rawLimits A comma separated string of threshold limits
   * @param thresholdLevels The number of threshold levels
   * @return The evaluated threshold limits
   */
  public static double[] getParam(String rawLimits, int thresholdLevels) {
    double[] parsedLimits = null;

    if (rawLimits != null && !rawLimits.isEmpty()) {
      String[] thresholds = rawLimits.split(",");
      if (thresholds.length != thresholdLevels) {
        logger.error("Could not find " + thresholdLevels + " threshold levels in "  + rawLimits);
        parsedLimits = null;
      } else {
        // Evaluate the limits
        parsedLimits = new double[thresholdLevels];
        ScriptEngineManager mgr = new ScriptEngineManager(null);
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        for (int i = 0; i < thresholdLevels; i++) {
          try {
            parsedLimits[i] = Double.parseDouble(engine.eval(thresholds[i]).toString());
          } catch (ScriptException e) {
            logger.error("Could not evaluate " + thresholds[i] + " in " + rawLimits);
            parsedLimits = null;
          }
        }
      }
    }

    return parsedLimits;
  }

  /**
   * Combine the parts into a comma separated String
   *
   * Example:
   * input: part1 = "foo" and part2 = "bar"
   * output = "foo,bar"
   *
   * @param parts The parts to combine
   * @return The comma separated string
   */
  public static String commaSeparated(String... parts) {
    StringBuilder sb = new StringBuilder();
    String comma = ",";
    if (parts.length != 0) {
      sb.append(parts[0]);
    }
    for (int i = 1; i < parts.length; i++) {
      if (parts[i] != null && !parts[i].isEmpty()) {
        sb.append(comma);
        sb.append(parts[i]);
      }
    }
    return sb.toString();
  }

  /**
   * Compute the score for the heuristic based on the number of tasks and severity.
   * This is applicable only to mapreduce applications.
   *
   * Score = severity * num of tasks (where severity NOT in [NONE, LOW])
   *
   * @param severity The heuristic severity
   * @param tasks The number of tasks (map/reduce)
   * @return
   */
  public static int getHeuristicScore(Severity severity, int tasks) {
    int score = 0;
    if (severity != Severity.NONE && severity != Severity.LOW) {
      score = severity.getValue() * tasks;
    }
    return score;
  }

  /**
   * Parse a comma separated string of key-value pairs into a {property -> value} Map.
   * e.g. string format: "foo1=bar1,foo2=bar2,foo3=bar3..."
   *
   * @param str The comma separated, key-value pair string to parse
   * @return A map of properties
   */
  public static Map<String, String> parseCsKeyValue(String str) {
    Map<String, String> properties = new HashMap<String, String>();
    String[] tokens = null;
    if (str != null) {
      tokens = str.trim().split(",");
    }
    for (String token : tokens) {
      if (!token.isEmpty()) {
        String[] parts = token.split("=", 2);
        if (parts.length == 2) {
          properties.put(parts[0], parts[1]);
        }
      }
    }
    return properties;
  }

  /**
   * Truncate the field by the specified limit
   *
   * @param field the field to br truncated
   * @param limit the truncation limit
   * @return The truncated field
   */
  public static String truncateField(String field, int limit, String appId) {
    if (field != null && limit > TRUNCATE_SUFFIX.length() && field.length() > limit) {
      logger.info("Truncating " + field + " to " + limit + " characters for " + appId);
      field = field.substring(0, limit - 3) + "...";
    }
    return field;
  }

  /**
   * Checks if the property is set
   *
   * @param property The property to tbe checked.
   * @return true if set, false otherwise
   */
  public static boolean isSet(String property) {
    return property != null && !property.isEmpty();
  }
}
