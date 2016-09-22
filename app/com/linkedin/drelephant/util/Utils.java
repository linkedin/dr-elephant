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

import com.google.common.collect.Sets;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import models.AppResult;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import play.Play;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.*;


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
        logger.error("Could not find " + thresholdLevels + " threshold levels in " + rawLimits);
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
   * Convert a millisecond duration to a string format
   *
   * @param millis A duration to convert to a string form
   * @return A string of the form "X:Y:Z Hours".
   */
  public static String getDurationBreakdown(long millis) {

    long hours = TimeUnit.MILLISECONDS.toHours(millis);
    millis -= TimeUnit.HOURS.toMillis(hours);
    long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
    millis -= TimeUnit.MINUTES.toMillis(minutes);
    long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

    return String.format("%d:%02d:%02d", hours, minutes, seconds);
  }

  /**
   * Convert a value in MBSeconds to GBHours
   * @param MBSeconds The value to convert
   * @return A string of form a.xyz GB Hours
   */
  public static String getDurationInGBHours(long MBSeconds) {

    if (MBSeconds == 0) {
      return "0 GB Hours";
    }
    double GBseconds = (double) MBSeconds / (double) FileUtils.ONE_KB;
    double GBHours = GBseconds / Statistics.HOUR;

    if ((long) (GBHours * 1000) == 0) {
      return "0 GB Hours";
    }

    DecimalFormat df = new DecimalFormat("0.000");
    String GBHoursString = df.format(GBHours);
    GBHoursString = GBHoursString + " GB Hours";
    return GBHoursString;
  }

  /**
   * Find percentage of numerator of denominator
   * @param numerator The numerator
   * @param denominator The denominator
   * @return The percentage string of the form `x.yz %`
   */
  public static String getPercentage(long numerator, long denominator) {

    if (denominator == 0) {
      return "NaN";
    }

    double percentage = ((double) numerator / (double) denominator) * 100;

    if ((long) (percentage) == 0) {
      return "0 %";
    }

    DecimalFormat df = new DecimalFormat("0.00");
    return df.format(percentage).concat(" %");
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

  /**
   * Returns the total resources used by the job list
   * @param resultList The job lsit
   * @return The total resources used by the job list
   */
  public static long getTotalResources(List<AppResult> resultList) {
    long totalResources = 0;
    for (AppResult result : resultList) {
      totalResources += result.resourceUsed;
    }
    return totalResources;
  }

  /**
   * Returns the total wasted resource of the job list
   * @param resultList The list of the jobs
   * @return the total wasted resources of the job list
   */
  public static long getTotalWastedResources(List<AppResult> resultList) {
    long totalWastedResources = 0;
    for (AppResult result : resultList) {
      totalWastedResources += result.resourceWasted;
    }
    return totalWastedResources;
  }

  /**
   * Returns the total runtime of the job list i.e. last finished job - first started job
   * @param mrJobsList The total runtime of the job list
   * @return The total runtime of the job list
   */
  public static long getTotalRuntime(List<AppResult> mrJobsList) {
    long lastFinished = 0;
    long firstStarted = Long.MAX_VALUE;

    for (AppResult result : mrJobsList) {
      if (result.finishTime > lastFinished) {
        lastFinished = result.finishTime;
      }
      if (result.startTime < firstStarted) {
        firstStarted = result.startTime;
      }
    }

    return lastFinished - firstStarted;
  }

  /**
   * Returns the total waittime of the job list. The total waittime is calculated by first finding
   * the longest trail of non overlapping jobs which includes the last finished job. Then we add the delay for
   * all the jobs in the trail and the difference in start and finish time between subsequent jobs of the
   * trail.
   * @param mrJobsList The job list
   * @return The total wait time of the joblist
   */
  public static long getTotalWaittime(List<AppResult> mrJobsList) {
    long totalWaittime = 0;

    if (mrJobsList.size() == 1) {
      return mrJobsList.get(0).totalDelay;
    }

    List<AppResult> finishedTimesSorted = new ArrayList<AppResult>(mrJobsList);

    // sort the jobs in reverse order of finished times.
    Collections.sort(finishedTimesSorted, new Comparator<AppResult>() {
      @Override
      public int compare(AppResult a, AppResult b) {
        return (int) (b.finishTime - a.finishTime);
      }
    });

    // add delay of the lastfinished job
    totalWaittime += finishedTimesSorted.get(0).totalDelay;

    for (int i = 1; i < finishedTimesSorted.size(); i++) {
      if (finishedTimesSorted.get(i).finishTime < finishedTimesSorted.get(i-1).startTime) {
        // add delay between the finishtime of current job and start time of just previous finished job
        totalWaittime += finishedTimesSorted.get(i-1).startTime - finishedTimesSorted.get(i).finishTime;
        // add delay in the current job
        totalWaittime += finishedTimesSorted.get(i).totalDelay;
      }
    }
    return totalWaittime;
  }

  public static Set<AnalyticJob> getIntersection(List<AnalyticJob> undefinedJobs, List<AnalyticJob> completedJobs) {
    if (undefinedJobs == null || completedJobs == null) {
      return Collections.<AnalyticJob>emptySet();
    }

    final Set<AnalyticJob> group1 = new TreeSet<AnalyticJob>(new Comparator<AnalyticJob>() {
      public int compare(final AnalyticJob o1, final AnalyticJob o2) {
        return o1.getAppId().compareToIgnoreCase(o2.getAppId());
      }
    });
    group1.addAll(undefinedJobs);

    final Set<AnalyticJob> group2 = new TreeSet<AnalyticJob>(new Comparator<AnalyticJob>() {
      public int compare(final AnalyticJob o1, final AnalyticJob o2) {
        return o1.getAppId().compareToIgnoreCase(o2.getAppId());
      }
    });
    group2.addAll(completedJobs);

    return Sets.intersection(group1, group2);
  }
}
