/*
 * Copyright 2019 LinkedIn Corp.
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
package com.linkedin.drelephant.tony.fetchers;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.tony.data.TonyApplicationData;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.util.ParserUtils;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.time.ZoneId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class TonyFetcher implements ElephantFetcher<TonyApplicationData> {
  private static final Logger _LOGGER = Logger.getLogger(TonyFetcher.class);
  private final Path _intermediateDir;
  private final Path _finishedDir;
  private String _finishedDirTimezone;
  private final FileSystem _fs;
  private ZoneId _zoneId;

  /**
   * Constructor for {@link TonyFetcher}.
   * @param fetcherConfig  the fetcher configuration
   * @throws IOException
   */
  public TonyFetcher(FetcherConfigurationData fetcherConfig) throws IOException {
    Configuration conf = new Configuration();

    String tonyConfDir = System.getenv(Constants.TONY_CONF_DIR);
    if (fetcherConfig.getParamMap().containsKey(Constants.TONY_CONF_DIR)) {
      tonyConfDir = fetcherConfig.getParamMap().get(Constants.TONY_CONF_DIR);
    }
    _LOGGER.info("Using TonY conf dir: " + tonyConfDir);

    conf.addResource(new Path(tonyConfDir + Path.SEPARATOR + Constants.TONY_SITE_CONF));
    _intermediateDir = new Path(conf.get(TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE));
    _finishedDir = new Path(conf.get(TonyConfigurationKeys.TONY_HISTORY_FINISHED));
    _fs = _finishedDir.getFileSystem(conf);

    _finishedDirTimezone = conf.get(TonyConfigurationKeys.TONY_HISTORY_FINISHED_DIR_TIMEZONE);
    if (_finishedDirTimezone == null) {
      _finishedDirTimezone = TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED_DIR_TIMEZONE;
    }
    _zoneId = ZoneId.of(_finishedDirTimezone);
    _LOGGER.info("Using ZoneID: " + _zoneId.getId());
  }

  @Override
  public TonyApplicationData fetchData(AnalyticJob job) throws Exception {
    _LOGGER.debug("Fetching data for job " + job.getAppId());
    long finishTime = job.getFinishTime();
    Date date = new Date(finishTime);

    // TODO: We are deriving yyyy/MM/dd from the RM's application finish time, but the TonY Portal actually creates the
    // yyyy/MM/dd directory based off the end time embedded in the jhist file name. This end time is taken before the
    // application finishes and thus is slightly before the RM's application finish time. So it's possible that the
    // yyyy/MM/dd derived from the RM's application finish time is a day later and we may not find the history files.
    // In case we don't find the history files in yyyy/MM/dd, we should check the previous day as well.
    String yearMonthDay = ParserUtils.getYearMonthDayDirectory(date, _zoneId);
    Path jobDir = new Path(_finishedDir, yearMonthDay + Path.SEPARATOR + job.getAppId());
    if (!_fs.exists(jobDir)) {
      // check intermediate dir
      jobDir = new Path(_intermediateDir, job.getAppId());
    }

    _LOGGER.debug("Job directory for " + job.getAppId() + ": " + jobDir);

    // parse config
    Path confFile = new Path(jobDir, Constants.TONY_FINAL_XML);
    if (!_fs.exists(confFile)) {
      // for backward compatibility, see https://github.com/linkedin/TonY/issues/271
      confFile = new Path(jobDir, "config.xml");
    }
    Configuration conf = new Configuration(false);
    if (_fs.exists(confFile)) {
      conf.addResource(_fs.open(confFile));
    }

    // Parse events. For a list of event types, see
    // https://github.com/linkedin/TonY/blob/master/tony-core/src/main/avro/EventType.avsc.
    // We get the task start time from the TASK_STARTED event and the finish time and metrics from the TASK_FINISHED
    // event.
    List<Event> events = ParserUtils.parseEvents(_fs, jobDir);

    return new TonyApplicationData(job.getAppId(), job.getAppType(), conf, events);
  }
}
