/*
 * Copyright 2016 Linkin Corp.
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

package com.linkedin.drelephant.mapreduce.fetchers;

import com.linkedin.drelephant.analysis.ElephantFetcher;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;


public abstract class MapReduceFetcher implements ElephantFetcher<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(MapReduceFetcher.class);
  protected static final int MAX_SAMPLE_SIZE = 200;
  protected static final String SAMPLING_ENABLED_XML_FIELD = "sampling_enabled";

  protected FetcherConfigurationData _fetcherConfigurationData;
  private boolean _samplingEnabled;

  public MapReduceFetcher(FetcherConfigurationData fetcherConfData) {
    this._fetcherConfigurationData = fetcherConfData;
    this._samplingEnabled = Boolean.parseBoolean(
            fetcherConfData.getParamMap().get(SAMPLING_ENABLED_XML_FIELD));
  }

  protected int sampleAndGetSize(String jobId, List<?> taskList) {
    // check if sampling is enabled
    if (_samplingEnabled) {
      if (taskList.size() > MAX_SAMPLE_SIZE) {
        logger.info(jobId + " needs sampling.");
        Collections.shuffle(taskList);
      }
      return Math.min(taskList.size(), MAX_SAMPLE_SIZE);
    }
    return taskList.size();
  }

  public boolean isSamplingEnabled() {
    return _samplingEnabled;
  }
}
