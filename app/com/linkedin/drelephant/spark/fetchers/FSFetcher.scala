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

package com.linkedin.drelephant.spark.fetchers

import com.linkedin.drelephant.analysis.{AnalyticJob, ElephantFetcher}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.legacydata.LegacyDataConverters
import org.apache.spark.deploy.history.SparkFSFetcher

/**
 * Wraps the SparkFSFetcher which has the actual logic to comply to the new SparkApplicationData interface
 * @param fetcherConfigurationData
 */
class FSFetcher(fetcherConfigurationData: FetcherConfigurationData)
  extends ElephantFetcher[SparkApplicationData] {
  lazy val legacyFetcher = new SparkFSFetcher(fetcherConfigurationData)

  override def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    val legacyData = legacyFetcher.fetchData(analyticJob)
    LegacyDataConverters.convert(legacyData)
  }
}

object FSFetcher {
}
