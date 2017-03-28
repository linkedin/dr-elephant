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

package com.linkedin.drelephant.spark.legacydata;

import java.util.List;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageStatus;


/**
 * This class holds information related to Spark storage (RDDs specifically) information.
 */
public class SparkStorageData {
  private List<RDDInfo> _rddInfoList;
  private List<StorageStatus> _storageStatusList;

  public List<RDDInfo> getRddInfoList() {
    return _rddInfoList;
  }

  public void setRddInfoList(List<RDDInfo> rddInfoList) {
    _rddInfoList = rddInfoList;
  }

  public List<StorageStatus> getStorageStatusList() {
    return _storageStatusList;
  }

  public void setStorageStatusList(List<StorageStatus> storageStatusList) {
    _storageStatusList = storageStatusList;
  }
}
