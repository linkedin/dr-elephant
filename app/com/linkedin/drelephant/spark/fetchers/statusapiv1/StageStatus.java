/*
*Added this class to accommodate the status "PENDING" for stages.
*
*TODO: remove this class if using the spark version having "PENDING" StageStatus.
 */

package com.linkedin.drelephant.spark.fetchers.statusapiv1;

import org.apache.spark.util.EnumUtil;

public enum StageStatus {
  ACTIVE,
  COMPLETE,
  FAILED,
  SKIPPED,
  PENDING;

  private StageStatus() {
  }

  public static StageStatus fromString(String str) {
    return (StageStatus) EnumUtil.parseIgnoreCase(StageStatus.class, str);
  }
}