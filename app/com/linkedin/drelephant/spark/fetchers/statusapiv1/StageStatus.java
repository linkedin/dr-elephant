package com.linkedin.drelephant.spark.fetchers.statusapiv1;

import org.apache.spark.util.EnumUtil;

public enum StageStatus {
  ACTIVE,
  COMPLETE,
  FAILED,
  SKIPPED,
  PENDING;

  private StageStatus() {}

  public static com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus fromString(String str) {
    return (com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus) EnumUtil.parseIgnoreCase(com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus.class, str);
  }
}
