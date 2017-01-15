package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Rule flags jobs which put files more than 500MB in the distributed cache.
 */
public class DistributedCacheLimitHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(DistributedCacheLimitHeuristic.class);
  private static final String DISTRIBUTED_CACHE_FILE_SIZE_LIMIT_CONF = "distributed.cache.file.size.limit";
  private static final String MAPREDUCE_JOB_CACHE_FILES_FILESIZES = "mapreduce.job.cache.files.filesizes";
  private static final String MAPREDUCE_JOB_CACHE_ARCHIVES_FILESIZES = "mapreduce.job.cache.archives.filesizes";
  private static final String MAPREDUCE_JOB_CACHE_FILES = "mapreduce.job.cache.files";
  private static final String MAPREDUCE_JOB_CACHE_ARCHIVES = "mapreduce.job.cache.archives";
  private static long distributedCacheFileSizeLimit = 500 * FileUtils.ONE_MB; // 500MB default
  private HeuristicConfigurationData _heuristicConfData;

  public DistributedCacheLimitHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    String cacheLimit = paramMap.get(DISTRIBUTED_CACHE_FILE_SIZE_LIMIT_CONF);
    if (cacheLimit != null) {
      try {
        distributedCacheFileSizeLimit = Long.parseLong(cacheLimit);
        logger.info(
            heuristicName + " will use " + DISTRIBUTED_CACHE_FILE_SIZE_LIMIT_CONF + " with the following setting: "
                + distributedCacheFileSizeLimit);
      } catch (NumberFormatException e) {
        logger
            .warn("Error parsing " + DISTRIBUTED_CACHE_FILE_SIZE_LIMIT_CONF + " from the conf file. Check for typos...",
                e);
      }
    }
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    if (data == null || !data.getSucceeded()) {
      return null;
    }

    Properties jobConf = data.getConf();
    String cacheFiles = jobConf.getProperty(MAPREDUCE_JOB_CACHE_FILES, null);
    String cacheFileSizes = jobConf.getProperty(MAPREDUCE_JOB_CACHE_FILES_FILESIZES, null);

    HeuristicResult result = null;

    if (cacheFiles != null && cacheFileSizes != null) {
      result =
          new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), Severity.NONE,
              0);
      List<String> cacheFilesList = new ArrayList<String>(Arrays.asList(cacheFiles.split(",")));
      List<String> cacheFileSizesList = new ArrayList<String>(Arrays.asList(cacheFileSizes.split(",")));

      int cacheFilesCount = cacheFilesList.size();
      int cacheFileSizesCount = cacheFileSizesList.size();

      if (cacheFilesCount != cacheFileSizesCount) {
        result.setSeverity(Severity.MODERATE);
        logger.warn("Mismatch in the number of files and their corresponding sizes for " + MAPREDUCE_JOB_CACHE_FILES);
        result.addResultDetail(MAPREDUCE_JOB_CACHE_FILES, Integer.toString(cacheFilesCount));
        result.addResultDetail(MAPREDUCE_JOB_CACHE_FILES_FILESIZES, Integer.toString(cacheFileSizesCount));
        return result;
      }

      Map<String, String> cacheFileToSizeMap = new HashMap<String, String>();
      for (int i = 0; i < cacheFilesCount; i++) {
        cacheFileToSizeMap.put(cacheFilesList.get(i), cacheFileSizesList.get(i));
      }

      if (checkFileSizeLimit(result, cacheFileToSizeMap)) {
        result.setSeverity(Severity.CRITICAL);
      }
    }

    String archiveCacheFiles = jobConf.getProperty(MAPREDUCE_JOB_CACHE_ARCHIVES, null);
    String archiveCacheFileSizes = jobConf.getProperty(MAPREDUCE_JOB_CACHE_ARCHIVES_FILESIZES, null);

    if (archiveCacheFiles != null && archiveCacheFileSizes != null) {

      if (result == null) {
        result =
            new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), Severity.NONE,
                0);
      }

      List<String> archiveCacheFilesList = new ArrayList<String>(Arrays.asList(archiveCacheFiles.split(",")));
      List<String> archiveCacheFileSizesList = new ArrayList<String>(Arrays.asList(archiveCacheFileSizes.split(",")));

      int archiveCacheFilesCount = archiveCacheFilesList.size();
      int archiveCacheFileSizesCount = archiveCacheFileSizesList.size();

      if (archiveCacheFilesCount != archiveCacheFileSizesCount) {
        result.setSeverity(Severity.MODERATE);
        logger
            .warn("Mismatch in the number of files and their corresponding sizes for " + MAPREDUCE_JOB_CACHE_ARCHIVES);
        result.addResultDetail(MAPREDUCE_JOB_CACHE_ARCHIVES, Integer.toString(archiveCacheFilesCount));
        result.addResultDetail(MAPREDUCE_JOB_CACHE_ARCHIVES_FILESIZES, Integer.toString(archiveCacheFileSizesCount));
        return result;
      }

      Map<String, String> archiveCacheFileToSizeMap = new HashMap<String, String>();
      for (int i = 0; i < archiveCacheFilesCount; i++) {
        archiveCacheFileToSizeMap.put(archiveCacheFilesList.get(i), archiveCacheFileSizesList.get(i));
      }

      if (checkFileSizeLimit(result, archiveCacheFileToSizeMap)) {
        result.setSeverity(Severity.CRITICAL);
      }
    }

    return result;
  }

  private boolean checkFileSizeLimit(HeuristicResult result, Map<String, String> cacheFileToSizeMap) {
    boolean limitViolated = false;
    for (String file : cacheFileToSizeMap.keySet()) {
      long size = 0;
      try {
        size = Long.parseLong(cacheFileToSizeMap.get(file));
      } catch (NumberFormatException e) {
        logger.warn("Unable to parse file size value: " + size + " for file: " + file);
      }

      if (size > distributedCacheFileSizeLimit) {
        limitViolated = true;
        result.addResultDetail(file, Long.toString(size));
      }
    }
    return limitViolated;
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }
}
