package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * Tests for the <code>DistributedCacheLimitHeuristic</code> class.
 */
public class DistributedCacheLimitHeuristicTest {
  private static Map<String, String> paramMap = new HashMap<String, String>();
  private static Properties jobConf = new Properties();
  private static final String cacheFileList =
      "/path/to/firstCacheFile,/path/to/secondCacheFile,/path/to/thirdCacheFile";
  private static final String archiveCacheFileList =
      "/path/to/firstArchiveCacheFile,/path/to/secondArchiveCacheFile,/path/to/thirdArchiveCacheFile";

  private static Heuristic<MapReduceApplicationData> _heuristic = new DistributedCacheLimitHeuristic(
      new HeuristicConfigurationData("test.heuristic", "test.class", "test.view", new ApplicationType("mapreduce"),
          paramMap));

  @Before
  public void setup() {
    paramMap.put("distributed.cache.file.size.limit", "500000000");
    jobConf.setProperty("mapreduce.job.cache.files", cacheFileList);
    jobConf.setProperty("mapreduce.job.cache.archives", archiveCacheFileList);
  }

  /**
   * All cache file sizes are within the limit.
   */
  @Test
  public void testHeuristicResult() {
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200,300");
    jobConf.setProperty("mapreduce.job.cache.archives.filesizes", "400,500,600");

    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.NONE);
  }

  /**
   * File size not found for all the files in cache.
   */
  @Test
  public void testHeuristicResultCacheFilesAndSizeLengthMismatch() {
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200");
    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.MODERATE);
  }

  /**
   * File size not found for all the files in archive cache.
   */
  @Test
  public void testHeuristicResultArchiveCacheFilesAndSizeLengthMismatch() {
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200,300");
    jobConf.setProperty("mapreduce.job.cache.archives.filesizes", "400,500");
    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.MODERATE);
  }

  /**
   * File size limit exceeded for file in cache.
   */
  @Test
  public void testHeuristicResultCacheFileLimitViolated() {
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200,600000000");
    jobConf.setProperty("mapreduce.job.cache.archives.filesizes", "400,500,600");

    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.CRITICAL);
  }

  /**
   * File size limit exceeded for file in archive cache.
   */
  @Test
  public void testHeuristicResultArchiveCacheFileLimitViolated() {
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200,300");
    jobConf.setProperty("mapreduce.job.cache.archives.filesizes", "400,500,600000000");

    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.CRITICAL);
  }

  /**
   * Either of the caches are not used by the application.
   */
  @Test
  public void testHeuristicResultNoDistributedCacheFiles() {
    jobConf.remove("mapreduce.job.cache.files");
    jobConf.remove("mapreduce.job.cache.archives");
    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result == null);
  }

  /**
   * Cache files are not used by the application.
   */
  @Test
  public void testHeuristicResultWithEmptyCacheFiles() {
    jobConf.remove("mapreduce.job.cache.files");
    jobConf.setProperty("mapreduce.job.cache.archives.filesizes", "400,500,600");
    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.NONE);
  }

  /**
   * Archive cache not used by the application.
   */
  @Test
  public void testHeuristicResultWithEmptyArchiveCacheFiles() {
    jobConf.remove("mapreduce.job.cache.archives");
    jobConf.setProperty("mapreduce.job.cache.files.filesizes", "100,200,300");
    MapReduceApplicationData data = new MapReduceApplicationData().setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    assertTrue("Failed to match on expected severity", result.getSeverity() == Severity.NONE);
  }
}
