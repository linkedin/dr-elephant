package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.HeuristicResultDetails;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import static com.linkedin.drelephant.mapreduce.heuristics.CommonConstantsHeuristic.ParameterKeys.*;


public class ConfigurationHeuristicTest extends TestCase {
  private static final Logger logger = Logger.getLogger(ConfigurationHeuristicTest.class);
  private static int NUMTASKS = 1;
  private static Map<String, String> paramMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new ConfigurationHeuristic(
      new HeuristicConfigurationData("test_heuristic", "test_class", "test_view", new ApplicationType("test_apptype"),
          paramMap));

  @Test
  public void testConfigurations() throws IOException {
    List<HeuristicResultDetails> results = analyzeJob().getHeuristicResultDetails();
    assertTrue(
        results.get(0).getName().equals(CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HEURISTICS_CONF.getValue())
            && results.get(0).getValue().equals("2048"));
    assertTrue(
        results.get(1).getName().equals(CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HEURISTICS_CONF.getValue())
            && results.get(1).getValue().equals("1536"));
    assertTrue(results.get(2)
        .getName()
        .equals(CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HEURISTICS_CONF.getValue()) && results.get(2)
        .getValue()
        .equals("3072"));
    assertTrue(
        results.get(3).getName().equals(CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HEURISTICS_CONF.getValue())
            && results.get(3).getValue().equals("2536"));
    assertTrue(
        results.get(4).getName().equals(CommonConstantsHeuristic.ParameterKeys.SORT_BUFFER_HEURISTICS_CONF.getValue())
            && results.get(4).getValue().equals("458"));
    assertTrue(
        results.get(5).getName().equals(CommonConstantsHeuristic.ParameterKeys.SORT_FACTOR_HEURISTICS_CONF.getValue())
            && results.get(5).getValue().equals("30"));
    assertTrue(
        results.get(6).getName().equals(CommonConstantsHeuristic.ParameterKeys.SORT_SPILL_HEURISTICS_CONF.getValue())
            && results.get(6).getValue().equals("0.35"));
    assertTrue(
        results.get(7).getName().equals(CommonConstantsHeuristic.ParameterKeys.SPLIT_SIZE_HEURISTICS_CONF.getValue())
            && results.get(7).getValue().equals("256"));
  }

  private HeuristicResult analyzeJob() throws IOException {
    MapReduceTaskData[] mappers = new MapReduceTaskData[NUMTASKS + 1];
    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new MapReduceTaskData("task-id-" + i, "task-attempt-id-" + i);
    }
    mappers[i] = new MapReduceTaskData("task-id-" + i, "task-attempt-id-" + i);
    MapReduceApplicationData data = new MapReduceApplicationData().setMapperData(mappers).setReducerData(mappers);
    Properties p = new Properties();
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.MAPPER_MEMORY_HADOOP_CONF.getValue(), Long.toString(2048));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.MAPPER_HEAP_HADOOP_CONF.getValue(), Long.toString(1536));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.SORT_BUFFER_HADOOP_CONF.getValue(), Long.toString(458));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.SORT_FACTOR_HADOOP_CONF.getValue(), Long.toString(30));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.SORT_SPILL_HADOOP_CONF.getValue(), Double.toString(0.35));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.REDUCER_MEMORY_HADOOP_CONF.getValue(), Long.toString(3072));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.REDUCER_HEAP_HADOOP_CONF.getValue(), Long.toString(2536));
    p.setProperty(CommonConstantsHeuristic.ParameterKeys.SPLIT_SIZE_HADOOP_CONF.getValue(), Long.toString(256));
    data.setJobConf(p);
    HeuristicResult results = _heuristic.apply(data);
    return results;
  }
}