package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResultDetails;
import com.linkedin.drelephant.analysis.genericheuristics.GenericPercentileHeuristic;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;

import org.apache.commons.io.FileUtils;

import java.util.List;

public class HDFSWriteHeuristic extends GenericPercentileHeuristic<MapReduceApplicationData> {

  public HDFSWriteHeuristic(HeuristicConfigurationData heuristicConfData){
    super(heuristicConfData);
  }

  @Override
  protected long getValue(MapReduceApplicationData data, List<HeuristicResultDetails> details) throws RuntimeException {
    MapReduceCounterData counters = data.getCounters();
    if (counters == null) {
      throw new RuntimeException("Failed to get MapReduce counter data");
    }
    return counters.get(MapReduceCounterData.CounterName.HDFS_BYTES_WRITTEN);
  }

  @Override
  protected int getScore(MapReduceApplicationData data, long value) throws RuntimeException {
    return (int)(value / 1e9);
  }

  @Override
  protected String format(long value) {
    return FileUtils.byteCountToDisplaySize(value);
  }

  @Override
  protected String getPercentileHeuristicDetailName() {
    return "HDFS bytes written";
  }

  @Override
  protected boolean shouldApply(MapReduceApplicationData data) {
    return data.getSucceeded();
  }
}
