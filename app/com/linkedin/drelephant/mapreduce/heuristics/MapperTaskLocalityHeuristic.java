package com.linkedin.drelephant.mapreduce.heuristics;


import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.util.Utils;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;

public class MapperTaskLocalityHeuristic implements Heuristic<MapReduceApplicationData> {

    private static final Logger logger = Logger.getLogger(MapperTaskLocalityHeuristic.class);

    // Severity parameters.
    private static final String NODE_LOCALITY_RATIO = "node_locality_severity";
    private static final String RACK_LOCALITY_RATIO = "rack_locality_severity";
    private static final String COUNTER_GROUP_NAME = "org.apache.hadoop.mapreduce.JobCounter";
    private static final String DATA_LOCAL_MAPS = "DATA_LOCAL_MAPS";
    private static final String RACK_LOCAL_MAPS = "RACK_LOCAL_MAPS";
    private static final String OTHER_LOCAL_MAPS = "OTHER_LOCAL_MAPS";
    private static final String TOTAL_LAUNCHED_MAPS = "TOTAL_LAUNCHED_MAPS";

    private double[] nodeLocalityRatioLimits = {0.5d, 0.4d, 0.3d, 0.2d};
    private double[] rackLocalityRatioLimits = {0.7d, 0.6d, 0.5d, 0.4d};

    private HeuristicConfigurationData _heuristicConfData;

    private void loadParameters() {
        Map<String, String> paramMap = _heuristicConfData.getParamMap();
        String heuristicName = _heuristicConfData.getHeuristicName();

        double[] nodeLocalityThreshold = Utils.getParam(paramMap.get(NODE_LOCALITY_RATIO), nodeLocalityRatioLimits.length);
        if (nodeLocalityThreshold != null) {
            nodeLocalityRatioLimits = nodeLocalityThreshold;
        }
        logger.info(heuristicName + " will use " + NODE_LOCALITY_RATIO + " with the following threshold settings: "
                + Arrays.toString(nodeLocalityRatioLimits));

        double[] rackLocalityThreshold = Utils.getParam(paramMap.get(RACK_LOCALITY_RATIO), rackLocalityRatioLimits.length);
        if (nodeLocalityThreshold != null) {
            rackLocalityRatioLimits = rackLocalityThreshold;
        }
        logger.info(heuristicName + " will use " + RACK_LOCALITY_RATIO + " with the following threshold settings: "
                + Arrays.toString(nodeLocalityRatioLimits));

    }

    public MapperTaskLocalityHeuristic(HeuristicConfigurationData heuristicConfData) {
        this._heuristicConfData = heuristicConfData;
        loadParameters();
    }

    @Override
    public HeuristicConfigurationData getHeuristicConfData() {
        return _heuristicConfData;
    }

    @Override
    public HeuristicResult apply(MapReduceApplicationData data) {

        if(!data.getSucceeded()) {
            return null;
        }

        MapReduceCounterData currentJobData = data.getCounters();

        Map<String,Long> totalCounters = currentJobData.getAllCountersInGroup(COUNTER_GROUP_NAME);

        long dataLocalMaps = totalCounters.containsKey(DATA_LOCAL_MAPS) ? totalCounters.get(DATA_LOCAL_MAPS) : 0;
        long rackLocalMaps = totalCounters.containsKey(RACK_LOCAL_MAPS) ? totalCounters.get(RACK_LOCAL_MAPS) : 0;
        long otherLocalMaps = totalCounters.containsKey(OTHER_LOCAL_MAPS) ? totalCounters.get(OTHER_LOCAL_MAPS) : 0;
        long totalLaunchedMaps = totalCounters.get(TOTAL_LAUNCHED_MAPS);

        long nodeLocalityRatio = (dataLocalMaps + otherLocalMaps) / totalLaunchedMaps;
        long rackLocalityRatio = rackLocalMaps/totalLaunchedMaps;


        Severity severity = getTaskLocalitySeverity(nodeLocalityRatio);

        severity = Severity.min(severity, getTaskLocalitySeverity(rackLocalityRatio));

        HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
                _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, data.getMapperData().length));

        result.addResultDetail("Data Local Map Tasks", Long.toString(dataLocalMaps));
        result.addResultDetail("Other Local Map Tasks", Long.toString(otherLocalMaps));
        result.addResultDetail("Rack Local Map Tasks", Long.toString(rackLocalMaps));
        result.addResultDetail("Total Launched Map Tasks", Long.toString(totalLaunchedMaps));

        return result;
    }

    private Severity getTaskLocalitySeverity(double ratio) {
        return Severity.getSeverityDescending(
                ratio, nodeLocalityRatioLimits[0], nodeLocalityRatioLimits[1], nodeLocalityRatioLimits[2], nodeLocalityRatioLimits[3]);
    }




}
