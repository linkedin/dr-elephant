package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import org.apache.log4j.Logger;

public class MapperCustomSagaHeuristic extends GenericCustomSagaHeuristic {
    private static final Logger logger = Logger.getLogger(GenericCustomSagaHeuristic.class);

    public MapperCustomSagaHeuristic(HeuristicConfigurationData heuristicConfData) {
        super(heuristicConfData);
        logger.info("MapperCustomSagaHeuristic");
    }

    @Override
    protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
        logger.info("getTasks");
        return data.getMapperData();
    }
}
