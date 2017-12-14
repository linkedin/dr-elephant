/*
 * Copyright 2017 Electronic Arts Inc.
 *
 * Licensed under the Apache License, Version 2.0
 *
 */
package com.linkedin.drelephant.tez.heuristics;


import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;

import java.util.Arrays;
import org.apache.log4j.Logger;


/**
 * This Heuristic analyses the skewness in the task input data
 */
public class MapperDataSkewHeuristic extends GenericDataSkewHeuristic {
    private static final Logger logger = Logger.getLogger(MapperDataSkewHeuristic.class);

    public MapperDataSkewHeuristic(HeuristicConfigurationData heuristicConfData) {
        super(Arrays.asList(
                TezCounterData.CounterName.HDFS_BYTES_READ,
                TezCounterData.CounterName.S3A_BYTES_READ,
                TezCounterData.CounterName.S3N_BYTES_READ
        ), heuristicConfData);
    }

    @Override
    protected TezTaskData[] getTasks(TezApplicationData data) {
        return data.getMapTaskData();
    }
}