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
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Analyses garbage collection efficiency
 */
public class MapperGCHeuristic extends GenericGCHeuristic{
    private static final Logger logger = Logger.getLogger(MapperGCHeuristic.class);

    public MapperGCHeuristic(HeuristicConfigurationData heuristicConfData) {
        super(heuristicConfData);
    }

    @Override
    protected TezTaskData[] getTasks(TezApplicationData data) {
        return data.getMapTaskData();
    }

}