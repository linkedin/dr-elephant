/*
 * Copyright 2017 Electronic Arts Inc.
 *
 * Licensed under the Apache License, Version 2.0
 *
 */
package com.linkedin.drelephant.tez.heuristics;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;

import com.linkedin.drelephant.tez.data.TezApplicationData;

import com.linkedin.drelephant.tez.data.TezTaskData;

import org.apache.log4j.Logger;

/**
 * Analyzes mapper memory allocation and requirements
 */
public class MapperMemoryHeuristic extends GenericMemoryHeuristic {

    private static final Logger logger = Logger.getLogger(MapperMemoryHeuristic.class);

    public static final String MAPPER_MEMORY_CONF = "mapreduce.map.memory.mb";

    public MapperMemoryHeuristic(HeuristicConfigurationData __heuristicConfData){
        super(MAPPER_MEMORY_CONF, __heuristicConfData);
    }

    @Override
    protected TezTaskData[] getTasks(TezApplicationData data) {
        return data.getMapTaskData();
    }

}