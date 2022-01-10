/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.mapreduce.heuristics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.util.Utils;

/**
 * Analyses garbage collection efficiency
 */
public abstract class GenericCustomSagaHeuristic implements Heuristic<MapReduceApplicationData> {
	private static final org.apache.log4j.Logger logger = Logger.getLogger(GenericCustomSagaHeuristic.class);

	// Default value of parameters
	// private double[] gcRatioLimits = {0.01d, 0.02d, 0.03d, 0.04d}; // Garbage
	// Collection Time / CPU Time
	// private double[] runtimeLimits = {5, 10, 12, 15}; // Task Runtime in milli
	// sec

	private HeuristicConfigurationData _heuristicConfData;

	protected GenericCustomSagaHeuristic(HeuristicConfigurationData heuristicConfData) {
		logger.info("GenericCustomSagaHeuristic");
		this._heuristicConfData = heuristicConfData;
	}

	protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

	@Override
	public HeuristicConfigurationData getHeuristicConfData() {
		logger.info("getHeuristicConfData");
		return _heuristicConfData;
	}

	@Override
	public HeuristicResult apply(MapReduceApplicationData data) {

		if (!data.getSucceeded()) {
			return null;
		}

		MapReduceTaskData[] tasks = getTasks(data);
		List<Long> durations = new ArrayList<Long>();

		for (MapReduceTaskData task : tasks) {
			logger.info("getTaskId : " + task.getTaskId());
			
			if (task.isTimeAndCounterDataPresent()) {
				logger.info("duration : " + task.getCounters().get(MapReduceCounterData.CounterName.DURATION_IN_FACADE_TASK));
				if(task.getCounters().get(MapReduceCounterData.CounterName.DURATION_IN_FACADE_TASK) > 0) {
					durations.add(task.getCounters().get(MapReduceCounterData.CounterName.DURATION_IN_FACADE_TASK));	
				}
			}
		}

		int nbTaskwithFacadeWork = 0;

		long min;
		long max;
		// check list is empty or not
		if (durations.isEmpty()) {
			min = Integer.MIN_VALUE;
			max = Integer.MIN_VALUE;
		}

		// create a new list to avoid modification
		// in the original list
		List<Long> sortedlist = durations;

		// sort list in natural order
		Collections.sort(sortedlist);

		// last element in the sorted list would be maximum
		long averageFacadeDuratins = 0;
		if(sortedlist.isEmpty()) {
			max = 0;
			min = 0;
		} else {
			max = sortedlist.get(sortedlist.size() - 1);
			min = sortedlist.get(0);

			for (Long duration : durations) {
				if (duration > 0) {
					nbTaskwithFacadeWork++;
				}
			}

			averageFacadeDuratins = Statistics.average(durations);
		}

		Severity severity = Severity.NONE;

		logger.info("getHeuristicName " + _heuristicConfData.getHeuristicName());
		logger.info("getClassName " + _heuristicConfData.getClassName());

		HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
				_heuristicConfData.getHeuristicName(), severity,
				Utils.getHeuristicScore(severity, nbTaskwithFacadeWork));

		result.addResultDetail("Number of tasks with façade work", Integer.toString(nbTaskwithFacadeWork));
		result.addResultDetail("Avg façade runtime (s)", Long.toString(averageFacadeDuratins));
		result.addResultDetail("Min (s)", Long.toString(min));
		result.addResultDetail("Max (s)", Long.toString(max));
		return result;
	}
}
