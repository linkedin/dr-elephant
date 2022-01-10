package com.linkedin.drelephant.mapreduce.fetchers;


import java.util.List;
import java.util.ArrayList;
import com.linkedin.drelephant.util.Utils;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;

public class InOutFetcher {


	public static ArrayList<Long> inOutFetch (MapReduceApplicationData data)
 {
/*
    MapReduceTaskData[] mapTasks = data.getMapperData();
    MapReduceTaskData[] reduceTasks = data.getReducerData();

    List<Long> mapInputObjects = new ArrayList<Long>();
    List<Long> mapOutputObjects = new ArrayList<Long>();

    for (MapReduceTaskData task : mapTasks) {      // In-Out mapper
      if (task.isSampled()) {
        mapInputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT));    //nombre de HDFS-In de chaque tache
        mapOutputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT));    //nombre de Gbin-In de chaque tache
      }
    }

    List<Long> ReduceInputObjects = new ArrayList<Long>();
    List<Long> ReduceOutputObjects = new ArrayList<Long>();

    for (MapReduceTaskData task : reduceTasks) {    // In-Out reducer
      if (task.isSampled()) {
        ReduceInputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT));    //nombre de HDFS-In de chaque tache
        ReduceOutputObjects.add(task.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT));    //nombre de Gbin-In de chaque tache
      }
    }

    long in = Utils.sum(mapInputObjects) + Utils.sum(ReduceInputObjects);
    long out = Utils.sum(mapOutputObjects) + Utils.sum(ReduceOutputObjects);
*/

    long in = data.getCounters().get(MapReduceCounterData.CounterName.JOB_INPUT_OBJECT);
    long out = data.getCounters().get(MapReduceCounterData.CounterName.JOB_OUTPUT_OBJECT);

    ArrayList al = new ArrayList<Long>();
    al.add(in);
    al.add(out);
    return al;
    
	}
}
