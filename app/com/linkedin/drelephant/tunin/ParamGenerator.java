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

package com.linkedin.drelephant.tunin;

import com.avaje.ebean.Expr;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.drelephant.analysis.AnalyticJob;
import models.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;
import play.libs.Json;
import java.util.ArrayList;
import java.util.List;


public abstract class ParamGenerator {

    private final Logger logger = Logger.getLogger(getClass());

    private static  final String PARAM_SET_STATE_FIELD_NAME = "paramSetState";
    private static final String JSON_CURRENT_POPULATION_KEY = "current_population";
    private static final String ALGO_FIELD_NAME = "algo";



    public  abstract TunerState generateParamSet(TunerState jobTunerState);

   // Done
    public List<Particle> jsonToParticleList(JsonNode jsonParticleList){

        List<Particle> particleList = new ArrayList<Particle>();


        for (JsonNode jsonParticle: jsonParticleList ){

            Particle particle = Json.fromJson(jsonParticle, Particle.class);
            if(particle!=null){
                particleList.add(particle);
            }
        }
        return particleList;
    }

    //Done
    public List<Job> fetchJobsForParamSuggestion(){
        List<Job> jobsForSwarmSuggestion = new ArrayList<Job>();

        List<JobExecution> pendingParamExecutionList = JobExecution.find.where().or(
                Expr.or(Expr.eq(PARAM_SET_STATE_FIELD_NAME, JobExecution.ParamSetStatus.CREATED),
                Expr.eq(PARAM_SET_STATE_FIELD_NAME, JobExecution.ParamSetStatus.SENT)),
                Expr.eq(PARAM_SET_STATE_FIELD_NAME, JobExecution.ParamSetStatus.EXECUTED)).findList();

        List<Job> pendingParamJobList = new ArrayList<Job>();
        for (JobExecution pendingParamExecution: pendingParamExecutionList){
            pendingParamJobList.add(pendingParamExecution.job);
        }

        for (Job job: Job.find.all()){
            if (!pendingParamJobList.contains(job)){
                    jobsForSwarmSuggestion.add(job);
            }
        }
        return jobsForSwarmSuggestion;
    }

    //Done
    public JsonNode particleListToJson(List<Particle> particleList){
        JsonNode jsonNode = Json.toJson(particleList);
        return jsonNode;
    }

    //Done
    public List<TunerState> getJobsTunerState(List<Job> tuninJobs){
        List<TunerState> tunerStateList = new ArrayList<TunerState>();
        for (Job job: tuninJobs){

            List<AlgoParam> algoParamList = AlgoParam.find.where().eq(ALGO_FIELD_NAME, job.algo).findList();
            TunerState tunerState = new TunerState();
            tunerState.setTuningJob(job);
            tunerState.setParametersToTune(algoParamList);

            JobSavedState jobSavedState = JobSavedState.find.byId(job.jobId);
            if(jobSavedState!=null){

                String savedState = new String(jobSavedState.savedState);
                ObjectNode jsonSavedState = (ObjectNode) Json.parse(savedState);
                JsonNode jsonCurrentPopulation = jsonSavedState.get(JSON_CURRENT_POPULATION_KEY);
                List<Particle> currentPopulation = jsonToParticleList(jsonCurrentPopulation);
                for( Particle particle: currentPopulation){
                    Long paramSetId = particle.getParamSetId();
                    JobExecution jobExecution = JobExecution.find.byId(paramSetId);
                    particle.setFitness(jobExecution.costMetric);
                }

                JsonNode updatedJsonCurrentPopulation = particleListToJson(currentPopulation);
                jsonSavedState.set(JSON_CURRENT_POPULATION_KEY, updatedJsonCurrentPopulation);
                savedState = Json.stringify(jsonSavedState);
                tunerState.setStringTunerState(savedState);

            }
            else{
                tunerState.setStringTunerState("{}");
            }
            tunerStateList.add(tunerState);
        }
        return tunerStateList;
    }

    // Done
    public List<JobSuggestedParamValue> getParamValueList(Particle particle, List<AlgoParam> paramList){
        LogUtility.log("Particle is: " + Json.toJson(particle));
        List<JobSuggestedParamValue> jobSuggestedParamValueList = new ArrayList<JobSuggestedParamValue>();

        List<Double> candidate = particle.getCandidate();
        LogUtility.log("Candidate is:" + Json.toJson(candidate));



        for (int i=0; i< candidate.size() && i<paramList.size(); i++){
            LogUtility.log("Candidate is " + candidate);
            JobSuggestedParamValue jobSuggestedParamValue = new JobSuggestedParamValue();
            JobSuggestedParamValue.PrimaryKey primaryKey = new JobSuggestedParamValue.PrimaryKey();

            int paramId = paramList.get(i).paramId;
            primaryKey.primaryKeyParamId = paramId;
            AlgoParam algoParam = AlgoParam.find.byId(paramId);

            jobSuggestedParamValue.paramValuePK = primaryKey;
            jobSuggestedParamValue.algoParam = algoParam;
            double tmpParamValue = candidate.get(i);
            jobSuggestedParamValue.paramValue = String.valueOf(tmpParamValue);
            jobSuggestedParamValueList.add(jobSuggestedParamValue);
        }

        return jobSuggestedParamValueList;
    }


    //Done
    public void updateDatabase(List<TunerState> jobTunerStateList){
        /**
         * For every tuner state:
         *  For every new particle:
         *      From the tuner set extract the list of suggested parameters
         *      Check penalty
         *      Save the param in the job execution table by creating execution instance
         *      Update the execution instance in each of the suggested params
         *      save th suggested parameters
         *      update the paramsetid in the particle and add particle to a particlelist
         *  Update the tunerstate from the updated particles
         *  save the tuner state in db
         */

        for (TunerState jobTunerState: jobTunerStateList){

            Job job = jobTunerState.getTuningJob();
            List<AlgoParam> paramList = jobTunerState.getParametersToTune();
            String stringTunerState = jobTunerState.getStringTunerState();

            JsonNode jsonTunerState = Json.parse(stringTunerState);
            JsonNode jsonSuggestedPopulation = jsonTunerState.get(JSON_CURRENT_POPULATION_KEY);

            if(jsonSuggestedPopulation == null){
                break;
            }
            List<Particle> suggestedPopulation = jsonToParticleList(jsonSuggestedPopulation);

            for (Particle suggestedParticle: suggestedPopulation){
                List<JobSuggestedParamValue> jobSuggestedParamValueList = getParamValueList(suggestedParticle, paramList);

                JobExecution jobExecution = new JobExecution();
                jobExecution.job = job;
                jobExecution.algo = job.algo;
                jobExecution.isDefaultExecution = false;
                if (isParamConstraintViolated(jobSuggestedParamValueList)){
                    jobExecution.paramSetState = JobExecution.ParamSetStatus.FITNESS_COMPUTED;
//                    jobExecution.resourceUsage = (double) -1;
//                    jobExecution.executionTime = (double) -1;
                    jobExecution.costMetric = 3 * job.averageResourceUsage * job.allowedMaxResourceUsagePercent / 100.0;
                }
                else{
                    jobExecution.paramSetState = JobExecution.ParamSetStatus.CREATED;
                }
                Long paramSetId = saveSuggestedParamMetadata(jobExecution);

                for (JobSuggestedParamValue jobSuggestedParamValue: jobSuggestedParamValueList){
                    jobSuggestedParamValue.jobExecution = jobExecution;
                    jobSuggestedParamValue.paramValuePK.primaryKeyParamSetId = paramSetId;
                }
                suggestedParticle.setPramSetId(paramSetId);
                saveSuggestedParams(jobSuggestedParamValueList);
            }

            JsonNode updatedJsonSuggestedPopulation = particleListToJson(suggestedPopulation);

            ObjectNode updatedJsonTunerState = (ObjectNode) jsonTunerState;
            updatedJsonTunerState.put(JSON_CURRENT_POPULATION_KEY, updatedJsonSuggestedPopulation);
            String updatedStringTunerState = Json.stringify(updatedJsonTunerState);
            jobTunerState.setStringTunerState(updatedStringTunerState);
        }
        saveTunerState(jobTunerStateList);
    }

    //Done
    public boolean isParamConstraintViolated(List<JobSuggestedParamValue> jobSuggestedParamValueList){
        //[1] sort.mb > 60% of map.memory: To avoid heap memory failure
        //[2] map.memory - sort.mb < 768: To avoid heap memory failure
        //[3] pig.maxCombinedSplitSize > 1.8*mapreduce.map.memory.mb

        Integer violations = 0;
        Double mrSortMemory = null;
        Double mrMapMemory = null;
        Double pigMaxCombinedSplitSize = null;

        for (JobSuggestedParamValue jobSuggestedParamValue: jobSuggestedParamValueList){
            if(jobSuggestedParamValue.algoParam.paramName.equals("mapreduce.task.io.sort.mb")){
                mrSortMemory = Double.parseDouble(jobSuggestedParamValue.paramValue);
            } else if(jobSuggestedParamValue.algoParam.paramName.equals("mapreduce.map.memory.mb")){
                mrMapMemory = Double.parseDouble(jobSuggestedParamValue.paramValue);
            }else if(jobSuggestedParamValue.algoParam.paramName.equals("pig.maxCombinedSplitSize")){
                pigMaxCombinedSplitSize = Double.parseDouble(jobSuggestedParamValue.paramValue);
            }
        }

        if (mrSortMemory!= null && mrMapMemory!= null){
            if (mrSortMemory>0.6*mrMapMemory){
                violations++;
            }
            if (mrMapMemory-mrSortMemory < 768){
                violations++;
            }
        }

        if(pigMaxCombinedSplitSize!= null && mrMapMemory!=null && (pigMaxCombinedSplitSize > 1.8*mrMapMemory)){
            violations++;
        }

        if(violations==0){
            return false;
        } else{
            return true;
        }
    }

    //Done
    public void saveTunerState(List<TunerState> tunerStateList){
        for (TunerState tunerState: tunerStateList){
            JobSavedState jobSavedState = JobSavedState.find.byId(tunerState.getTuningJob().jobId);
            if(jobSavedState==null){
                jobSavedState = new JobSavedState();
                jobSavedState.jobId = tunerState.getTuningJob().jobId;
            }
            jobSavedState.savedState = tunerState.getStringTunerState().getBytes();
            jobSavedState.save();
        }
    }


    //Done
    public void saveSuggestedParams(List<JobSuggestedParamValue> jobSuggestedParamValueList){
        for(JobSuggestedParamValue jobSuggestedParamValue: jobSuggestedParamValueList){
            jobSuggestedParamValue.save();
        }
    }

    //Done
    public Long saveSuggestedParamMetadata(JobExecution jobExecution){
        jobExecution.save();
        return jobExecution.paramSetId;
    }


    public void getParams(){
        List<Job> jobsForSwarmSuggestion = fetchJobsForParamSuggestion();
        List<TunerState> jobTunerStateList= getJobsTunerState(jobsForSwarmSuggestion);
        List<TunerState> updatedJobTunerStateList = new ArrayList<TunerState>();
        for (TunerState jobTunerState: jobTunerStateList){
            TunerState newJobTunerState = generateParamSet(jobTunerState);
            updatedJobTunerStateList.add(newJobTunerState);
        }
        updateDatabase(updatedJobTunerStateList);
    }

}