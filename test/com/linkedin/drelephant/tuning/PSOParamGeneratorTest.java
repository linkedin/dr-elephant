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

package com.linkedin.drelephant.tuning;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.linkedin.drelephant.DrElephant;
import com.linkedin.drelephant.ElephantContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamValue;
import models.TuningAlgorithm;
import models.TuningJobDefinition;
import models.TuningJobExecution;
import models.TuningParameter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import play.Application;
import play.GlobalSettings;
import play.libs.Json;
import play.test.FakeApplication;
import org.apache.hadoop.conf.Configuration;


import static common.DBTestUtil.*;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static play.test.Helpers.*;


public class PSOParamGeneratorTest {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PSOParamGeneratorTest.class);
  private static FakeApplication fakeApp;
  private int numParametersToTune;

  @Before
  public void setup() {
    Map<String, String> dbConn = new HashMap<String, String>();
    dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE);
    dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE);
    dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
    dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

    GlobalSettings gs = new GlobalSettings() {
      @Override
      public void onStart(Application app) {
        LOGGER.info("Starting FakeApplication");
      }
    };

    fakeApp = fakeApplication(dbConn, gs);
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Boolean autoTuningEnabled = configuration.getBoolean(DrElephant.AUTO_TUNING_ENABLED, false);
    org.junit.Assume.assumeTrue(autoTuningEnabled);
  }

  private void populateTestData() {
    try {
      initDB();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void generateParamSetTest() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {

        populateTestData();
        JobDefinition jobDefinition = JobDefinition.find.byId(100003);
        TuningJobDefinition tuningJobDefinition =
            TuningJobDefinition.find.where().eq("job.id", jobDefinition.id).findUnique();
        TuningAlgorithm tuningAlgorithm = tuningJobDefinition.tuningAlgorithm;

        List<TuningParameter> tuningParameterList = TuningParameter.find.where()
            .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id, tuningAlgorithm.id)
            .eq(TuningParameter.TABLE.isDerived, 0)
            .findList();

        numParametersToTune = tuningParameterList.size();

        LOGGER.info("PSOParamGeneratorTest parameter list: " + Json.toJson(tuningParameterList));

        JobTuningInfo jobTuningInfo = new JobTuningInfo();
        jobTuningInfo.setTuningJob(jobDefinition);
        jobTuningInfo.setParametersToTune(tuningParameterList);
        jobTuningInfo.setTunerState("{}");

        PSOParamGenerator psoParamGenerator = new PSOParamGenerator();

        JobTuningInfo updatedJobTuningInfo = psoParamGenerator.generateParamSet(jobTuningInfo);
        assertTrue("Updated JobTuningInfo: Job definition mismatch",
            updatedJobTuningInfo.getTuningJob().equals(jobDefinition));
        assertTrue("Updated JobTuningInfo: Parameter list mismatch",
            updatedJobTuningInfo.getParametersToTune().equals(tuningParameterList));

        String tunerState = updatedJobTuningInfo.getTunerState();
        JsonNode jsonTunerState = Json.parse(tunerState);

        assertTrue("Updated JobTuningInfo: Current population not found",
            jsonTunerState.has(JSON_CURRENT_POPULATION_KEY));
        assertTrue("Updated JobTuningInfo: Previous population not found",
            jsonTunerState.has(JSON_PREVIOUS_POPULATION_KEY));
        assertTrue("Updated JobTuningInfo: Archive not found", jsonTunerState.has(JSON_ARCHIVE_KEY));
        assertTrue("Updated JobTuningInfo: Random number state not found", jsonTunerState.has(JSON_RND_STATE_KEY));

        JsonNode currentPopulation = jsonTunerState.get(JSON_CURRENT_POPULATION_KEY);
        assertEquals("Current population not of type array", JsonNodeType.ARRAY, currentPopulation.getNodeType());
        assertEquals("Current population size not equal to swarm size", SWARM_SIZE, currentPopulation.size());

        JsonNode previousPopulation = jsonTunerState.get(JSON_PREVIOUS_POPULATION_KEY);
        assertEquals("Previous population not of type array", JsonNodeType.ARRAY, previousPopulation.getNodeType());
        assertEquals("Previous population size not equal to swarm size", 0, previousPopulation.size());

        JsonNode archive = jsonTunerState.get(JSON_ARCHIVE_KEY);
        assertEquals("Archive population not of type array", JsonNodeType.ARRAY, archive.getNodeType());
        assertEquals("Archive population size not equal to swarm size", SWARM_SIZE, archive.size());

        JsonNode particle = currentPopulation.get(0);
        assertTrue("Particle doesn't contain candidate", particle.has(JSON_PARTICLE_CANDIDATE_KEY));
        assertTrue("Particle doesn't contain birthday", particle.has(JSON_PARTICLE_BIRTHDATE_KEY));
        assertTrue("Particle doesn't contain maximize", particle.has(JSON_PARTICLE_MAXIMIZE_KEY));
        assertTrue("Particle doesn't contain fitness", particle.has(JSON_PARTICLE_FITNESS_KEY));

        JsonNode candidate = particle.get(JSON_PARTICLE_CANDIDATE_KEY);
        assertEquals("Candidate not of type array", JsonNodeType.ARRAY, candidate.getNodeType());
        assertEquals("Candidate size not equal to tuning parameters size", numParametersToTune, candidate.size());

        JsonNode randomNumberState = jsonTunerState.get(JSON_RND_STATE_KEY);
        assertEquals("Random number state not of type string", JsonNodeType.STRING, randomNumberState.getNodeType());

        jobTuningInfo.setTunerState(updatedJobTuningInfo.getTunerState());
        updatedJobTuningInfo = psoParamGenerator.generateParamSet(jobTuningInfo);
        assertTrue("Updated JobTuningInfo: Job definition mismatch",
            updatedJobTuningInfo.getTuningJob().equals(jobDefinition));
        assertTrue("Updated JobTuningInfo: Parameter list mismatch",
            updatedJobTuningInfo.getParametersToTune().equals(tuningParameterList));

        tunerState = updatedJobTuningInfo.getTunerState();
        jsonTunerState = Json.parse(tunerState);

        assertTrue("Updated JobTuningInfo: Current population not found",
            jsonTunerState.has(JSON_CURRENT_POPULATION_KEY));
        assertTrue("Updated JobTuningInfo: Previous population not found",
            jsonTunerState.has(JSON_PREVIOUS_POPULATION_KEY));
        assertTrue("Updated JobTuningInfo: Archive not found", jsonTunerState.has(JSON_ARCHIVE_KEY));
        assertTrue("Updated JobTuningInfo: Random number state not found", jsonTunerState.has(JSON_RND_STATE_KEY));

        currentPopulation = jsonTunerState.get(JSON_CURRENT_POPULATION_KEY);
        assertEquals("Current population not of type array", JsonNodeType.ARRAY, currentPopulation.getNodeType());
        assertEquals("Current population size not equal to swarm size", SWARM_SIZE, currentPopulation.size());

        previousPopulation = jsonTunerState.get(JSON_PREVIOUS_POPULATION_KEY);
        assertEquals("Previous population not of type array", JsonNodeType.ARRAY, previousPopulation.getNodeType());
        assertEquals("Previous population size not equal to swarm size", SWARM_SIZE, previousPopulation.size());

        archive = jsonTunerState.get(JSON_ARCHIVE_KEY);
        assertEquals("Archive population not of type array", JsonNodeType.ARRAY, archive.getNodeType());
        assertEquals("Archive population size not equal to swarm size", SWARM_SIZE, archive.size());

        particle = currentPopulation.get(0);
        assertTrue("Particle doesn't contain candidate", particle.has(JSON_PARTICLE_CANDIDATE_KEY));
        assertTrue("Particle doesn't contain birthday", particle.has(JSON_PARTICLE_BIRTHDATE_KEY));
        assertTrue("Particle doesn't contain maximize", particle.has(JSON_PARTICLE_MAXIMIZE_KEY));
        assertTrue("Particle doesn't contain fitness", particle.has(JSON_PARTICLE_FITNESS_KEY));

        candidate = particle.get(JSON_PARTICLE_CANDIDATE_KEY);
        assertEquals("Candidate not of type array", JsonNodeType.ARRAY, candidate.getNodeType());
        assertEquals("Candidate size not equal to tuning parameters size", numParametersToTune, candidate.size());

        randomNumberState = jsonTunerState.get(JSON_RND_STATE_KEY);
        assertEquals("Random number state not of type string", JsonNodeType.STRING, randomNumberState.getNodeType());
      }
    });
  }

  @Test
  public void getParamsTest() {
    running(testServer(TEST_SERVER_PORT, fakeApp), new Runnable() {
      public void run() {
        populateTestData();
        PSOParamGenerator psoParamGenerator = new PSOParamGenerator();
        psoParamGenerator.getParams();

        List<TuningJobExecution> tuningJobExecutionList = TuningJobExecution.find.where()
            .eq(TuningJobExecution.TABLE.paramSetState, TuningJobExecution.ParamSetStatus.CREATED)
            .findList();
        assertEquals("Swarm size did not match", SWARM_SIZE, tuningJobExecutionList.size());

        TuningJobExecution tuningJobExecution = tuningJobExecutionList.get(0);

        List<JobSuggestedParamValue> jobSuggestedParamValueList = JobSuggestedParamValue.find.where()
            .eq(JobSuggestedParamValue.TABLE.jobExecution + '.' + JobExecution.TABLE.id,
                tuningJobExecution.jobExecution.id)
            .findList();

        TuningAlgorithm tuningAlgorithm = tuningJobExecution.tuningAlgorithm;
        List<TuningParameter> tuningParameterList = TuningParameter.find.where()
            .eq(TuningParameter.TABLE.tuningAlgorithm + "." + TuningAlgorithm.TABLE.id, tuningAlgorithm.id)
            .findList();
        numParametersToTune = tuningParameterList.size();

        assertEquals("Number of parameters didn't match", numParametersToTune, jobSuggestedParamValueList.size());
      }
    });
  }
}
