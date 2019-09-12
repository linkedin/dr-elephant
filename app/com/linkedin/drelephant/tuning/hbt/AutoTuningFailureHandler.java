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
package com.linkedin.drelephant.tuning.hbt;

import com.avaje.ebean.OrderBy;
import com.linkedin.drelephant.tuning.AbstractFitnessManager;
import com.linkedin.drelephant.tuning.TuningHelper;
import java.util.List;
import models.JobDefinition;
import models.JobExecution;
import models.JobSuggestedParamSet;
import org.apache.log4j.Logger;


/**
 * This class will be used in Fitness calulation and handle the scenarios
 * if the failure is becuase of Autotuning.
 */
public class AutoTuningFailureHandler implements FailureHandler {
  private JobExecution jobExecution;
  private JobSuggestedParamSet jobSuggestedParamSet;
  private AbstractFitnessManager _abstractFitnessManager;
  private final Logger logger = Logger.getLogger(getClass());
  @Override
  public void calculateFitness(JobExecution jobExecution, JobSuggestedParamSet jobSuggestedParamSet,
      AbstractFitnessManager fitnessManager) {
    this.jobExecution = jobExecution;
    this.jobSuggestedParamSet = jobSuggestedParamSet;
    this._abstractFitnessManager = fitnessManager;
    handleAutoTuningFailure();
  }

  private void handleAutoTuningFailure() {
    _abstractFitnessManager.applyPenalty(jobSuggestedParamSet, jobExecution);
    boolean isCurrentParameterBest = jobSuggestedParamSet.isParamSetBest;
    jobSuggestedParamSet.isParamSetBest = false;
    TuningHelper.updateJobExecution(jobExecution);
    TuningHelper.updateJobSuggestedParamSet(jobSuggestedParamSet,jobExecution);
    if (isCurrentParameterBest) {
      logger.info(" Current parameter is the best parameter ");
      JobSuggestedParamSet bestParameter = calculateNewBestParameter(jobSuggestedParamSet.jobDefinition.id);
      bestParameter.isParamSetBest = true;
      bestParameter.save();
    }
  }

  private JobSuggestedParamSet calculateNewBestParameter(Integer jobDefinitionID) {
    List<JobSuggestedParamSet> jobSuggestedParamSets = JobSuggestedParamSet.find.select("*")
        .where()
        .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinitionID)
        .eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
        .lt(JobSuggestedParamSet.TABLE.fitness, 10000D)
        .findList();
    if (jobSuggestedParamSets != null && jobSuggestedParamSets.size() > 0) {
      return getBestParameter(jobSuggestedParamSets);
    } else {
      return JobSuggestedParamSet.find.select("*")
          .where()
          .eq(JobSuggestedParamSet.TABLE.jobDefinition + "." + JobDefinition.TABLE.id, jobDefinitionID)
          .eq(JobSuggestedParamSet.TABLE.paramSetState, JobSuggestedParamSet.ParamSetStatus.FITNESS_COMPUTED)
          .eq(JobSuggestedParamSet.TABLE.isParamSetDefault, true)
          .orderBy(JobSuggestedParamSet.TABLE.createdTs + " desc")
          .setMaxRows(1)
          .findUnique();
    }
  }

  private JobSuggestedParamSet getBestParameter(List<JobSuggestedParamSet> jobSuggestedParamSets) {
    JobSuggestedParamSet currentMin = jobSuggestedParamSets.get(0);
    for (JobSuggestedParamSet tempJobSuggestedParamSet : jobSuggestedParamSets) {
      if (tempJobSuggestedParamSet.fitness < currentMin.fitness) {
        currentMin = tempJobSuggestedParamSet;
      } else if (tempJobSuggestedParamSet.fitness.equals(currentMin.fitness)) {
        if (TuningHelper.isNewParamBestParam(tempJobSuggestedParamSet, currentMin)) {
          currentMin = tempJobSuggestedParamSet;
        }
      }
    }
    return currentMin;
  }
}
