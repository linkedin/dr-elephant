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

import Ember from 'ember';

// add other schedulers here
const Schedulers = {'AZKABAN': 'azkaban'};

export default Ember.Object.extend({

  getFlowName(flowExecutionId, flowDefinitionId, schedulerName) {

    var flowName;

      try {

        // can add multiple schedulers in the switch statement.
        switch (schedulerName) {


          case Schedulers.AZKABAN:
            var parser = document.createElement('a');

            // flowdefid is always of the form http://localhost:8043/manager?project=projectname&flow=flowname.
            // throw exception for anything else
            parser.href = flowDefinitionId;

            var queryString = (parser.search).substring(1);
            var projectname = queryString.split("&")[0].split("=")[1];
            var flowname = queryString.split("&")[1].split("=")[1];

            // flowexecid is always of the form http://localhost:8043/executor?execid=id.
            // throw exception for anything else
            parser.href = flowExecutionId;

            var execution = (parser.search).substring(1).split("&")[0].split("=")[1];

            // create name of the flow scheduler:project:flow:execution
            flowName = Schedulers.AZKABAN + ": " + projectname + ": " + flowname + ": " + execution;
            break;

          default:
            flowName = flowExecutionId;

          }
        } catch (err) {
            flowName = flowExecutionId;
        }
        return flowName;
    },

  getJobDisplayName(jobExecutionId, jobDefinitionId, schedulerName) {

    var displayName;

    try {

      // can add multiple schedulers in the switch statement.
      switch (schedulerName) {


        case Schedulers.AZKABAN:
          var parser = document.createElement('a');

          parser.href = jobDefinitionId;

          var queryString = (parser.search).substring(1);
          var jobname = queryString.split("&")[2].split("=")[1];

          parser.href = jobExecutionId;
          queryString = (parser.search).substring(1);
          var execution = queryString.split("&")[0].split("=")[1];

          displayName = jobname + ": " + execution;
          console.log(displayName);
          break;

        default:
          displayName = jobExecutionId;

      }
    } catch (err) {
      displayName = jobExecutionId;
    }
    return displayName;
  }
});
