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

import Scheduler from 'dr-elephant/utils/scheduler';
import { module, test } from 'qunit';

module('Unit | Utility | scheduler');

test('Test scheduler utility', function(assert) {
  var scheduler = new Scheduler();
  var flowdefid = "https://localhost:8443/manager?project=project1&flow=flow1";
  var flowexecid = "https://localhost:8443/executor?execid=1342787";
  var schedulerName = "azkaban";
  assert.equal(scheduler.getFlowName(flowexecid, flowdefid,schedulerName),"azkaban: project1: flow1: 1342787");

  schedulerName = "oozie";
  assert.equal(scheduler.getFlowName(flowexecid, flowdefid,schedulerName),flowexecid);

  flowdefid = "https://x:y:x"
  assert.equal(scheduler.getFlowName(flowexecid, flowdefid,schedulerName),flowexecid);

  flowexecid = "https://x:y:z"
  assert.equal(scheduler.getFlowName(flowexecid, flowdefid,schedulerName),flowexecid);

});

test('Test job display name', function(assert) {
  var scheduler = new Scheduler();
  var jobdefid = "https://localhost:8443/manager?project=project1&flow=flow1&job=job1"
  var jobexecid = "https://localhost:8443/executor?execid=1&job=job1&attempt=0";
  var schedulerName = "azkaban";
  assert.equal(scheduler.getJobDisplayName(jobexecid, jobdefid,schedulerName),"job1: 1");

  schedulerName = "oozie";
  assert.equal(scheduler.getJobDisplayName(jobexecid, jobdefid,schedulerName),jobexecid);

  jobdefid = "https://x:y:x"
  assert.equal(scheduler.getJobDisplayName(jobexecid, jobdefid,schedulerName),jobexecid);

  jobexecid = "https://x:y:z"
  assert.equal(scheduler.getJobDisplayName(jobexecid, jobdefid,schedulerName),jobexecid);

});
