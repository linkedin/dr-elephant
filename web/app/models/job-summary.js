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

import DS from 'ember-data';
import Scheduler from 'dr-elephant/utils/scheduler';

export default DS.Model.extend({
  jobname: DS.attr("string"),
  jobtype: DS.attr("string"),
  username: DS.attr("string"),
  starttime: DS.attr("date"),
  finishtime: DS.attr("date"),
  runtime: DS.attr("string"),
  waittime: DS.attr("string"),
  resourceused: DS.attr("string"),
  resourcewasted: DS.attr("string"),
  severity: DS.attr("string"),
  jobdefid: DS.attr("string"),
  jobexecid: DS.attr("string"),
  tasksseverity: DS.attr(),
  scheduler: DS.attr("string"),
  jobdisplayname: Ember.computed('jobname', 'jobdefid', 'jobexecid', 'scheduler', function () {
    var jobname = this.get('jobname');
    if(jobname!=null && jobname!="") {
      return jobname;
    }
    var scheduler = new Scheduler();
    return scheduler.getJobDisplayName(this.get('jobexecid'), this.get('jobdefid'), this.get('scheduler'));
  })
});
