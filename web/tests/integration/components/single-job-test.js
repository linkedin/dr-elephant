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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('single-job', 'Integration | Component | single job', {
  integration: true
});

test('test for single-job component', function (assert) {
  this.set("job", {
    id: "id1",
    jobname: "sample_job",
    username: "user1",
    finishtime: 332823048,
    startime: 332432432,
    resourceused: 3423423,
    resourcewasted: 234343,
    runtime: 1899687,
    waittime: 1099583,
    tasksseverity: [
      {
        severity: "Severe",
        count: 1
      },
      {
        severity: "Critical",
        count: 5
      }
    ]
  });

  this.render(hbs`{{single-job job=job}}`);

  assert.equal(this.$('#job_name').text().trim(), ': sample_job');
  assert.equal(this.$('#job_summary_username').text().trim(), 'user1');
  assert.equal(this.$('#job_summary_finishtime').text().trim(), 'Mon Jan 05 1970 01:57:03 GMT+0530 (IST)');
  assert.equal(this.$('#job_summary_aggregated_metrics').text().trim().split("\n").join("").replace(/ /g, ''), '0.929GBHours6.85%00:31:3957.88%');
  assert.equal(this.$('#job_summary_task_severity').text().trim().split("\n").join("").replace(/ /g, ''), '1Severe5Critical');


  this.set("job", {
    id: "id2",
    jobname: "sample_job_2",
    username: "user2",
    finishtime: 3328230,
    startime: 33243,
    resourceused: 3423423,
    resourcewasted: 234343,
    runtime: 1899687,
    waittime: 1099583,
    tasksseverity: [
      {
        severity: "Critical",
        count: 5
      }
    ]
  });

  this.render(hbs`{{single-job job=job}}`);
  assert.equal(this.$('#job_name').text().trim(), ': sample_job_2');
  assert.equal(this.$('#job_summary_username').text().trim(), 'user2');
  assert.equal(this.$('#job_summary_finishtime').text().trim(), "Thu Jan 01 1970 06:25:28 GMT+0530 (IST)");
  assert.equal(this.$('#job_summary_aggregated_metrics').text().trim().split("\n").join("").replace(/ /g, ''), '0.929GBHours6.85%00:31:3957.88%');
  assert.equal(this.$('#job_summary_task_severity').text().trim().split("\n").join("").replace(/ /g, ''), '5Critical');

});

