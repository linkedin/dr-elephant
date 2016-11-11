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

moduleForComponent('single-application', 'Integration | Component | single application', {
  integration: true
});

test('Test for single application component', function(assert) {

  this.set("application",{
    "id": "application_1",
    "username": "user1",
    "jobname": "job1",
    "jobtype": "HadoopJava",
    "starttime": 1475501548193,
    "finishtime": 1475501987103,
    "runtime": 438910,
    "waittime": 21976,
    "resourceused": 252366848,
    "resourcewasted": 86381426,
    "queue": "random",
    "severity": "Severe",
    "heuristicsummary": [
      {
        "name": "Mapper Data Skew",
        "severity": "None"
      },
      {
        "name": "Mapper GC",
        "severity": "None"
      },
      {
        "name": "Mapper Time",
        "severity": "Moderate"
      },
      {
        "name": "Mapper Speed",
        "severity": "None"
      },
      {
        "name": "Mapper Spill",
        "severity": "None"
      },
      {
        "name": "Mapper Memory",
        "severity": "Severe"
      },
      {
        "name": "Reducer Data Skew",
        "severity": "None"
      },
      {
        "name": "Reducer GC",
        "severity": "None"
      },
      {
        "name": "Reducer Time",
        "severity": "None"
      },
      {
        "name": "Reducer Memory",
        "severity": "None"
      },
      {
        "name": "Shuffle & Sort",
        "severity": "None"
      }
    ]
  });

  this.render(hbs`{{single-application application=application}}`);

  assert.equal(this.$('#app_summary_user').text().trim(), 'user1');
  assert.equal(this.$('#app_summary_id').text().trim().replace(/ /g,''), 'job1application_1');
  assert.equal(this.$('#app_summary_finishtime').text().trim(), 'Mon Oct 03 2016 19:09:47 GMT+0530 (IST)');
  assert.equal(this.$('#app_summary_aggregated_metrics').text().trim().split("\n").join("").replace(/ /g, ''), '68.459GBHours34.23%00:07:185.01%');
  assert.equal(this.$('#app_summary_heuristics_summary').text().trim().split("\n").join(""), 'Mapper Data Skew    Mapper GC    Mapper Time    Mapper Speed    Mapper Spill    Mapper Memory    Reducer Data Skew    Reducer GC    Reducer Time    Reducer Memory    Shuffle & Sort');


  // set jobtype to null, the id should be only application name
  this.set("application", {
    "id": "application_1",
    "username": "user1",
    "jobname": null,
    "jobtype": "HadoopJava",
    "starttime": 1475501548193,
    "finishtime": 1475501987103,
    "runtime": 438910,
    "waittime": 21976,
    "resourceused": 252366848,
    "resourcewasted": 86381426,
    "queue": "random",
    "severity": "Severe",
    "heuristicsummary": [
      {
        "name": "Mapper Data Skew",
        "severity": "None"
      }
    ]
  });

  assert.equal(this.$('#app_summary_id').text().trim().replace(/ /g,''), 'application_1');

});

