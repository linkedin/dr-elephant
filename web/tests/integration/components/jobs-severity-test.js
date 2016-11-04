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

moduleForComponent('jobs-severity', 'Integration | Component | jobs severity', {
  integration: true
});

test('Tests for the job severity component', function(assert) {

  this.set("jobsseverity", [
    {
      severity: "Severe",
      count: 1
    },
    {
      severity: "Moderate",
      count: 2
    },
    {
      severity: "Critical",
      count: 1
    }
  ]);
  this.render(hbs`{{jobs-severity jobsseverity=jobsseverity}}`);

  assert.equal(this.$('#job_severities').text().trim().split("\n").join("").replace(/ /g, ''), '1Severe2Moderate1Critical');

});
