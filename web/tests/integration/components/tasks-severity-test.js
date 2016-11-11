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

moduleForComponent('tasks-severity', 'Integration | Component | tasks severity', {
  integration: true
});

test('Test for task severity component', function(assert) {

  // set task severities here
  this.set("job", {
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
  this.render(hbs`{{tasks-severity tasksseverity=job.tasksseverity}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g,''), '1Severe5Critical');

  this.set("job",{})
  this.render(hbs`{{tasks-severity job=job}}`);
  assert.equal(this.$().text().split("\n").join(""),'');

});

