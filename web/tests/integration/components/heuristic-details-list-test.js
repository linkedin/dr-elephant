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

moduleForComponent('heuristic-details-list', 'Integration | Component | heuristic details list', {
  integration: true
});

test('Test for heuristic-details', function(assert) {
  this.set("heuristic-details", [{
    name: "Mapper Data Skew",
    severity: "None",
    details: [
      {
        name: "Group A",
        value: "4 tasks @ 443 MB avg"
      },
      {
        name: "Group B",
        value: "53 tasks @ 464 MB avg"
      },
      {
        name: "Number of tasks",
        value: "57"
      }
    ]
  },
  {
    name: "Mapper GC",
    severity: "None",
    details: [
      {
        name: "Avg task CPU time (ms)",
        value: "27565"
      },
      {
        name: "Avg task GC time (ms)",
        value: "885"
      },
      {
        name: "Avg task runtime (ms)",
        value: "40890"
      },
      {
        name: "Number of tasks",
        value: "57"
      },
      {
        name: "Task GC/CPU ratio",
        value: "0.03210593143479049"
      }
    ]
  }]);

  this.render(hbs`{{heuristic-details-list heuristic-details=heuristic-details}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g, ''), 'MapperSkewSeverity:NoneGroupA4tasks@443MBavgGroupB53tasks@464MBavgNumberoftasks57MapperGCSeverity:NoneAvgtaskCPUtime(ms)27565AvgtaskGCtime(ms)885Avgtaskruntime(ms)40890Numberoftasks57TaskGC/CPUratio0.03210593143479049');

});
