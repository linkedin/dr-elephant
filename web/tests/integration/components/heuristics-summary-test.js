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

moduleForComponent('heuristics-summary', 'Integration | Component | heuristics summary', {
  integration: true
});

test('Tests for the rendering of heuristics-summary component', function (assert) {
  this.set("heuristics", [
    {
      name: "Mapper Data Skew",
      severity: "None"
    },
    {
      name: "Mapper GC",
      severity: "None"
    },
    {
      name: "Mapper Time",
      severity: "Low"
    },
    {
      name: "Mapper Speed",
      severity: "Low"
    },
    {
      name: "Mapper Spill",
      severity: "Low"
    },
    {
      name: "Mapper Memory",
      severity: "None"
    },
    {
      name: "Reducer Data Skew",
      severity: "None"
    },
    {
      name: "Reducer GC",
      severity: "Low"
    },
    {
      name: "Reducer Time",
      severity: "Low"
    },
    {
      name: "Reducer Memory",
      severity: "None"
    },
    {
      name: "Shuffle & Sort",
      severity: "Low"
    }
  ]);
  this.render(hbs`{{heuristics-summary heuristics=heuristics}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g, ''), 'MapperSkewMapperGCMapperTimeMapperSpeedMapperSpillMapperMemoryReducerSkewReducerGCReducerTimeReducerMemoryShuffle&Sort');

});
