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

moduleForComponent('aggregated-metrics', 'Integration | Component | aggregated metrics', {
  integration: true
});

test("Test for rendering the aggregated-metrics component", function(assert) {

  this.set('application', { resourceused: 1000000000, resourcewasted: 10000000, runtime: 1000000, waittime: 10000});
  this.render(hbs`{{aggregated-metrics application=application}}`);

  assert.equal(this.$().text().trim().replace(/ /g,'').split("\n").join(""), '271.267GBHours1.00%00:16:401.00%');

  this.set('application', { resourceused: 2342342342342, resourcewasted: 23423423, runtime:32324320, waittime: 3000});
  this.render(hbs`{{aggregated-metrics application=application}}`);

  assert.equal(this.$().text().trim().replace(/ /g,'').split("\n").join(""), "635401.026GBHours0.00%08:58:440.01%");
});
