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

moduleForComponent('tunein-info', 'Integration | Component | tunein-info', {
  integration: true
});

test("Test for rendering the tunein-info component", function(assert) {
  this.set('tunein', {jobSuggestedParamSetId: "null", autoApply: false, iterationCount: 10});
  this.set('job', { severity: "NONE"});
  this.render(hbs`{{tunein-info tunein=tunein job=job}}`);

  assert.equal(this.$().text().trim().replace(/ /g,'').split("\n").join(""), '');

  this.set('tunein', {jobSuggestedParamSetId: "100010",
    tuningAlgorithmList: [{name: "HBT"}, {name: "OBT"}],
    tuningParameters: [{currentParamValue: "7949",
      jobSuggestedParamValue: "7949", name: "spark.executor.memory", paramId: 20, userSuggestedParamValue: "7949"}],
    autoApply: true,
    tuningAlgorithm: "HBT",
    iterationCount: 10});
  this.set('job', { severity: "Critical"});
  this.render(hbs`{{tunein-info tunein=tunein job=job}}`);

  assert.equal(this.$('button').text(), 'Show TuneIn Recommended Parameters');

  //after clicking show Tunein Recommendation button
  this.$('button').click();

  assert.equal(this.$('.iteration-count').val(), 10);
  assert.equal(this.$('.param-value').val(), "7949");
  assert.ok(this.$('input[type="checkbox"]').is(":checked"));
  assert.equal(this.$('#soflow').find(":selected").text(), "HBT");
});
