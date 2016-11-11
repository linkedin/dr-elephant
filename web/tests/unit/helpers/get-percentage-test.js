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

import { getPercentage } from 'dr-elephant/helpers/get-percentage';
import { module, test } from 'qunit';

module('Unit | Helper | get percentage');

test('Test for getPercentage helper', function(assert) {
  let result = getPercentage([5,200]);
  assert.equal(result,"2.50%");
  result = getPercentage([50,200]);
  assert.equal(result,"25.00%");
  result = getPercentage([0,100]);
  assert.equal(result,"0.00%");
  result = getPercentage([100,100]);
  assert.equal(result,"100.00%");
  result = getPercentage([0,0]);
  assert.equal(result,"0%");
  result = getPercentage([1,20]);
  assert.equal(result,"5.00%");
  result = getPercentage([100,20]);
  assert.equal(result,"500.00%");
});
