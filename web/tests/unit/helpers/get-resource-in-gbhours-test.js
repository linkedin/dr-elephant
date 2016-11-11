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

import { getResourceInGBHours } from 'dr-elephant/helpers/get-resource-in-gbhours';
import { module, test } from 'qunit';

module('Unit | Helper | get resource in gbhours');

test('Test for getResourceInGBHours helper', function(assert) {
  let result = getResourceInGBHours([100001010]);
  assert.equal(result,"27.127 GB Hours");
  result = getResourceInGBHours([0]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([100]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([-1]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([33]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([3080328048302480]);
  assert.equal(result,"835592461.020 GB Hours");
});
