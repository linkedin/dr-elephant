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

import { eq } from 'dr-elephant/helpers/eq';
import { module, test } from 'qunit';

module('Unit | Helper | eq');

test('Test for eq helper', function(assert) {
  let result = eq([100,100]);
  assert.ok(result);
  result = eq([10,100]);
  assert.ok(!result);
  result = eq(["100","100"]);
  assert.ok(result);
  result = eq(["100","10"]);
  assert.ok(!result);
  result = eq(["100",100]);
  assert.ok(!result);
  result = eq([100.00,100.00]);
  assert.ok(result);
  result = eq([100.0,100.1]);
  assert.ok(!result);
});
