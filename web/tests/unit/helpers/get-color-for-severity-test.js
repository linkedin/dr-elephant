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

import { getColorForSeverity } from 'dr-elephant/helpers/get-color-for-severity';
import { module, test } from 'qunit';

module('Unit | Helper | get color for severity');

test('Test for getColorForSeverity helper', function(assert) {
  let result = getColorForSeverity(["critical"]);
  assert.equal(result,"#D9534F");
  result = getColorForSeverity(["severe"]);
  assert.equal(result,"#E4804E");
  result = getColorForSeverity(["moderate"]);
  assert.equal(result,"#F0AD4E");
  result = getColorForSeverity(["low"]);
  assert.equal(result,"#5CB85C");
  result = getColorForSeverity(["none"]);
  assert.equal(result,"#5CB85C");
});

