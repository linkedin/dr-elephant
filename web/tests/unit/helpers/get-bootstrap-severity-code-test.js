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

import { getBootstrapSeverityCode } from 'dr-elephant/helpers/get-bootstrap-severity-code';
import { module, test } from 'qunit';

module('Unit | Helper | get bootstrap severity code');

test('Test for getBootstrapSeverityCode helper', function(assert) {
  let result = getBootstrapSeverityCode(["critical"]);
  assert.equal("danger",result);
  result = getBootstrapSeverityCode(["severe"]);
  assert.equal("severe",result);
  result = getBootstrapSeverityCode(["moderate"]);
  assert.equal("warning",result);
  result = getBootstrapSeverityCode(["low"]);
  assert.equal("success",result);
  result = getBootstrapSeverityCode(["none"]);
  assert.equal("success",result);
});
