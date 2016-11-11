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

import { urlEncode } from 'dr-elephant/helpers/url-encode';
import { module, test } from 'qunit';

module('Unit | Helper | url encode');

test('Test for urlEncode helper', function(assert) {
  let result = urlEncode(["http://localhost:8090?flowid=abc&page=5&heuristic=Mapper Spill Heuristic"]);
  assert.equal(result,"http%3A%2F%2Flocalhost%3A8090%3Fflowid%3Dabc%26page%3D5%26heuristic%3DMapper%20Spill%20Heuristic");
});
