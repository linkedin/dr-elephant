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

import Ember from 'ember';

/**
 * Calculates the percentage given two params
 * @param params The arguments for percentage
 * @returns The percentage in the form PP.PP%
 */
export function getPercentage(params) {
  let [arg1, arg2] = params;
  if(Number(arg2)===0) {
    return "0%";
  }

  var percentage = ( arg1 / arg2 ) * 100;
  var percentString = percentage.toFixed(2).toString()+ "%";
  return percentString;
}

export default Ember.Helper.helper(getPercentage);
