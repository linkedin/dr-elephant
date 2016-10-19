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

/** Map to convert serverity to bootstrap class **/
const SEVERITY_TO_BOOTSTRAP_MAP = {
  critical: "danger",
  severe: "severe",
  moderate: "warning",
  low: "success",
  none:"success"
};

/**
 * This helper takes the serverity as the parameter value and returns the corresponding bootstrap code
 * @param params The parameters
 * @returns  one of {"danger","severe","warning","success"}
 */
export function getBootstrapSeverityCode(params) {
  let [severity] = params;
  if (severity == null) {
    return SEVERITY_TO_BOOTSTRAP_MAP.none;
  }
  return SEVERITY_TO_BOOTSTRAP_MAP[severity.toLowerCase()];
}

export default Ember.Helper.helper(getBootstrapSeverityCode);
