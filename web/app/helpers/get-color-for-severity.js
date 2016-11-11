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


/** Map to convert severity to color **/
const SEVERITY_TO_COLOR_CODE_MAP = {
  critical: "#D9534F",
  severe: "#E4804E",
  moderate: "#F0AD4E",
  low: "#5CB85C",
  none:"#5CB85C"
};

/**
 * Returns the color based on the severity
 * @param params The severity value
 * @returns The color based on the serverity
 */
export function getColorForSeverity(params) {
  let [severity] = params;
  if(severity==null) {
    return SEVERITY_TO_COLOR_CODE_MAP.none;
  }
  return SEVERITY_TO_COLOR_CODE_MAP[severity.toLowerCase()];
}

export default Ember.Helper.helper(getColorForSeverity);
