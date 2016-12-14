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

const TIME = {
  milliseconds_in_seconds: 1000,
  seconds_in_minutes: 60,
  minutes_in_hours: 60,
  hours_in_days: 24
};

/**
 * Breaks down milliseconds to HH:MM:SS
 * @param params time in milliseconds
 * @returns {*}
 */
export function getDurationBreakdown(params) {
  let [duration] = params;
  var seconds = parseInt((duration / TIME.milliseconds_in_seconds) % TIME.seconds_in_minutes), minutes = parseInt((duration / (TIME.milliseconds_in_seconds * TIME.seconds_in_minutes)) % TIME.minutes_in_hours), hours = parseInt((duration / (TIME.milliseconds_in_seconds * TIME.seconds_in_minutes * TIME.minutes_in_hours)));

  if(duration<TIME.milliseconds_in_seconds) {
    return "00:00:00";
  }
  hours = (hours < 10) ? "0" + hours : hours;
  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;

  return hours + ":" + minutes + ":" + seconds;
}

export default Ember.Helper.helper(getDurationBreakdown);
