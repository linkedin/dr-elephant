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
  var seconds = parseInt((duration / TIME.milliseconds_in_seconds) % TIME.seconds_in_minutes), minutes = parseInt((duration / (TIME.milliseconds_in_seconds * TIME.seconds_in_minutes)) % TIME.minutes_in_hours), hours = parseInt((duration / (TIME.milliseconds_in_seconds * TIME.seconds_in_minutes * TIME.minutes_in_hours)) % TIME.hours_in_days);

  if(duration<TIME.milliseconds_in_seconds) {
    return "00:00:00";
  }
  hours = (hours < 10) ? "0" + hours : hours;
  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;

  return hours + ":" + minutes + ":" + seconds;
}

export default Ember.Helper.helper(getDurationBreakdown);
