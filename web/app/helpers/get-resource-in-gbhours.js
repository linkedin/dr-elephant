import Ember from 'ember';

const mbs_in_one_gb = 1024;
const seconds_in_one_hour = 3600;

/**
 * Returns the resource after converting to GB Hours
 * @param params The resource in MB Seconds
 * @returns Resource in GB Hours
 */
export function getResourceInGBHours(params) {
  let [MBSeconds] = params;
  if (MBSeconds == 0) {
    return "0 GB Hours";
  }

  var GBseconds = MBSeconds/mbs_in_one_gb;
  var GBHours = GBseconds / seconds_in_one_hour;

  if ((GBHours * 1000).toFixed(0) == 0) {
    return "0 GB Hours";
  }

  var GBHoursString = GBHours.toFixed(3).toString();
  GBHoursString = GBHoursString + " GB Hours";
  return GBHoursString;
}

export default Ember.Helper.helper(getResourceInGBHours);
