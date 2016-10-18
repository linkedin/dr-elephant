import Ember from 'ember';

/**
 * Returns the date from milliseconds
 * @param params The date to convert
 * @returns The converted date
 */
export function getDate(params) {
  let [date] = params;
  return new Date(date);
}

export default Ember.Helper.helper(getDate);
