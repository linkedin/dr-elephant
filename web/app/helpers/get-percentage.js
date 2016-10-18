import Ember from 'ember';

/**
 * Calculates the percentage given two params
 * @param params The arguments for percentage
 * @returns The percentage in the form PP.PP%
 */
export function getPercentage(params) {
  let [arg1, arg2] = params;
  if(arg2===0) {
    return "0%";
  }

  var percentage = ( arg1 / arg2 ) * 100;
  var percentString = percentage.toFixed(2).toString()+ "%";
  return percentString;
}

export default Ember.Helper.helper(getPercentage);
