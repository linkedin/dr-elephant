import Ember from 'ember';

/**
 * Checks if a given string is empty
 * @param params
 * @returns {boolean}
 */
export function notEmpty(params) {
  let [id] = params;
  if(id==="") {
    return false;
  }
  return true;
}

export default Ember.Helper.helper(notEmpty);
