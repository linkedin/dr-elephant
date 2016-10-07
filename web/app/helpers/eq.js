import Ember from 'ember';

/**
 * helper takes two parameters and returns true if both are equal else returns false
 * @param params The parameters for the helper
 * @returns {boolean}
 */
export function eq(params) {
  if (params[0] === params[1]) {
    return true;
  }
  return false;
}

export default Ember.Helper.helper(eq);
