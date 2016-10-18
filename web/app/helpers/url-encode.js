import Ember from 'ember';

/**
 * Encodes a url
 * @param params The url to encode
 * @returns The encoded url
 */
export function urlEncode(params) {
  let [uri] = params;
  return encodeURIComponent(uri);
}

export default Ember.Helper.helper(urlEncode);
