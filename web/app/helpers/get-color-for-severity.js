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
