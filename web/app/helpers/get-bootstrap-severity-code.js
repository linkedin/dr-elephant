import Ember from 'ember';

/** Map to convert serverity to bootstrap class **/
const SEVERITY_TO_BOOTSTRAP_MAP = {
  critical: "danger",
  severe: "severe",
  moderate: "warning",
  low: "success",
  none:"success"
};

/**
 * This helper takes the serverity as the parameter value and returns the corresponding bootstrap code
 * @param params The parameters
 * @returns  one of {"danger","severe","warning","success"}
 */
export function getBootstrapSeverityCode(params) {
  let [severity] = params;
  if (severity == null) {
    return SEVERITY_TO_BOOTSTRAP_MAP.none;
  }
  return SEVERITY_TO_BOOTSTRAP_MAP[severity.toLowerCase()];
}

export default Ember.Helper.helper(getBootstrapSeverityCode);
