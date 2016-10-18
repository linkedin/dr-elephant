import { getBootstrapSeverityCode } from 'dr-elephant/helpers/get-bootstrap-severity-code';
import { module, test } from 'qunit';

module('Unit | Helper | get bootstrap severity code');

test('Test for getBootstrapSeverityCode helper', function(assert) {
  let result = getBootstrapSeverityCode(["critical"]);
  assert.equal("danger",result);
  result = getBootstrapSeverityCode(["severe"]);
  assert.equal("severe",result);
  result = getBootstrapSeverityCode(["moderate"]);
  assert.equal("warning",result);
  result = getBootstrapSeverityCode(["low"]);
  assert.equal("success",result);
  result = getBootstrapSeverityCode(["none"]);
  assert.equal("success",result);
});
