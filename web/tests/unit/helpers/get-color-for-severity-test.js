import { getColorForSeverity } from 'dr-elephant/helpers/get-color-for-severity';
import { module, test } from 'qunit';

module('Unit | Helper | get color for severity');

test('Test for getColorForSeverity helper', function(assert) {
  let result = getColorForSeverity(["critical"]);
  assert.equal(result,"#D9534F");
  result = getColorForSeverity(["severe"]);
  assert.equal(result,"#E4804E");
  result = getColorForSeverity(["moderate"]);
  assert.equal(result,"#F0AD4E");
  result = getColorForSeverity(["low"]);
  assert.equal(result,"#5CB85C");
  result = getColorForSeverity(["none"]);
  assert.equal(result,"#5CB85C");
});

