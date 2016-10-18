import { getDurationBreakdown } from 'dr-elephant/helpers/get-duration-breakdown';
import { module, test } from 'qunit';

module('Unit | Helper | get duration breakdown');

test('Test for getDurationBreakdown helper', function(assert) {
  let result = getDurationBreakdown([10000000]);
  assert.equal(result,"02:46:40");
  result = getDurationBreakdown([0]);
  assert.equal(result,"00:00:00");
  result = getDurationBreakdown([1]);
  assert.equal(result,"00:00:00");
  result = getDurationBreakdown([1000]);
  assert.equal(result,"00:00:01");
  result = getDurationBreakdown([3600000]);
  assert.equal(result,"01:00:00");
  result = getDurationBreakdown([60000]);
  assert.equal(result,"00:01:00");
});
