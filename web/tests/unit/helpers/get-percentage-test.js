import { getPercentage } from 'dr-elephant/helpers/get-percentage';
import { module, test } from 'qunit';

module('Unit | Helper | get percentage');

test('Test for getPercentage helper', function(assert) {
  let result = getPercentage([5,200]);
  assert.equal(result,"2.50%");
  result = getPercentage([50,200]);
  assert.equal(result,"25.00%");
  result = getPercentage([0,100]);
  assert.equal(result,"0.00%");
  result = getPercentage([100,100]);
  assert.equal(result,"100.00%");
  result = getPercentage([0,0]);
  assert.equal(result,"0%");
  result = getPercentage([1,20]);
  assert.equal(result,"5.00%");
  result = getPercentage([100,20]);
  assert.equal(result,"500.00%");
});
