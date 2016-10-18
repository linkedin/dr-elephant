import { eq } from 'dr-elephant/helpers/eq';
import { module, test } from 'qunit';

module('Unit | Helper | eq');

test('Test for eq helper', function(assert) {
  let result = eq([100,100]);
  assert.ok(result);
  result = eq([10,100]);
  assert.ok(!result);
  result = eq(["100","100"]);
  assert.ok(result);
  result = eq(["100","10"]);
  assert.ok(!result);
  result = eq(["100",100]);
  assert.ok(!result);
  result = eq([100.00,100.00]);
  assert.ok(result);
  result = eq([100.0,100.1]);
  assert.ok(!result);
});
