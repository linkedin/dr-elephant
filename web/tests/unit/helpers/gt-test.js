
import { gt } from 'dr-elephant/helpers/gt';
import { module, test } from 'qunit';

module('Unit | Helper | gt');

// Replace this with your real tests.
test('it works', function(assert) {
  let result = gt([42,30]);
  assert.ok(result);
  result = gt([30,42])
  assert.ok(!result);
  result = gt([-1,30]);
  assert.ok(!result);
  result = gt([30,-1]);
  assert.ok(result);
  result = gt([-1,-5]);
  assert.ok(result);
  result = gt([-9,-5]);
  assert.ok(!result);
});

