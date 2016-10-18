import { getDate } from 'dr-elephant/helpers/get-date';
import { module, test } from 'qunit';

module('Unit | Helper | get date');

test('test for getDate helper', function(assert) {
  let result = getDate([42]);
  assert.ok(result);
});
