import { getResourceInGBHours } from 'dr-elephant/helpers/get-resource-in-gbhours';
import { module, test } from 'qunit';

module('Unit | Helper | get resource in gbhours');

test('Test for getResourceInGBHours helper', function(assert) {
  let result = getResourceInGBHours([100001010]);
  assert.equal(result,"27.127 GB Hours");
  result = getResourceInGBHours([0]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([100]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([-1]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([33]);
  assert.equal(result,"0 GB Hours");
  result = getResourceInGBHours([3080328048302480]);
  assert.equal(result,"835592461.020 GB Hours");
});
