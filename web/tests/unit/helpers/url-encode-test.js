import { urlEncode } from 'dr-elephant/helpers/url-encode';
import { module, test } from 'qunit';

module('Unit | Helper | url encode');

test('Test for urlEncode helper', function(assert) {
  let result = urlEncode(["http://localhost:8090?flowid=abc&page=5&heuristic=Mapper Spill Heuristic"]);
  assert.equal(result,"http%3A%2F%2Flocalhost%3A8090%3Fflowid%3Dabc%26page%3D5%26heuristic%3DMapper%20Spill%20Heuristic");
});
