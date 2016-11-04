/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('single-tab', 'Integration | Component | single tab', {
  integration: true
});

test('Test for single-tab component', function(assert) {

  this.set("name","user1");
  this.render(hbs`{{single-tab name=name}}`);
  assert.equal(this.$().text().trim(), 'user1');

  this.set("name","");
  this.render(hbs`{{single-tab name=name}}`);
  assert.equal(this.$().text().trim(), '');


  this.set("name","all");
  this.render(hbs`{{single-tab name=name}}`);
  assert.equal(this.$().text().trim(), 'all');

});
