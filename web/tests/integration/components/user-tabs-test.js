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
import Ember from 'ember';

moduleForComponent('user-tabs', 'Integration | Component | user tabs', {
  integration: true
});

test('Test for user tabs component', function(assert) {

  // single tab with All text when no data is passed
  this.render(hbs`{{user-tabs}}`);
  assert.equal(this.$().text().trim(), 'All');


  // multiple tabs with id and tabname as the name of the user
  var usernamesArray = Ember.A(["user1","user2","user3","user4"]);
  this.set("users", usernamesArray);
  this.render(hbs`{{user-tabs usernames=users}}`);

  assert.equal(this.$('#all').text().trim(),'All');
  assert.equal(this.$('#user1').text().trim(),'user1');
  assert.equal(this.$('#user2').text().trim(),'user2');
  assert.equal(this.$('#user3').text().trim(),'user3');
  assert.equal(this.$('#user4').text().trim(),'user4');
});
