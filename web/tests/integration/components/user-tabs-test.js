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
