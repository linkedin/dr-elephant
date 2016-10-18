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
