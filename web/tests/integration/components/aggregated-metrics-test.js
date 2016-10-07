import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('aggregated-metrics', 'Integration | Component | aggregated metrics', {
  integration: true
});

test("Test for rendering the aggregated-metrics component", function(assert) {

  this.set('application', { resourceused: 1000000000, resourcewasted: 10000000, runtime: 1000000, waittime: 10000});
  this.render(hbs`{{aggregated-metrics application=application}}`);

  assert.equal(this.$().text().trim().replace(/ /g,'').split("\n").join(""), '271.267GBHours1.00%00:16:401.00%');

  this.set('application', { resourceused: 2342342342342, resourcewasted: 23423423, runtime:32324320, waittime: 3000});
  this.render(hbs`{{aggregated-metrics application=application}}`);

  assert.equal(this.$().text().trim().replace(/ /g,'').split("\n").join(""), "635401.026GBHours0.00%08:58:440.01%");
});
