import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('jobs-severity', 'Integration | Component | jobs severity', {
  integration: true
});

test('Tests for the job severity component', function(assert) {

  this.set("jobsseverity", [
    {
      severity: "Severe",
      count: 1
    },
    {
      severity: "Moderate",
      count: 2
    },
    {
      severity: "Critical",
      count: 1
    }
  ]);
  this.render(hbs`{{jobs-severity jobsseverity=jobsseverity}}`);

  assert.equal(this.$('#job_severities').text().trim().split("\n").join("").replace(/ /g, ''), '1Severe2Moderate1Critical');

});
