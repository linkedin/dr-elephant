import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('tasks-severity', 'Integration | Component | tasks severity', {
  integration: true
});

test('Test for task severity component', function(assert) {

  // set task severities here
  this.set("job", {
    tasksseverity: [
      {
        severity: "Severe",
        count: 1
      },
      {
        severity: "Critical",
        count: 5
      }
    ]
  });
  this.render(hbs`{{tasks-severity job=job}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g,''), '1Severe5Critical');

  this.set("job",{})
  this.render(hbs`{{tasks-severity job=job}}`);
  assert.equal(this.$().text().split("\n").join(""),'');

});

