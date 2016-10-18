import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('heuristics-summary', 'Integration | Component | heuristics summary', {
  integration: true
});

test('Tests for the rendering of heuristics-summary component', function (assert) {
  this.set("heuristics", [
    {
      name: "Mapper Data Skew",
      severity: "None"
    },
    {
      name: "Mapper GC",
      severity: "None"
    },
    {
      name: "Mapper Time",
      severity: "Low"
    },
    {
      name: "Mapper Speed",
      severity: "Low"
    },
    {
      name: "Mapper Spill",
      severity: "Low"
    },
    {
      name: "Mapper Memory",
      severity: "None"
    },
    {
      name: "Reducer Data Skew",
      severity: "None"
    },
    {
      name: "Reducer GC",
      severity: "Low"
    },
    {
      name: "Reducer Time",
      severity: "Low"
    },
    {
      name: "Reducer Memory",
      severity: "None"
    },
    {
      name: "Shuffle & Sort",
      severity: "Low"
    }
  ]);
  this.render(hbs`{{heuristics-summary heuristics=heuristics}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g, ''), 'MapperDataSkewMapperGCMapperTimeMapperSpeedMapperSpillMapperMemoryReducerDataSkewReducerGCReducerTimeReducerMemoryShuffle&Sort');

});
