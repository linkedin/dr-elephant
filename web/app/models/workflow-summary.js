import DS from 'ember-data';

export default DS.Model.extend({
  username: DS.attr("string"),
  starttime: DS.attr("date"),
  finishtime: DS.attr("date"),
  runtime: DS.attr("string"),
  waittime: DS.attr("string"),
  resourceused: DS.attr("string"),
  resourcewasted: DS.attr("string"),
  severity: DS.attr("string"),
  jobsseverity: DS.attr(),
  queue: DS.attr("string")
});
