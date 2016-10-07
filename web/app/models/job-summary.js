import DS from 'ember-data';

export default DS.Model.extend({
  jobname: DS.attr("string"),
  jobtype: DS.attr("string"),
  username: DS.attr("string"),
  starttime: DS.attr("date"),
  finishtime: DS.attr("date"),
  runtime: DS.attr("string"),
  waittime: DS.attr("string"),
  resourceused: DS.attr("string"),
  resourcewasted: DS.attr("string"),
  severity: DS.attr("string"),
  tasksseverity: DS.attr(),
});
