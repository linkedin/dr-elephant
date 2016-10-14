import DS from 'ember-data';
import Ember from 'ember';

export default DS.JSONAPIAdapter.extend({
  namespace: 'rest'
});

export default DS.RESTAdapter.extend({
  namespace: 'rest',
  pathForType: function (type) {
    return  Ember.String.pluralize(type);
  }
});
