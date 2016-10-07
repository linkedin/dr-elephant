import Ember from 'ember';

export default Ember.Component.extend({
  actions: {
    /** bubble this action up **/
    deleteTab: function (tabname) {
      this.sendAction('deleteTab', tabname);
    }
  }
});
