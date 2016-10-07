import Ember from 'ember';

export default Ember.Component.extend({
  newUser: null,  // this is binded to the text box for adding user
  actions: {

    /** bubble this action up */
    setTab: function(tabname) {
      this.sendAction('setTab', tabname);
    },

    /** Adds the tab **/
    addTab: function() {
      this.sendAction('addTab', this.newUser);
      Ember.$("#user-input").attr("type","hidden");
      Ember.$("#user-input").attr("placeholder","Add user");
      Ember.$("#user-input").attr("value","");
    },

    /** bubble this action up */
    deleteTab: function(tabname) {
      this.sendAction('deleteTab', tabname);
    },

    /** brings focus to the input box **/
    showInput: function() {
      Ember.$("#user-input").attr("type","text");
      Ember.$("#user-input").focus();
    }
  }
});
