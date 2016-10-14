import Ember from 'ember';

export default Ember.Component.extend({
  newUser: null,  // this is binded to the text box for adding user
  showInputBox: false,
  actions: {

    /**
     * sets showInputBox to true to show the input box
     */
    showInput() {
      this.set("showInputBox", true);
    },

    /**
     * sets showInputBox to false to hide the input box
     */
    resetInput() {
      this.set("showInputBox", false);
    }
  }
});
