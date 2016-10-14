import Ember from 'ember';

export default Ember.Controller.extend({
  showInputBox: false,
  actions: {

    /**
     * This action adds a new tab and clicks on it once the tab is added and rendered
     * @params user The user to be added as a tab
     */
    addTab(user) {
      this.users.addToUsername(user);
      this.users.setActiveUser(user);
      this.set('model.usernames',this.users.getUsernames());
      Ember.run.scheduleOnce('afterRender', this, function() {
        Ember.$("#"+user).trigger("click");
      });
    },

    /**
     * This action deletes the tab from the list and clicks on the `all` tab
     * @params tabname the tab to delete
     */
    deleteTab(tabname) {
      this.users.deleteUsername(tabname);
      this.set('model.usernames',this.users.getUsernames());
      if(this.users.getActiveUser()===tabname) {
        Ember.run.scheduleOnce('afterRender', this, function () {
          Ember.$("#all a").trigger("click");
        });
      }
    }
  }
});
