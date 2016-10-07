import Ember from 'ember';

export default Ember.Controller.extend({
  actions: {

    /** add a new tab and click on it once the rendering is complete **/
    addTab: function(user) {
      this.users.addToUsername(user);
      this.users.setActiveUser(user);
      this.set('model.usernames',this.users.getUsernames());
      Ember.run.scheduleOnce('afterRender', this, function() {
        Ember.$("#"+user).trigger("click");
      });
    },

    /** delete tab and go to `all` tab if deleted tab is the current tab.**/
    deleteTab: function(tabname) {
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
