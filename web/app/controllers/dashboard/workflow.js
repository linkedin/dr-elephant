import Ember from 'ember';
import users from 'dr-elephant/models/users';
import Dashboard from 'dr-elephant/controllers/dashboard';

export default Dashboard.extend({
  users: new users(),
  loading: false,
  usernames: function () {
    return this.users.getUsernames();
  },
  actions: {

    /** change tab to the clicked user **/
    changeTab: function (tabname) {
      this.set("loading", true);
      this.users.setActiveUser(tabname);
      var _this = this;
      _this.store.unloadAll();
      var newworkflows = this.store.query('workflow-summary', {username: tabname});
      newworkflows.then(function () {
        _this.set("model.workflows", newworkflows);
        _this.set("loading", false);
      });
    }

  }
});
