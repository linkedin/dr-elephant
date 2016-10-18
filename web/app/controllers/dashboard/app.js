import users from 'dr-elephant/models/users';
import Dashboard from 'dr-elephant/controllers/dashboard';

export default Dashboard.extend({
  users: new users(),
  loading: false,

  /**
   * This function returns the list of usernames currently stored
   * @returns The list of usernames currently stored
   */
  usernames() {
    return this.users.getUsernames();
  },

  actions: {

    /**
     * changes the tab to the clicked user
     * @params The name of the user tab
     */
    changeTab(tabname) {
      this.set("loading", true);
      this.users.setActiveUser(tabname);
      var _this = this;
      _this.store.unloadAll();
      var newApplications = this.store.query('application-summary', {username: tabname});
      newApplications.then(function () {
        _this.set("model.applications", newApplications);
        _this.set("loading", false);
      });
    }
  }
});
