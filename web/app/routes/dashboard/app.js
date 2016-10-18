import Ember from 'ember';
import Users from 'dr-elephant/models/users';

export default Ember.Route.extend({
  users: new Users(),
  beforeModel(){
    this.usernames = this.users.getUsernames();
    this.set('usernames',this.users.getUsernames());
  },
  model(){
    /** Do not load applications model here, they will be loaded in afterModel **/
    return Ember.RSVP.hash({
      usernames: this.users.getUsernames(),
      applications: {}
    });
  },
  afterModel() {
    /** once the page is rendered, click on the current user tab **/
    Ember.run.scheduleOnce('afterRender', this, function() {
      if(this.users.getActiveUser()==null) {
        Ember.$("#all a").trigger("click");
      } else {
        Ember.$("#" + this.users.getActiveUser()).trigger("click");
      }
    });
  }
});
