import Ember from 'ember';

/**
 * Custom model to store usernames in the local html store.
 */
export default Ember.Object.extend({

  /**
   * Sets the active user to user
   */
  setActiveUser: function(user) {
    localStorage.setItem('active-user',user);
  },

  /**
   * Returns the current active user from store
   */
  getActiveUser: function() {
    if(localStorage.getItem("active-user")=="null") {
      return null;
    }
    return localStorage.getItem("active-user");
  },

  /**
   * Returns all the stored usernames
   */
  getUsernames: function () {

    var usernamesString = localStorage.getItem('dr-elephant-users');
    if(usernamesString == null || usernamesString==="") {
      return Ember.A([]);
    }
    var usernamesArray = Ember.A([]);
    usernamesArray.pushObjects(usernamesString.split(","));
    return usernamesArray;
  },

  /**
   * Stores the usernames
   */
  storeUsernames: function () {
    var usernamesString = this.usernames.join(",");
    localStorage.setItem('dr-elephant-users', usernamesString);
  },

  /**
   * Adds a new user to the localstore
   */
  addToUsername: function (user) {
    var userNames = this.getUsernames();
    if(!userNames.contains(user)) {
      userNames.pushObject(user);
    }
    var usernamesString  = userNames.join(",");
    localStorage.setItem('dr-elephant-users',usernamesString);
  },

  /**
   * Deletes a username from the store
   */
  deleteUsername: function(user) {
    var userNames = this.getUsernames();
    if(userNames.contains(user)) {
      userNames.removeObject(user);
    }
    var usernamesString  = "";
    if(userNames.length!==0) {
      usernamesString  = userNames.join(",");
    }
    localStorage.setItem('dr-elephant-users',usernamesString);
  },

  /**
   * Clears the local storage
   */
  clearStorage: function () {
    localStorage.clear();
  }
});
