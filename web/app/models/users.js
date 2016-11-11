/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
