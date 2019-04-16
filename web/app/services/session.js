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

export default Ember.Service.extend({
  currentUser: null,
  isLoggedIn: Ember.computed.bool('currentUser'),
  store: Ember.inject.service(),
  login(username, password, schedulerUrl){
    return Ember.$.ajax({
      url: "/rest/login",
      type: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({
        username: username,
        password: password,
        schedulerUrl: schedulerUrl,
      })
    })
  },
  logout(){
    this.set("currentUser", null);
  },
  setLoggedInUser(username) {
    this.set("currentUser", username);
    //saving username to localStorage for persistence even after browser refresh
    localStorage.setItem('currentUser', username);
  },
  getActiveUser(){
    let activeUser = this.get('currentUser');
    if (activeUser) {
      return activeUser;
    } else {
      activeUser = localStorage.getItem('currentUser');
      if (activeUser) {
        this.set('currentUser', activeUser);
      }
      return activeUser;
    }
  }
})
