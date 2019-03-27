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


export default Ember.Controller.extend({
  session: Ember.inject.service(),
  notifications: Ember.inject.service('notification-messages'),
  error: '',
  showError: false,
  schedulerUrl: null,
  cluster: null,
  actions: {
    login() {
      let {username, password} = this.getProperties('username', 'password');
      username = this.getValidStringValue(username);
      password = this.getValidStringValue(password);
      if (!username.length || !password.length) {
        this.get('notifications').error("Username or Password cannot be empty.", {
          autoClear: true
        });
      } else if (this.cluster == null || this.schedulerUrl == null){
        /*if cluster or the schedulerUrl is empty this means that
           login route is not transitioned from the job page */
        this.get('notifications').error("Cannot process your request!! Visit the Job Page again and retry", {
          autoClear: true
        });
      } else {
          //session service will return an Ajax promise as a response of the POST call to /login API
          this.get("session").login(username, password, this.schedulerUrl).then((response) => {
            if (response.hasOwnProperty("status") && response.status === "success") {
            //Setting same TTL for the cookie as the TTL of Azkaban session.id i.e. 10 hours
              var inTenHours = new Date(new Date().getTime() + 10 * 60 * 60 * 1000);
              const cookieName = "elephant." + this.cluster + ".session.id";
              Cookies.set(cookieName, response.session_id, {
                expires: inTenHours
              });
              //setting user
              this.get("session").setLoggedInUser(username);
              this.set("error", '');
              //Transit to previous route if available or transit to index page
              this.get('notifications').success("Successful Login", {
                autoClear: true
              });
              this.transitionToPreviousRoute();
            } else if (response.hasOwnProperty("error")) {
                this.set("showError", true);
                this.set("error", response.error)
            } else {
                this.get('notifications').error("Something went wrong while processing your request", {
                  autoClear: true
                });
            }
            this.resetLoginProperties();
          }, (error) => {
              if (error.status === 500) {
                this.get('notifications').error("The server was unable to process your request", {
                  autoClear: true
                });
              } else if (error.responseText) {
                this.get('notifications').error(error.responseText, {
                  autoClear: true
                });
              }
              this.resetLoginProperties();
          });
      }
    }
  },

  /*Function to return valid string object, if argument is not a valid string
  then empty string will be returned */

  getValidStringValue(value) {
    if (typeof(value) == `undefined`) {
      return ''
    } else {
      return value
    }
  },

  //Function to make a transition to previous route or the index route
  transitionToPreviousRoute() {
    var previousTransition = this.get('previousTransition');
    if (previousTransition) {
      //resetting previous to Null
      this.set('previousTransition', null);
      previousTransition.retry();
    } else {
      // Default back to homepage
      this.transitionToRoute('index');
    }
  },
  /*reset the username and password object to empty shown in login page*/
  resetLoginProperties() {
    // Resetting the properties on login page
    this.setProperties({
      username: '',
      password: ''
    });
  }
})