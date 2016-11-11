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
  showInputBox: false,
  notifications: Ember.inject.service('notification-messages'),
  actions: {

    /**
     * This action adds a new tab and clicks on it once the tab is added and rendered
     * @params user The user to be added as a tab
     */
    addTab(user) {

      if(user===null || user==="") {
        this.get('notifications').error('The user cannot be empty', {
          autoClear: true
        });
        return;
      }
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
