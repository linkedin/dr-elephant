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

export default Ember.Route.extend({

  notifications: Ember.inject.service('notification-messages'), beforeModel(transition) {
    this.finishTimeBegin = transition.queryParams.finishTimeBegin;
    this.finishTimeEnd = transition.queryParams.finishTimeEnd;
    this.sortKey = transition.queryParams.sortKey;
    this.increasing = transition.queryParams.increasing;
    this.users = transition.queryParams.usernames;
    if (this.users != "" && this.users != null) {
      this.set("usernames", transition.queryParams.usernames.split(","));
    } else {
      this.set("usernames", {});
    }
  },

  model() {
    if (this.users != null && this.users != "") {
      let userdetails = this.store.queryRecord('user-detail', {
        'usernames': this.users,
        'finished-time-begin': this.finishTimeBegin,
        'finished-time-end': this.finishTimeEnd,
        'sortKey': this.sortKey,
        'increasing': this.increasing
      });
      return userdetails;
    } else {
      return null;
    }
  },

  actions: {
    error(error, transition) {
      if (error.errors[0].status == 404) {
        this.get('notifications').error('No applications found for given query!', {
          autoClear: true,
        });
        this.set("showUserDetails", false);
      }
    }
  },

  setupController: function (controller, model) {
    if (model == null) {
      controller.set("showUserDetails", false);
      controller.set("usernameSet", new Set());
      controller.set("usernamesArray", Ember.A([]));
      return;
    }
    controller.set('model', model);
    controller.set("showUserDetails", true);

    let usernameSet = new Set();
    for (var i = 0; i < this.get('usernames').length; i++) {
      usernameSet.add(this.get('usernames')[i]);
    }
    controller.set("usernameSet", usernameSet);
    controller.set("usernamesArray", Array.from(usernameSet));
  }
});
