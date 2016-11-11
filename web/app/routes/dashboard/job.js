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
import Users from 'dr-elephant/models/users';

export default Ember.Route.extend({
  users: new Users(),
  beforeModel(){
    this.usernames = this.users.getUsernames();
    this.set('usernames',this.users.getUsernames());
  },
  model(){
    /** Do not load jobs here, jobs will be loaded in afterModel **/
    return Ember.RSVP.hash({
      usernames: this.users.getUsernames(),
      jobs: {}
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
