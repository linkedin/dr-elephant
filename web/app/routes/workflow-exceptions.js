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
  notifications: Ember.inject.service('notification-messages'),

  model(){
    let exceptionStatuses = this.store.queryRecord('exception-status', {});
    let exceptionValues = null;
    if (this.workflowId == null || this.workflowId == "") {
      exceptionValues = null;
    } else {
      exceptionValues = this.store.query('workflow-exception', {
        'flow-exec-url': this.workflowId, 'scheduler': this.scheduler
      });
    }
    return Ember.RSVP.hash({
      exceptionStatus: exceptionStatuses, exceptions: exceptionValues
    });

  },

  beforeModel(transition) {
    this.workflowId = transition.queryParams.workflowId;
  },

  actions: {
    error(error, transition) {
      console.log("error occured");
      if (error.errors[0].status == 404) {
        this.get('notifications').error('No applications found for given query!', {
          autoClear: true
        });
        this.set("showExceptions", false);
      } else if (error.errors[0].status == 503) {
        this.set("loading", false);
        this.get('notifications').error("This feature is still in progress", {
          autoClear: true
        });
        this.transitionTo('work-in-progress');
      } else {
        console.log("error occured");
        this.get('notifications').error('Unexpected error occured!', {
          autoClear: true
        });
        this.set("showExceptions", false);
      }
    }
  },

  setupController: function (controller, model) {
    controller.set("model", model);
    if (model.exceptions == null) {
      controller.set("showExceptions", false);
      return;
    } else if (Ember.isEmpty(model.exceptions)) {
      controller.set("showExceptions", false);

      this.get('notifications').info('All applications succeeded!', {
        autoClear: true
      });
      return;
    }
    controller.set("showExceptions", true);
  }
});
