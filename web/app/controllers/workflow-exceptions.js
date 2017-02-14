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
  queryParams: ['workflowId', 'scheduler'],
  workflowId: null,
  workflowIdValue: null,
  loading: false,
  showExceptions: false,
  scheduler: null,

  showSchedulerDropdown: true,

  watchModel: Ember.observer('model.exceptionStatus', function () {
    console.log(this.get('model.exceptionStatus.exceptionenabled'));
    if (this.get('model.exceptionStatus.exceptionenabled') == "false") {
      this.transitionToRoute('work-in-progress');
      return;
    }
    if (this.get("model.exceptionStatus.schedulers").length == 1) {
      this.set('showSchedulerDropdown', false);
      this.set('scheduler', this.get('model.exceptionStatus.schedulers')[0].name);
    }
  }),

  actions: {

    /**
     * select the scheduler from dropdown
     */
    selectScheduler(schedulerName) {
      this.set("scheduler", schedulerName);
    },

    /**
     * Search the exceptions
     */
    search() {
      this.set("showExceptions", false);
      this.set("loading", true);
      this.set("workflowId", this.get("workflowIdValue"));
      var _this = this;

      var exceptionResults = this.store.query('workflow-exception', {
        'flow-exec-url': this.get("workflowId"), 'scheduler': this.get('scheduler')
      });

      /**
       * update model after fetching the searched data
       */
      exceptionResults.then(() => {
        if (Ember.isEmpty(exceptionResults)) {
          _this.set("showExceptions", false);
          this.get('notifications').info('All applications succeeded!', {
            autoClear: true
          });
          _this.set("loading", false);
          return;
        }
        _this.set("model.exceptions", exceptionResults);
        _this.set("loading", false);
        _this.set("showExceptions", true);
      }).catch(err => {
        if (err.errors[0].status == 404) {
          _this.set("loading", false);
          this.get('notifications').error('Workflow not found for given url', {
            autoClear: true
          });
          this.set("showExceptions", false);
        } else if (err.errors[0].status == 503) {
          _this.set("loading", false);
          _this.transitionToRoute('work-in-progress');
        } else {
          _this.set("loading", false);
          this.set("showExceptions", false);
          this.get('notifications').error('Unexpected error occured finding the exception', {
            autoClear: true
          });
        }
      });

    }
  }
});
