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

const APPLICATION_TYPES = {
    workflow: "Workflow", job: "Job", application: "Application"
};

export default Ember.Component.extend({

    searchQuery: null,
    selectedType: APPLICATION_TYPES.workflow,
    applicationTypes: [APPLICATION_TYPES.workflow, APPLICATION_TYPES.job, APPLICATION_TYPES.application],
    selectedTypeToolTip: "Workflow execution id/url",

    notifications: Ember.inject.service('notification-messages'),

    actions: {
        selected(selectionName) {
    if (selectionName === "Advanced") {
        // go to advanced search when Advanced is clicked
        this.get('router').transitionTo("search");
    } else {
        this.set("selectedType", selectionName);
        if(selectionName=="Workflow") {
            this.set("selectedTypeToolTip","Workflow execution url/id");
        } else if (selectionName=="Job") {
            this.set("selectedTypeToolTip","Job execution url/id");
        } else if (selectionName=="Application") {
            this.set("selectedTypeToolTip","application id of the job. e.g. application_23423423_343" );
        }
    }
  },

  search() {
    let searchText = this.get("searchQuery");
    let type = this.get("selectedType");

    if (searchText === "" || searchText == null) {
        this.get('notifications').error('Search field cannot be empty!', {
            autoClear: true
        });
        return;
    }

    if (type === APPLICATION_TYPES.workflow) {
        this.get('router').transitionTo('workflow', {queryParams: {workflowid: searchText}});
    } else if (type === APPLICATION_TYPES.job) {
        this.get('router').transitionTo('job', {queryParams: {jobid: searchText}});
    } else if (type === APPLICATION_TYPES.application) {
        this.get('router').transitionTo('app', {queryParams: {applicationid: searchText}});
    }
}
}
});

