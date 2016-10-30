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
    beforeModel: function(transition){
        this.workflowid = transition.queryParams.workflowid;
    },
    model(){
        this.workflows = this.store.queryRecord('workflow',{workflowid: this.get("workflowid")});
        return this.workflows;
    },
    actions: {
        error(error, transition) {
            if (error.errors[0].status == 404) {
                return this.transitionTo('not-found', { queryParams: {'previous': window.location.href}});
            } else {
                this.get('notifications').error('Uh-oh! Something went wrong..', {
                    autoClear: true
                });
                return;
            }
        }
    }
});
