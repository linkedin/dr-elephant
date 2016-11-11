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
    model(){
        return Ember.RSVP.hash({
            searchOptions: this.store.queryRecord('search-option', {}),
            summaries: this.store.queryRecord('search-result', {
                'username': this.username,
                'queue-name': this.queueName,
                'job-type': this.jobType,
                'severity': this.severity,
                'analysis': this.analysis,
                'finish-time-begin': this.finishTimeBegin,
                'finish-time-end': this.finishTimeEnd,
                'type': this.type,
                'offset': this.offset,
                'limit': this.limit
            })
        });
    },
    beforeModel(transition) {
        this.username = transition.queryParams.username;
        this.queueName = transition.queryParams.queueName;
        this.jobType = transition.queryParams.jobType;
        this.severity = transition.queryParams.severity;
        this.analysis = transition.queryParams.analysis;
        this.finishTimeBegin = transition.queryParams.finishTimeBegin;
        this.finishTimeEnd = transition.queryParams.finishTimeEnd;
        this.type = transition.queryParams.type;
        this.offset = transition.queryParams.offset;
        this.limit = transition.queryParams.limit;
    }
});
