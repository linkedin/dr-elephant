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
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType,

  metrics: Ember.inject.service(),

  didTransition() {
    this._super(...arguments);
    if(config.APP.enableMetrics) {
      this._trackPage();
    }
  },

  _trackPage() {
    Ember.run.scheduleOnce('afterRender', this, () => {
      const page = this.get('url');
      const title = this.getWithDefault('currentRouteName', 'unknown');

      Ember.get(this, 'metrics').trackPage({ page, title });
    });
  }
});

Router.map(function () {
  this.route('dashboard', function () {
    this.route('workflow');
    this.route('job');
    this.route('app');
  });
  this.route('help');
  this.route('workflow');
  this.route('job');
  this.route('app');
  this.route('search');
  this.route('not-found');
  this.route('user-details');
  this.route('workflow-exceptions');
  this.route('work-in-progress');
});

export default Router;
