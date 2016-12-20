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

/* jshint node: true */

module.exports = function(environment) {
  var ENV = {

    /* you can enable 'ENV.APP.enableMetrics': true, and add your own metrics tracking code below. Example shown */
    /**
    metricsAdapters: [
      {
        name: 'Piwik',
        environments: ['production','development'],
        config: {
          piwikUrl: 'https://localhost/piwik/',
          siteId: 86
        }
      }
    ],
    **/
    modulePrefix: 'dr-elephant',
    environment: environment,
    baseURL: '/',
    locationType: 'auto',
    EmberENV: {
      FEATURES: {}
    },

    APP: {
      'enableMetrics': false
    },
    'ember-cli-notifications': {
      icons: 'bootstrap'
    }
  };

  if (environment === 'development') {
     ENV.APP.LOG_RESOLVER = true;
     ENV.APP.LOG_ACTIVE_GENERATION = true;
     ENV.APP.LOG_TRANSITIONS = true;
     ENV.APP.LOG_TRANSITIONS_INTERNAL = true;
     ENV.APP.LOG_VIEW_LOOKUPS = true;
  }

  if (environment === 'test') {
    ENV.rootURL = '/new/';
    ENV.locationType = 'none';
    ENV.APP.LOG_ACTIVE_GENERATION = false;
    ENV.APP.LOG_VIEW_LOOKUPS = false;
    ENV.APP.rootElement = '#ember-testing';
  }

  if (environment === 'production') {
    ENV.rootURL = "/new";
    ENV.locationType = 'hash';
  }

  return ENV;
};
