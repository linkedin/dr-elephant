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
import Scheduler from 'dr-elephant/utils/scheduler';

export default Ember.Route.extend({
  ajax: Ember.inject.service(),
  notifications: Ember.inject.service('notification-messages'),

  beforeModel: function(transition) {
    let loginController = this.controllerFor('login');
    loginController.set('previousTransition', transition);
    this.jobid = transition.queryParams.jobid;
  },

  model() {
    return Ember.RSVP.hash({
      jobs: this.store.queryRecord('job', {
        jobid: this.get('jobid')
      }),
      tunein: this.store.queryRecord('tunein', {
        id: this.get('jobid')
      })
    });
  },
  doLogin(schedulerUrl, cluster) {
    //confirm if the user want to proceed with login
    const userWantToLogin = confirm('To perform this action user needs to login. Are you sure to proceed?')
    if (userWantToLogin) {
      this.transitionTo('login').then((loginRoute) => {
        loginRoute.controller.set('schedulerUrl', schedulerUrl);
        loginRoute.controller.set('cluster', cluster);
      });
    }
  },
  getUserAuthorizationStatus(jobdefid, schedulerUrl, cookieName) {
    let authorizationStatus;
    const is_authorised_key = 'hasWritePermission'
    $.ajax({
      url: '/rest/userAuthorization',
      type: 'GET',
      data: {
        sessionId: Cookies.get(cookieName),
        jobDefId: jobdefid,
        schedulerUrl: schedulerUrl
      },
      async: false
    }).then((response) => {
      if (response.hasOwnProperty('hasWritePermission')) {
        if (response.hasWritePermission === 'true') {
          authorizationStatus = 'authorised';
        } else {
          authorizationStatus = 'unauthorised';
        }
    } else if (response.hasOwnProperty('error')) {
      if (response.error === 'session') {
        authorizationStatus = 'session_expired';
      } else {
        //Some other error occurred
        authorizationStatus = 'error';
      }
    }
  },
    (error) => {
      switch (error.status) {
        case 400:
          this.showError(error.responseText);
          break;
        case 500:
          this.showError('The server was unable to process your request');
          break;
        default:
          this.showError('Something went wrong!!');
      }
    });
    return authorizationStatus;
  },
  showError(errorMessage) {
    this.controller.set('showError', true);
    this.controller.set('errorMessage', errorMessage);
  },
  clearError() {
    this.controller.set('showError', false);
    this.controller.set('errorMessage', '');
  },
  actions: {
    updateShowRecommendationCount(jobDefinitionId) {
      return this.get('ajax').post('/rest/showTuneinParams', {
        contentType: 'application/json; charset=UTF-8',
        data: JSON.stringify({
          id: jobDefinitionId
        })
      });
    },
    submitUserChanges(job) {
      const jobDefId = job.get('jobdefid');
      const schedulerName = job.get('scheduler');
      const cluster = job.get('cluster');
      const cookieName = 'elephant.' + cluster + '.session.id';
      const scheduler = new Scheduler();
      const schedulerUrl = scheduler.getSchedulerUrl(jobDefId, schedulerName);
      if (!Cookies.get(cookieName)) {
        this.doLogin(schedulerUrl, cluster);
      } else {
        var userAuthorizationStatus = this
            .getUserAuthorizationStatus(jobDefId, schedulerUrl, cookieName);
        if (userAuthorizationStatus === 'authorised') {
          //call the param change function
          //clear error for previous attempt if exists
          this.clearError();
        } else if (userAuthorizationStatus === 'unauthorised') {
          this.showError('User is not authorised to modify TuneIn details!!');
        } else if (userAuthorizationStatus === 'session_expired') {
          //Removing the existing session_id Cookie
          Cookies.remove(cookieName);
          this.doLogin(schedulerUrl, cluster);
        } else if (userAuthorizationStatus === 'error') {
          this.showError('Some error occured while User Authorization!!');
        }
      }
    },
    error(error, transition) {
      if (error.errors[0].status == 404) {
        return this.transitionTo('not-found', {
          queryParams: {
            'previous': window.location.href
          }
        });
      }
    }
  }
});
