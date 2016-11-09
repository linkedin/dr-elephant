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
import moment from 'moment';

export default Ember.Controller.extend({
  notifications: Ember.inject.service('notification-messages'),
  loading: false,

  queryParams: ['username', 'queueName', 'jobType', 'severity', 'analysis', 'finishTimeBegin', 'finishTimeEnd',
    'offset', 'limit'],

  /** query params **/
  username: null,
  queueName: null,
  jobType: null,
  severity: null,
  analysis: null,
  finishTimeBegin: null,
  finishTimeEnd: null,
  offset: null,
  limit: null,

  /** values for parameters **/
  usernameValue: null,
  queueNameValue: null,
  severityValue: null,
  analysisValue: null,
  jobTypeValue: null,

  /** values binded to form inputs **/
  finishTimeBeginValue: null,
  finishTimeEndValue: null,
  isJobTypeChecked: false,
  isSeverityChecked: false,
  isFinishDateChecked: false,

  /** pagination variables **/
  paging: null,
  shouldShowPaging: false,
  entriesPerPage: 20,
  maxPagesToShow: 10,
  currentPage: 1,

  /**
   * Watcher for model. We need this watcher for paging and notifications
   */
  watchModel: Ember.observer('model.summaries', function () {
    var totalEntries = this.get("model.summaries.total");
    var startOfEntries = this.get("model.summaries.start");

    var numberOfPages = Math.ceil(totalEntries / this.get("entriesPerPage"));
    var startPage = Math.ceil((startOfEntries + 1) / this.get("entriesPerPage"));

    var currentPage = Math.ceil((startOfEntries + 1) / this.get("entriesPerPage"));

    var pages = [];
    for (var i = startPage; i <= Math.min(numberOfPages, startPage + this.get("maxPagesToShow")); i++) {
      var singleObject = {};
      singleObject['number'] = (i);
      pages.push(singleObject);
    }

    /** show paging when number of pages are more than one **/
    if (numberOfPages > 1) {
      this.set("shouldShowPaging", true);
    } else {
      this.set("shouldShowPaging", false);
    }

    /** set variables for paging **/
    this.set("currentPage", currentPage);
    this.set("paging", {pages: pages, currentPage: currentPage, numberOfPages: numberOfPages});

    /** show notification if no results **/
    if (this.get("model.summaries.total") == 0) {
      this.get('notifications').error('No applications found for given query!', {
        autoClear: true
      });
    }
  }),

  /**
   * Watches the isJobTypeChecked boolean flag. This flag is true when the checkbox for jobtype is ticked.
   * We need to tie the jobType with the value of the jobtype selection input whenever the checkbox is checked.
   */
  watchJobCheck: Ember.observer('isJobTypeChecked', function () {
    if (!this.get("isJobTypeChecked")) {
      this.set("jobTypeValue", null);
    } else {
      this.set("jobTypeValue",
          this.get("model.searchOptions.jobcategory").get('firstObject').jobtypes.get('firstObject').name);
    }
  }),

  /**
   * Watches the isFinishDateChecked boolean flag. This flag is true when the checkbox for FinishDate is ticked.
   * We need to tie the finishTimeBegin and finishTimeEnd  with the value of the jobtype selection input whenever the checkbox is checked.
   */
  watchFinishTimeCheck: Ember.observer('isFinishDateChecked', function () {
    this.set("finishTimeBeginValue", null);
    this.set("finishTimeEndValue", null);
  }),

  /**
   * Watches the isSeverityChecked boolean flag. This flag is true when the checkbox for Severity is ticked.
   * We need to tie the severity and analysis with the value of the severity and analysis selection input whenever the checkbox is checked.
   */
  watchSeverityCheck: Ember.observer('isSeverityChecked', function () {
    if (!this.get("isSeverityChecked")) {
      this.set("analysisValue", null);
      this.set("severityValue", null);
    } else {
      this.set("severityValue", this.get("model.searchOptions.severities").get('firstObject').value);
    }
  }),

  /**
   * Actions
   **/
  actions: {

    /**
     * Actions for select inputs
     */

    selectHeuristic(heuristic) {
      this.set("analysisValue", heuristic);
    },
    selectSeverity(severity) {
      this.set("severityValue", severity);
    },
    selectJobType(jobType) {
      this.set("jobTypeValue", jobType);
    },

    /**
     * loads the page
     */
    loadPage (page) {
      this.set("loading", true);
      var _this = this;
      this.set("offset", this.get("entriesPerPage") * (page - 1));
      this.set("limit", this.get("entriesPerPage"));
      this.set("finishTimeBegin", this.get("finishTimeBeginValue"));
      this.set("finishTimeEnd", this.get("finishTimeEndValue"));
      this.set("severity", this.get("severityValue"));
      this.set("jobType", this.get("jobTypeValue"));
      this.set("username", this.get("usernameValue"));
      this.set("queueName", this.get("queueNameValue"));
      this.set("analysis", this.get("analysisValue"));
      var newsummaries = this.store.queryRecord('search-result', {
        'username': this.username,
        'queue-name': this.queueName,
        'job-type': this.jobType,
        'severity': this.severity,
        'analysis': this.analysis,
        'finished-time-begin': moment(this.get('finishTimeBegin')).valueOf(),
        'finished-time-end': moment(this.get('finishTimeEnd')).valueOf(),
        'type': this.type,
        'offset': this.offset,
        'limit': this.limit
      });

      /**
       * update model after fetching the searched data
       */
      newsummaries.then(() => {
        _this.set("model.summaries", newsummaries);
        _this.set("loading", false);
      });
    },

    /**
     * loads the first page
     */
    search: function () {
      this.send('loadPage', 1);
    }
  }
});
