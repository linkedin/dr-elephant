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
  usernameSet: null,
  usernamesArray: null,
  queryParams: ['usernames','finishTimeBegin', 'finishTimeEnd', 'sortKey', 'increasing'],
  increasing: false,
  usernames: null,
  finishTimeBeginValue: null,
  finishTimeEndValue: null,
  finishTimeBegin: null,
  finishTimeEnd: null,
  sortKey: "severity",
  showInputBox: false,
  newUser: null,
  loading: false,

  showUserDetails: false,

  /** paging variables **/
  paging: null,
  shouldShowPaging: true,
  entriesPerPage: 20,
  maxPagesToShow: 10,
  currentPage: 1,


  /**
   * Watcher for model. We need this watcher for paging and notifications
   */
  watchModel: Ember.observer('model', function () {



    var totalEntries = this.get("model.total");
    if(totalEntries>0 && this.get("model")!={}) {
      if(this.get("finishTimeBegin")==null && this.get("finishTimeEnd")==null) {
        this.get('notifications').info('Showing result for last one week!', {
          autoClear: true
        });
      }
      this.set("showUserDetails",true);
    } else {
      this.set("showUserDetails", false);
    }
    var startOfEntries = this.get("model.start");

    let entriesPerPage = this.get("entriesPerPage");
    var numberOfPages = Math.ceil(totalEntries / entriesPerPage);
    var startPage = Math.ceil((startOfEntries + 1) / entriesPerPage);

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
    if (this.get("model.total") == 0) {
      if(this.get("finishTimeBegin")==null && this.get("finishTimeEnd")==null) {
        this.get('notifications').error('No applications found for last week. Try different date range!', {
          autoClear: true
        });
      } else {
        this.get('notifications').error('No applications found!', {
          autoClear: true
        });
      }
    }
  }),

  actions: {

    /**
     * This action adds a new tab and clicks on it once the tab is added and rendered
     * @params user The user to be added as a tab
     */
    addTab(user) {
      if(user===null || user==="") {
        this.get('notifications').error('The user cannot be empty', {
          autoClear: true
        });
        return;
      }
      this.set("usernamesArray", Array.from(this.get("usernameSet").add(user)));
      this.set("usernames", this.get("usernamesArray").join(","));
      this.send('loadPage', 1);
    },

    /**
     * This action deletes the tab from the list and clicks on the `all` tab
     * @params tabname the tab to delete
     */
    deleteTab(user) {
      this.get("usernameSet").delete(user);
      this.set("usernamesArray", Array.from(this.get("usernameSet")));
      this.set("usernames", this.get("usernamesArray").join(","));
      this.send('loadPage', 1);
    },

    /**
     * This action searches the results based on given parameters
     */
    search() {
      this.send('loadPage', 1);
    },

    /**
     * Selects the sort key.
     * @param sortBy
     */
    selectSortKey(sortBy) {
      this.set("sortKey", sortBy);
    },

    /**
     * This action loads the given page.
     * @param page number
     */
    loadPage(page) {
      var _this = this;
      this.set("loading", true);
      this.set("offset", this.get("entriesPerPage") * (page - 1));
      this.set("limit", this.get("entriesPerPage"));
      if(this.get("finishTimeBeginValue")!=null) {
        this.set("finishTimeBegin", moment(this.get("finishTimeBeginValue")).valueOf());
      } else {
        this.set("finishTimeBegin", null);
      }

      if(this.get("finishTimeEndValue")!=null) {
        this.set("finishTimeEnd", moment(this.get("finishTimeEndValue")).valueOf());
      } else {
        this.set("finishTimeEnd", null);
      }
      this.set("jobType", this.get("jobTypeValue"));
      this.set("username", this.get("usernameValue"));

      var newsummaries = this.store.queryRecord('user-detail', {
        'usernames': this.usernames,
        'finished-time-begin': this.get('finishTimeBegin'),
        'finished-time-end': this.get('finishTimeEnd'),
        'offset': this.offset,
        'limit': this.limit,
        'sortKey': this.sortKey,
        'increasing': this.increasing
      });


      /**
       * update model after fetching the searched data
       */
      newsummaries.then(() => {
        _this.set("model", newsummaries);
        _this.set("loading", false);
      }).catch(err => {
        _this.set("loading", false);
        this.set("showUserDetails", false);
        this.set("shouldShowPaging", false);
      });
    }
  }
});
