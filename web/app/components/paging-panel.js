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

export default Ember.Component.extend({

  shouldShowPrevious: false,
  shouldShowNext: false,
  nextPageNumber: 1,
  previousPageNumber: 1,

  didReceiveAttrs() {
    this._super(...arguments);
    let currentPage = this.get('paging.currentPage');

    /**
     * if currentPage is not first page, show previous button and assign a page number to previous button
     */
    if (currentPage > 1) {
      this.set('shouldShowPrevious', true);
      this.set('previousPageNumber', currentPage - 1);
    } else {
      this.set('shouldShowPrevious', false);
      this.set('previousPageNumber', 1);
    }

    /**
     * if currentPage is not the last page, show next button and assign a page number to next button
     */
    if (currentPage != this.get("paging.numberOfPages")) {
      this.set("shouldShowNext", true);
      this.set("nextPageNumber", currentPage + 1);
    } else {
      this.set("shouldShowNext", false);
    }
  }
});
