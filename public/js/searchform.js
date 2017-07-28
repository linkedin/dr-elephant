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

$(document).ready(function(){

  var form = $("#search-form");
  var formSubmit = $("#submit-button");

  var jobId = $("#form-job-id");
  var flowExecId = $("#form-flow-exec-id");
  var jobDefId = $("#form-job-def-id");
  var user = $("#form-username");
  var queueName = $("#form-queue-name");
  var jobtypeEnable = $("#form-job-type-enable");
  var jobtype = $("#form-job-type");
  var severityEnable = $("#form-severity-enable");
  var severity = $("#form-severity");
  var analysis = $("#form-analysis");
  var datetimeEnable = $("#form-datetime-enable");
  var finishTimeBeginDate = $("#form-finished-time-begin-date");
  var finishTimeEndDate = $("#form-finished-time-end-date");
  var finishTimeBeginTimestamp = $("#form-finished-time-begin");
  var finishTimeEndTimestamp = $("#form-finished-time-end");

  finishTimeBeginDate.datepicker({
    autoclose: true,
    todayHighlight: true,
  });
  finishTimeEndDate.datepicker({
    autoclose: true,
    todayHighlight: true,
  });

  var updateForm = function(){
    if(jobId.val()) {
      jobDefId.prop('disabled', true);
      flowExecId.prop('disabled', true);
      user.prop('disabled', true);
      queueName.prop('disabled', true);
      severity.prop('disabled', true);
      analysis.prop('disabled', true);
      jobtype.prop('disabled', true);
      jobtypeEnable.prop('disabled', true);
      severityEnable.prop('disabled', true);
      datetimeEnable.prop('disabled', true);
      finishTimeBeginDate.prop('disabled', true);
      finishTimeEndDate.prop('disabled', true);
    } else if(flowExecId.val()) {
      jobId.prop('disabled', true);
      jobDefId.prop('disabled', true);
      user.prop('disabled', true);
      queueName.prop('disabled', true);
      severity.prop('disabled', true);
      analysis.prop('disabled', true);
      jobtype.prop('disabled', true);
      jobtypeEnable.prop('disabled', true);
      severityEnable.prop('disabled', true);
      datetimeEnable.prop('disabled', true);
      finishTimeBeginDate.prop('disabled', true);
      finishTimeEndDate.prop('disabled', true);
    } else if (jobDefId.val()) {
      jobId.prop('disabled', true);
      flowExecId.prop('disabled', true);
      user.prop('disabled', true);
      queueName.prop('disabled', true);
      severity.prop('disabled', true);
      analysis.prop('disabled', true);
      jobtype.prop('disabled', true);
      jobtypeEnable.prop('disabled', true);
      severityEnable.prop('disabled', true);
      datetimeEnable.prop('disabled', true);
      finishTimeBeginDate.prop('disabled', true);
      finishTimeEndDate.prop('disabled', true);
    }
    else{
      jobId.prop('disabled', false);
      jobDefId.prop('disabled', false);
      flowExecId.prop('disabled', false);
      jobtypeEnable.prop('disabled', false);
      severityEnable.prop('disabled', false);
      datetimeEnable.prop('disabled', false);
      user.prop('disabled', false);
      queueName.prop('disabled', false);
      if(jobtypeEnable.prop('checked')){
        jobtype.prop('disabled', false);
      }
      else {
        jobtype.prop('disabled', true);
      }
      if(severityEnable.prop('checked')){
        severity.prop('disabled', false);
        analysis.prop('disabled', false);
      }
      else {
        severity.prop('disabled', true);
        analysis.prop('disabled', true);
      }
      if(datetimeEnable.prop('checked')){
        finishTimeBeginDate.prop('disabled', false);
        finishTimeEndDate.prop('disabled', false);
      }
      else {
        finishTimeBeginDate.prop('disabled', true);
        finishTimeEndDate.prop('disabled', true);
      }
    }
  }
  jobId.on("propertychange keyup input paste", updateForm);
  flowExecId.on("propertychange keyup input paste", updateForm);
  jobDefId.on("propertychange keyup input paste", updateForm);
  jobtypeEnable.change(updateForm);
  severityEnable.change(updateForm);
  datetimeEnable.change(updateForm);

  formSubmit.click(function() {

    var formParams = form.serialize();

    // Convert the dates from user time-zone to epoch timestamp
    if(datetimeEnable.prop('checked')) {
      var dateBegin = finishTimeBeginDate.val();
      if (dateBegin !== '') {
        finishTimeBeginTimestamp.val(new Date(dateBegin).getTime());
        finishTimeBeginDate.val('');        // Remove this parameter from appearing in url
      }
      var dateEnd = finishTimeEndDate.val();
      if (dateEnd !== '') {
        finishTimeEndTimestamp.val(new Date(dateEnd).getTime());
        finishTimeEndDate.val('');          // Remove this parameter from appearing in url
      }
    }

    // Cache the search parameters
    localStorage.setItem('search-form', formParams);
    //Remove useless fields from the URL
    form.find('input[name]').filter(function(){return !$(this).val();}).attr('name', '');

    form.submit();
  });

  try {
    var data = localStorage.getItem('search-form');
    form.deserialize(data);
  }
  catch(e){}

  updateForm();
});