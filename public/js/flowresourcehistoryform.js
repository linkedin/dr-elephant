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

    /* Plot graph for data obtained from ajax call */
    $.getJSON('/rest/flowmetricsgraphdata?id=' + queryString()['flow-def-id'], function(data) {
        updateExecTimezone(data);

        // Compute the jobDefId list such that the job numbers in the tooltip match the corresponding job in the table.
        var jobDefList = [];
        for (var i = data.length - 1 ; i >=0 ; i--) {
            for (var j = 0; j < data[i].jobmetrics.length; j++) {
                var jobDefUrl = data[i].jobmetrics[j]["jobdefurl"];
                if (jobDefList.indexOf(jobDefUrl) == -1) {
                    jobDefList.push(jobDefUrl);
                }
            }
        }

        plotter(data, jobDefList);
    });

    loadTableTooltips();
});
