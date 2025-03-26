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
  $.getJSON('./rest/flowgraphdata?id=' + queryString()['flow-def-id'], function(data) {
    updateExecTimezone(data);

    // Compute the jobDefId list such that the job numbers in the tooltip match the corresponding job in the table.
    var jobDefList = [];
    for (var i = data.length - 1 ; i >=0 ; i--) {
      for (var j = 0; j < data[i].jobscores.length; j++) {
        var jobDefUrl = data[i].jobscores[j]["jobdefurl"];
        if (jobDefList.indexOf(jobDefUrl) == -1) {
          jobDefList.push(jobDefUrl);
        }
      }
    }

    plotter(data, jobDefList);
  });

  loadTableTooltips();
});

/**
 * Example tooltip content:
 *
 * Sat Oct 17 2015 01:47:59 GMT+0530 (IST)
 * Flow score = 163672
 * Top poor jobs
 * Job 25  45%
 * Job 16  20%
 * job 14  10%
 *
 */
function getGraphTooltipContent(record, jobDefList) {

  var content = document.createElement("div");
  content.style.textAlign = "center";

  var heading = document.createElement("b");
  heading.appendChild(document.createTextNode(record.flowtime));
  heading.appendChild(document.createElement("br"));

  var details = document.createElement("p");
  details.appendChild(document.createTextNode("Flow Score = " + record.score));

  var jobTable = document.createElement("table");
  if (record.score != 0) {
    var jobLimit = 3;
    details.appendChild(document.createElement("br"));

    var tableHeader = document.createElement("th");
    tableHeader.setAttribute("colspan", "2");
    tableHeader.style.padding = "3px";
    tableHeader.style.textAlign = "center";
    tableHeader.appendChild(document.createTextNode("Score Distribution"));
    jobTable.appendChild(tableHeader);

    var scoreList = [];
    for (var i = 0; i < record.jobscores.length; i++) {
      var scoreWidth = record.jobscores[i]["jobscore"] * 100 / record.score;
      scoreList.push([scoreWidth, i]);
    }

    scoreList.sort(function(left, right) {
      return left[0] > right[0] ? -1 : 1;
    });

    // Traverse ordered list
    for (var jobIndex = 0;  jobIndex < scoreList.length; jobIndex++) {

      var width = scoreList[jobIndex][0];
      var index = scoreList[jobIndex][1];

      // Skip after jobLimit jobs are captured or when width is 0.
      if (jobIndex >= jobLimit || width == 0) {
        break;
      }

      var jobDefUrl = record.jobscores[index]['jobdefurl'];
      //var jobLink = "/jobhistory?job-def-id=" + encodeURIComponent(jobDefUrl);
      var jobExecUrl = record.jobscores[index]['jobexecurl'];
      var jobRef = document.createElement("a");
      jobRef.setAttribute("href", jobExecUrl);
      jobRef.appendChild(document.createTextNode("Job " + (jobDefList.indexOf(jobDefUrl) + 1)));

      var tableCell1 = document.createElement("td");
      tableCell1.style.padding = "3px";
      tableCell1.style.border = "none";
      tableCell1.setAttribute("width", "65px");
      tableCell1.appendChild(jobRef);

      var jobScoreRect = document.createElement("div");
      jobScoreRect.style.padding = "3px";
      jobScoreRect.style.background = "red";
      jobScoreRect.style.width = width + "%";
      jobScoreRect.appendChild(document.createTextNode(+width.toFixed(2) + "%"));

      var tableCell2 = document.createElement("td");
      tableCell2.style.border = "none";
      tableCell2.appendChild(jobScoreRect);

      var tableRow = document.createElement("tr");
      tableRow.appendChild(tableCell1);
      tableRow.appendChild(tableCell2);

      jobTable.appendChild(tableRow);
    }

    jobTable.setAttribute("border", "2px solid black");
    jobTable.style.width = "100%";
  }

  content.appendChild(heading);
  content.appendChild(details);
  content.appendChild(jobTable);
  return content;
}