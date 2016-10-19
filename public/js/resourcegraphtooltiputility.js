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

function getGraphTooltipContent(record, jobDefList) {

  var content = document.createElement("div");
  content.style.textAlign = "center";

  var heading = document.createElement("b");
  heading.appendChild(document.createTextNode(record.flowtime));
  heading.appendChild(document.createElement("br"));

  var resourcesTable = document.createElement("table");
  if (record.resourceused != 0) {
    var jobLimit = 3;

    var tableHeader = document.createElement("th");
    tableHeader.setAttribute("colspan", "2");
    tableHeader.style.padding = "3px";
    tableHeader.style.textAlign = "center";
    tableHeader.style.width = "100%";
    tableHeader.appendChild(document.createTextNode("Resources"));
    resourcesTable.appendChild(tableHeader);

    // add total used resources
    var tableCell1 = document.createElement("td");
    tableCell1.style.padding = "3px";
    tableCell1.style.border = "none";
    tableCell1.setAttribute("width", "90px");
    tableCell1.appendChild(document.createTextNode("Used (GB Hrs)"));

    var stageScoreRect = document.createElement("div");
    stageScoreRect.style.padding = "3px";
    stageScoreRect.style.background = "#0077b5";
    stageScoreRect.style.width = "100%";
    stageScoreRect.appendChild(document.createTextNode(parseFloat(Math.round((record.resourceused)/(1024*3600)*100)/100).toFixed(2)));
    console.log(record.resourceused);

    var tableCell2 = document.createElement("td");
    tableCell2.style.border = "none";
    tableCell2.appendChild(stageScoreRect);

    var tableRow = document.createElement("tr");
    tableRow.appendChild(tableCell1);
    tableRow.appendChild(tableCell2);

    resourcesTable.appendChild(tableRow);

    // add total resourcewasted
    var tableCell3 = document.createElement("td");
    tableCell3.style.padding = "3px";
    tableCell3.style.border = "none";
    tableCell3.setAttribute("width", "65px");
    tableCell3.appendChild(document.createTextNode("Wasted (GB Hrs)"));

    var resourcewastedpercent = (record.resourcewasted/ record.resourceused) * 100;

    var stageScoreRect2 = document.createElement("div");
    stageScoreRect2.style.padding = "3px";
    stageScoreRect2.style.background = "red";
    stageScoreRect2.style.width = (Math.floor(resourcewastedpercent+1)) + "%";
    stageScoreRect2.appendChild(document.createTextNode(parseFloat(Math.round(record.resourcewasted/(1024*3600) * 100)/100).toFixed(2) + "(" + Math.floor(resourcewastedpercent) + "%)"));

    console.log(record.resourcewasted + "(" + resourcewastedpercent + "%)");

    var tableCell4 = document.createElement("td");
    tableCell4.style.border = "none";
    tableCell4.appendChild(stageScoreRect2);

    var tableRow2 = document.createElement("tr");
    tableRow2.appendChild(tableCell3);
    tableRow2.appendChild(tableCell4);

    resourcesTable.appendChild(tableRow2);

    resourcesTable.setAttribute("border", "2px solid black");
    resourcesTable.style.width = "100%";
  }

  content.appendChild(heading);
  content.appendChild(resourcesTable);
  content.style.padding = "0";
  return content;
}