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

    //var details = document.createElement("p");
    //details.appendChild(document.createTextNode("Job Score = " + record.totalruntime));

    var runtimeTable = document.createElement("table");
    if (record.runtime != 0) {
        var jobLimit = 3;

        var tableHeader = document.createElement("th");
        tableHeader.setAttribute("colspan", "2");
        tableHeader.style.padding = "3px";
        tableHeader.style.textAlign = "center";
        tableHeader.style.width = "100%";
        tableHeader.appendChild(document.createTextNode("Time"));
        runtimeTable.appendChild(tableHeader);

        var maxRuntime = 0;
        for (var i = 0; i < record.jobmetrics.length; i++) {
            if(record.jobmetrics[i]["runtime"]> maxRuntime) {
                maxRuntime = record.jobmetrics[i]["runtime"]
            }
        }

        // add total runtime
        var tableCell1 = document.createElement("td");
        tableCell1.style.padding = "3px";
        tableCell1.style.border = "none";
        tableCell1.setAttribute("width", "90px");
        tableCell1.appendChild(document.createTextNode("Run Time (hh:mm:ss)"));

        var stageScoreRect = document.createElement("div");
        stageScoreRect.style.padding = "3px";
        stageScoreRect.style.background = "#0077b5";
        stageScoreRect.style.width = "100%";
        stageScoreRect.appendChild(document.createTextNode(msToHMS(record.runtime)));
        console.log(record.runtime);

        var tableCell2 = document.createElement("td");
        tableCell2.style.border = "none";
        tableCell2.appendChild(stageScoreRect);

        var tableRow = document.createElement("tr");
        tableRow.appendChild(tableCell1);
        tableRow.appendChild(tableCell2);

        runtimeTable.appendChild(tableRow);

        // add total waittime
        var tableCell3 = document.createElement("td");
        tableCell3.style.padding = "3px";
        tableCell3.style.border = "none";
        tableCell3.setAttribute("width", "65px");
        tableCell3.appendChild(document.createTextNode("Wait Time (hh:mm:ss)"));

        var waittimepercent = (record.waittime/ record.runtime) * 100;

        var stageScoreRect2 = document.createElement("div");
        stageScoreRect2.style.padding = "3px";
        stageScoreRect2.style.background = "red";
        stageScoreRect2.style.width = (Math.floor(waittimepercent+1)) + "%";
        stageScoreRect2.appendChild(document.createTextNode(msToHMS(record.waittime) + "(" + Math.floor(waittimepercent) + "%)"));

        console.log(record.waittime + "(" + waittimepercent + "%)");

        var tableCell4 = document.createElement("td");
        tableCell4.style.border = "none";
        tableCell4.appendChild(stageScoreRect2);

        var tableRow2 = document.createElement("tr");
        tableRow2.appendChild(tableCell3);
        tableRow2.appendChild(tableCell4);

        runtimeTable.appendChild(tableRow2);

        runtimeTable.setAttribute("border", "2px solid black");
        runtimeTable.style.width = "100%";
    }

    content.appendChild(heading);
    content.appendChild(runtimeTable);
    content.style.padding = "0";
    return content;
}

function msToHMS( ms ) {
    // Convert to seconds:
    var seconds = ms / 1000;
    // Extract hours:
    var hours = parseInt( seconds / 3600 );
    seconds = seconds % 3600;
    // Extract minutes:
    var minutes = parseInt( seconds / 60 );
    // Keep only seconds not extracted to minutes:
    seconds = parseInt(seconds % 60);

    return  hours+":"+minutes+":"+seconds;
}