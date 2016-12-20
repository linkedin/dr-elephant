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

/* Show loading sign during ajax call */
$(document).ajaxStart(function() {
    $("#loading-indicator").show();
});

$(document).ajaxStop(function() {
    $("#loading-indicator").hide();
});

/* Plot the performance graph for the data */
function plotter(graphData, jobDefList) {

    graphData.forEach(function(d) { d.flowtime = new Date(d.flowtime); });

    var graphContainer = d3.select("#visualisation");

    /////////// DEFINE THE GRAPH ATTRIBUTES /////////////

    // Define the Margins for the GRAPH Dimensions
    var MARGINS = {top: 50, right: 50, bottom: 100, left: 50},
        WIDTH = graphContainer.style("width").replace("px", ""),
        HEIGHT = graphContainer.style("height").replace("px", ""),
        GRAPH_WIDTH = WIDTH - MARGINS.left - MARGINS.right,
        GRAPH_HEIGHT = HEIGHT - MARGINS.top - MARGINS.bottom;

    // Set the domain of x
    var millisDay = 86400000; // Offset to the domain. Also makes a single execution to be at the center.
    var xRange = d3.time.scale().range([MARGINS.left, MARGINS.left + GRAPH_WIDTH])
        .domain([
            d3.min(graphData, function (d) { return Math.min(d.flowtime) - millisDay/2}),
            d3.max(graphData, function (d) { return Math.max(d.flowtime) + millisDay/2})
        ]);

    // Set the domain of y
    var yRange = d3.scale.linear().range([MARGINS.top + GRAPH_HEIGHT, MARGINS.top])
        .domain([0, d3.max(graphData, function (d) { return d.resourceused + d.resourceused/5; })])
        .nice(5);                                                    // Ensures a nice round value at the end of y axis

    // The graph function
    var lineFunc = d3.svg.line()
        .x(function (d) { return xRange(d.flowtime); })
        .y(function (d) { return yRange(d.resourceused); })
        .interpolate('linear');



    var lineWastedFunc = d3.svg.line()
        .x(function (d) { return xRange(d.flowtime); })
        .y(function (d) { return yRange(d.resourcewasted); })
        .interpolate('linear');

    /*
     var customTimeFormat = d3.time.format.multi([
     [".%L", function(d) { return d.getMilliseconds(); }],
     [":%S", function(d) { return d.getSeconds(); }],
     ["%I:%M", function(d) { return d.getMinutes(); }],
     ["%I %p", function(d) { return d.getHours(); }],
     ["%a %d", function(d) { return d.getDay() && d.getDate() != 1; }],
     ["%b %d", function(d) { return d.getDate() != 1; }],
     ["%B", function(d) { return d.getMonth(); }],
     ["%Y", function() { return true; }]
     ]);
     */

    var customTimeFormat = d3.time.format("%Y-%b-%d");

    // x-axis definition
    var xAxis = d3.svg.axis()
        .scale(xRange)
        .tickSize(0)
        .orient("bottom")
        .ticks(9)
        .tickFormat(customTimeFormat);

    // y-axis definition
    var yAxis = d3.svg.axis()
        .scale(yRange)
        //.tickFormat(function(d) { return d +"GB Hours"})
        //.tickSize(-1 * (GRAPH_WIDTH))                              // Adds horizontal lines in the graph
        .ticks(5)                                                    // Set 5 levels (5 horizontal lines)
        .tickFormat(function(d) {
            if((d/(1024*3600))>100.0) {
                return d3.round(d/(1024*3600),0);        // convert to GB Hours with 0 decimal places for large numbers
            } else {
                return d3.round(d/(1024*3600),2);       // convert to GB Hours with 2 decimal places for small numbers
            }
        })
        .orient("left");

    /////////// ADD CONTENTS TO THE GRAPH CONTAINER /////////////

    // add the x axis
    graphContainer.append("svg:g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + (HEIGHT - MARGINS.bottom) + ")")
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor","end")
        .attr("dx", "-.8em")
        .attr("dy", ".15em")
        .attr("transform","rotate(-65)");

    // Add the y-axis
    graphContainer.append("svg:g")
        .attr("class", "y axis")
        .attr("transform", "translate(" + (MARGINS.left) + ", 0)")
        .call(yAxis)
        .selectAll("text")
        .attr("fill", "rgb(0, 119, 181)");

    // Add label for the y axis
    graphContainer.append("svg:text")
        .style("font-size", "16px")
        .style("fill", "#606060")
        .attr("transform", "translate(" + (MARGINS.left/10) + ", " + MARGINS.top/2 + ")")
        .text("Resources(GB Hours)");

    // Add the graph function
    graphContainer.append("svg:path")
        .attr("d", lineFunc(graphData))
        .attr("stroke", "#0077b5")
        .attr("stroke-width", 1.5)
        .attr("fill", "none");

    graphContainer.append("svg:path")
        .attr("d", lineWastedFunc(graphData))
        .attr("stroke", "#FF0000")
        .attr("stroke-width", 1.5)
        .attr("fill", "none");


    // Add the small rectangles to specify the graph meaning
    graphContainer.append("rect")
        .attr("x", GRAPH_WIDTH - 18)
        .attr("width", 14)
        .attr("height", 14)
        .style("fill", "#0077b5" );

    graphContainer.append("text")
        .attr("x", GRAPH_WIDTH - 26)
        .attr("y", 9)
        .attr("dy", ".30em")
        .style("text-anchor", "end")
        .text(function(d) { return "Used Resources" });

    graphContainer.append("rect")
        .attr("x", GRAPH_WIDTH - 18)
        .attr("y", 20)
        .attr("width", 14)
        .attr("height", 14)
        .style("fill", "#FF0000" );

    graphContainer.append("text")
        .attr("x", GRAPH_WIDTH - 26)
        .attr("y", 29)
        .attr("dy", ".30em")
        .style("text-anchor", "end")
        .text(function(d) { return "Wasted Resources" });


    // Add the small bubble dots on the graph line
    graphContainer.append("svg:g")
        .selectAll("scatter-dots")
        .data(graphData)
        .enter().append("svg:circle")
        .style({stroke: 'white', fill: '#0077b5'})
        .attr("cx", function (d) { return xRange(d.flowtime); } )
        .attr("cy", function (d) { return yRange(d.resourceused); } )
        .attr("r", 4);

    graphContainer.append("svg:g")
        .selectAll("scatter-dots")
        .data(graphData)
        .enter().append("svg:circle")
        .style({stroke: 'white', fill: '#FF0000'})
        .attr("cx", function (d) { return xRange(d.flowtime); } )
        .attr("cy", function (d) { return yRange(d.resourcewasted); } )
        .attr("r", 4);

    /////////// THE TOOLTIPS FOR THE GRAPH /////////////

    // Add a transparent rectangle on top of the graph area to compute x-value mouse over
    graphContainer.append("svg:rect")
        .attr("class", "overlay")
        .attr("width", GRAPH_WIDTH)
        .attr("height", GRAPH_HEIGHT)
        .attr("transform", "translate(" + (MARGINS.left) + ", " + (MARGINS.top) + ")")
        .attr("opacity", 0)
        .on("mouseover", function() { tooltip.style("display", null); })    // Reset tooltip display (default value)
        .on("mousemove", mousemove);                                        // Compute position and show the tooltip

    // The tooltip container (Top of the stack)
    var tooltip = graphContainer.append("svg:g");

    // Add the highlight bubble
    var highlightCircleRad = 7;
    tooltip.append("svg:circle")
        .attr("stroke", "white")
        .attr("fill", "#0077b5")
        .attr("r", highlightCircleRad)
        .style("display", "none");

    tooltip.append("svg:circle")
        .attr("stroke", "white")
        .attr("fill", "#FF0000")
        .attr("r", highlightCircleRad)
        .style("display", "none").attr("class","bluecircle");

    // Add the tooltip
    var tooltipWidth = 260;
    tooltip.append("foreignObject")
        .attr("width", tooltipWidth + "px")
        .append("xhtml:body")
        .attr("id", "graph_tooltip")
        .style("font-size", "12px")
        .attr("class","graphColor")
        .style("text-align", "center")
        .style("border-radius", "5px")
        .style("padding", "5px")
        .style("border", "1.5px solid black");

    var bisectExec = d3.bisector(function(d) { return d.flowtime; }).left;

    function mousemove(d) {

        // Compute tooltip to be shown depending on mouse position
        var record;
        if (graphData.length == 1) {
            record = graphData[0];
        } else {
            var xValueMouse = xRange.invert(MARGINS.left + d3.mouse(this)[0]),
                index = bisectExec(graphData, xValueMouse, 1),
                dleft = graphData[index - 1],
                dright = graphData[index];
            record = xValueMouse - dleft.flowtime > dright.flowtime - xValueMouse ? dright : dleft;
        }

        // Add content to tooltip
        var graphTooltip = document.getElementById("graph_tooltip");
        graphTooltip.innerHTML = '';
        graphTooltip.appendChild(getGraphTooltipContent(record, jobDefList));

        // Set position of highlighted circle
        tooltip.select("circle")
            .style("display", "inline")
            .attr("transform", "translate(" + xRange(record.flowtime) + "," + yRange(record.resourceused) +")");

        tooltip.select("circle.bluecircle")
            .style("display", "inline")
            .attr("transform", "translate(" + xRange(record.flowtime) + "," + yRange(record.resourcewasted) +")");

        // Set position of tooltip.
        var x = xRange(record.flowtime) - (tooltipWidth) - 10;
        var y = yRange(record.resourceused) - tooltip.select("body").style("height").replace("px", "")/2;

        // Don't let the tooltip cross the left margin
        if (x < MARGINS.left) {
            x = xRange(record.flowtime) + 10;
        }

        // Don't let the tooltip cross the bottom margin
        if ((yRange(record.resourceused) + tooltip.select("body").style("height").replace("px", "")/2) >= yRange(0)) {
            y = yRange(record.resourceused) - tooltip.select("body").style("height").replace("px", "") - 10;
        }

        tooltip.select("foreignObject")
            .attr("height", tooltip.select("body").style("height"));
        tooltip.select("foreignObject")
            .transition()
            .duration(75)
            .attr("transform", "translate(" + x + "," + y + ")");
    }
}

/* Return the query parameters */
function queryString() {

    var query_string = {};
    var query = window.location.search.substring(1);   // Returns the query parameters excluding ?
    var vars = query.split("&");

    for (var i = 0; i < vars.length; i++) {
        var pair = vars[i].split("=");
        if (typeof query_string[pair[0]] === "undefined") {
            query_string[pair[0]] = pair[1];
        }
    }
    return query_string;
}

/* Update tooltip position on mouse-move over table */
function loadTableTooltips() {

    var tooltipDiv = document.querySelectorAll('.hasTooltip div');
    window.onmousemove = function (e) {
        var x = e.clientX,
            y = e.clientY;

        for (var i = 0; i < tooltipDiv.length; i++) {
            tooltipDiv[i].style.top = (y - tooltipDiv[i].offsetHeight - 10)+ "px";
            tooltipDiv[i].style.left = (x + 10) + "px";
        }
    };
}

/* Update execution table with time in user timezone */
function updateExecTimezone(data) {
    var parse = d3.time.format("%b %d, %Y %I:%M %p");
    var time = document.querySelectorAll('.exectime');
    for (var i = time.length - 1; i >= 0; i--) {
        time[i].innerHTML = parse(new Date(data[time.length - 1 - i].flowtime));
    }
}