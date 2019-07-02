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
function plotter(data , jobDefList) {

  var lastEle = data[data.length-1];

  data.pop();

  var lastEle2 = data[data.length-1];

  data.pop();

  var lastEle3 = data[data.length-1];

  data.pop();

  var graphContainer = d3.select("#visualisation");
  var MARGINS = {top: 50, right: 50, bottom: 100, left: 50},
      WIDTH = graphContainer.style("width").replace("px", ""),
      HEIGHT = graphContainer.style("height").replace("px", ""),
      GRAPH_WIDTH = WIDTH - MARGINS.left - MARGINS.right,
      GRAPH_HEIGHT = HEIGHT - MARGINS.top - MARGINS.bottom;
  var parseDate = d3.time.format("%Y-%m-%d %H:%M:%S").parse;

  data.forEach(function(d) {
    d.createdTs = parseDate(d.createdTs);});

  var xScale = d3.time.scale()
          .range([2*MARGINS.left, GRAPH_WIDTH-MARGINS.right])
          .domain(d3.extent(data, function(d) { return d.createdTs; })),

      yScale = d3.scale.linear().range([MARGINS.top + GRAPH_HEIGHT, MARGINS.top+30])
          .domain([0, Math.max(d3.max(data, function (d) { return Math.max(d.inputSizeInBytes) }),d3.max(data, function (d) { return Math.max(d.resourceused) }))]);

  var yScaleRight = d3.scale.linear().range([MARGINS.top + GRAPH_HEIGHT, MARGINS.top+30 ]).domain([d3.min(data, function (d) { return Math.min(d.executionTime) }),
    d3.max(data, function (d) { return Math.max(d.executionTime) })]);

  var customTimeFormat = d3.time.format("%b-%d %I:%M:%S");


  var xAxis = d3.svg.axis()
          .scale(xScale)
          .orient("bottom")
          .ticks(5)
          .tickFormat(customTimeFormat)
          .tickSize(1),

      yAxis = d3.svg.axis()
          .scale(yScale)
          .orient("left")
          .ticks(5)
// .tickFormat(function(d) {
//             if((d/(1024*3600))>100.0) {
//                 return d3.round(d/(1024*3600),0);        // convert to GB Hours with 0 decimal places for large numbers
//             } else {
//                 return d3.round(d/(1024*3600),2);       // convert to GB Hours with 2 decimal places for small numbers
//             }
//         })
  ;

  var yAxisRight = d3.svg.axis()
      .scale(yScaleRight)
      .orient("right")
      .ticks(5);


  graphContainer.append("svg:g")
      .attr("class", "x axis")
      .attr("transform", "translate(0 ," + (HEIGHT - MARGINS.bottom) + ")")
      .call(xAxis)
      .selectAll("text")
      .style("text-anchor","end")
      .attr("dx", "-.8em")
      .attr("dy", ".15em")
      .attr("transform","rotate(-35)");

  graphContainer.append("svg:g")
      .attr("class", "y axis")
      .attr("transform", "translate(" + (MARGINS.left) + ", 0)")
      .call(yAxis)
      .selectAll("text")
      .attr("fill", "rgb(0, 119, 181)");

  graphContainer.append("svg:g")
      .attr("class", "y axis")
      .attr("transform", "translate(" + (GRAPH_WIDTH) + ", 0)")
      .call(yAxisRight)
      .attr("id", "ExecTimeAxis")
      .selectAll("text")
      .attr("fill", "rgb(0, 119, 181)");


  //specify meaning of graphs

  graphContainer.append("svg:text")
      .style("font-size", "16px")
      .style("fill", "#006060")
      .attr("transform", "translate(" + (MARGINS.left/10) + ", " + MARGINS.top + ")")
      .text("ResourcesUsed");


  graphContainer.append("svg:text")
      .attr("id","ExecText")
      .style("font-size", "16px")
      .style("fill", "#006060")
      .attr("transform", "translate(" + (GRAPH_WIDTH - MARGINS.left/10) + ", " + MARGINS.top + ")")
      .text("Execution Time");


  // Add the small rectangles to specify the graph meaning
  graphContainer.append("rect")
      .attr("x", GRAPH_WIDTH - 18)
      .attr("width", 14)
      .attr("height", 14)
      .style("fill", 'blue' );

  graphContainer.append("text")
      .attr("x", GRAPH_WIDTH - 26)
      .attr("y", 9)
      .attr("dy", ".30em")
      .style("text-anchor", "end")
      .text(function(d) { return "Resource Usage" });

  graphContainer.append("rect")
      .attr("x", GRAPH_WIDTH - 18)
      .attr("y", 20)
      .attr("width", 14)
      .attr("height", 14)
      .style("fill", 'red' );

  graphContainer.append("text")
      .attr("x", GRAPH_WIDTH - 26)
      .attr("y", 29)
      .attr("dy", ".30em")
      .style("text-anchor", "end")
      .text(function(d) { return "InputSizeInBytes" });

  graphContainer.append("rect")
      .attr("x", GRAPH_WIDTH - 18)
      .attr("y", HEIGHT - 13)
      .attr("width", 14)
      .attr("height", 14)
      .style("fill", 'green' )
      .on("click", function(){
        // Determine if current line is visible
        var active   = ExecTime.active ? false : true,
            newOpacity = active ? 0 : 1;
        // Hide or show the elements
        d3.select("#ExecTime").style("opacity", newOpacity);
        d3.select("#ExecDots").style("opacity", newOpacity);
        d3.select("#ExecTimeAxis").style("opacity", newOpacity);
        d3.select("#ExecText").style("opacity", newOpacity);
        // Update whether or not the elements are active
        ExecTime.active = active;
      });

  graphContainer.append("text")
      .attr("x", GRAPH_WIDTH - 30)
      .attr("y", HEIGHT - 5)
      .attr("dy", ".30em")
      .style("text-anchor", "end")
      .on("click", function(){
        // Determine if current line is visible
        var active   = ExecTime.active ? false : true,
            newOpacity = active ? 0 : 1;
        // Hide or show the elements
        d3.select("#ExecTime").style("opacity", newOpacity);
        d3.select("#ExecDots").style("opacity", newOpacity);
        d3.select("#ExecTimeAxis").style("opacity", newOpacity);
        d3.select("#ExecText").style("opacity", newOpacity);
        // Update whether or not the elements are active
        ExecTime.active = active;
      })
      .text(function(d) { return "Execution Time" });


  var lineGen = d3.svg.line()
      .x(function(d) { return xScale(d.createdTs); })
      .y(function(d) {
        return yScale(d.resourceused);
      })
      .interpolate('linear');

  var lineGenInputSize = d3.svg.line()
      .x(function(d){ return xScale(d.createdTs); })
      .y(function(d) {
        return yScale(d.inputSizeInBytes);
      })
      .interpolate('linear');

  var lineGenExecTime = d3.svg.line()
      .x(function(d){ return xScale(d.createdTs); })
      .y(function(d) {
        return yScaleRight(d.executionTime);
      })
      .interpolate('linear');



  var div = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);


  graphContainer.append("svg:g")
      .selectAll("scatter-dots")
      .data(data)
      .enter().append("svg:circle")
      .style({stroke: 'white', fill: 'blue'})
      .attr("cx", function (d) { return xScale(d.createdTs); } )
      .attr("cy", function (d) { return yScale(d.resourceused); } )
      .attr("r", 7)

      .on("mouseover", function(d) {
        div.transition()
            .duration(50)
            .style("opacity", .9);
        div.html(d.jobExecutionId)
            .style('top', (d3.event.pageY + 10)+'px')
            .style('left', (d3.event.pageX + 10)+'px');
        ;
      })
      .on("mouseout", function(d) {
        div.transition()
            .duration(500)
            .style("opacity", 0);
      });

  graphContainer.append("svg:g")
      .selectAll("scatter-dots")
      .data(data)
      .enter().append("svg:circle")
      .style({stroke: 'white', fill: 'red'})
      .attr("cx", function (d) { return xScale(d.createdTs); } )
      .attr("cy", function (d) { return yScale(d.inputSizeInBytes); } )
      .attr("r", 5);


  graphContainer.append("svg:g")
      .attr("id","ExecDots")
      .selectAll("scatter-dots")
      .data(data)
      .enter().append("svg:circle")
      .style({stroke: 'white', fill: 'green'})
      .attr("cx", function (d) { return xScale(d.createdTs); } )
      .attr("cy", function (d) { return yScaleRight(d.executionTime); } )
      .attr("r", 5);


//plot linear graphs

  graphContainer.append('svg:path')
      .attr('d', lineGen(data))
      .attr('stroke', 'blue')
      .attr('stroke-width', 3.5)
      .attr('fill', 'none');


  graphContainer.append('svg:path')
      .attr('d', lineGenInputSize(data))
      .attr('stroke', 'red')
      .attr('stroke-width', 2)
      .attr('fill', 'none');

  graphContainer.append('svg:path')
      .attr('d', lineGenExecTime(data))
      .attr("id", "ExecTime")
      .attr('stroke', 'green')
      .attr('stroke-width', 1)
      .attr('fill', 'none');


  //Mark the date when autotuning is enabled for the first time

  graphContainer.append('svg:line')
      .style('stroke', 'black')
      .attr('stroke-width', 1)
      .attr('x1', xScale(parseDate(lastEle.createdTs)))
      .attr('y1', MARGINS.top-5)
      .attr('x2', xScale(parseDate(lastEle.createdTs)))
      .attr('y2', MARGINS.top+ GRAPH_HEIGHT);

  graphContainer.append("text")
      .attr('x', xScale(parseDate(lastEle.createdTs)))
      .attr('y', MARGINS.top - 10)
      .attr("text-anchor", "middle")
      .style("font-size", "15px")
      .text("Autotuning enabled");

  graphContainer.append("text")
      .attr('x', xScale(parseDate(lastEle2.createdTs)))
      .attr('y', MARGINS.top + 3 )
      .attr("text-anchor", "middle")
      .style("font-size", "15px")
      .text("Best Parameter Set");



  graphContainer.append('svg:line')
      .style('stroke', 'black')
      //  .data(lastEle)
      .attr('stroke-width', 1)
      .attr('x1', xScale(parseDate(lastEle2.createdTs)))
      .attr('y1', MARGINS.top+7)
      .attr('x2', xScale(parseDate(lastEle2.createdTs)))
      .attr('y2', MARGINS.top + GRAPH_HEIGHT);

  graphContainer.append("text")
      .attr('x',2*MARGINS.left )
      .attr('y', HEIGHT-5)
      .attr("text-anchor", "middle")
      .style("font-size", "16px")
      .text(function(d){
        if(lastEle3.Autotuning== true)
          return "";
        else
          return "Autotuning Disabled after Best Param Set";
      });

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//Plot for parameters //
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  for (var i =0 ;i <=7 ;i++){

    var yParam = d3.scale.linear().range([MARGINS.top + GRAPH_HEIGHT, MARGINS.top+40]).domain([0,
      d3.max(data, function (d) { return Math.max(d.suggestedParameters[i].parameterValue) })]);


    var yAxisParam = d3.svg.axis()
        .scale(yParam)
        .orient("left")
        .ticks(5);

    var width = 900;
    var height = 350;

    //Create SVG element
    var paramcontainer1 = d3.select("#parameter")
        .append("svg")
        .attr("width", width)
        .attr("height", height);


// var paramcontainer1= d3.select("#parameters");

    paramcontainer1.append("svg:g")
        .attr("class", "y axis")
        .attr("transform", "translate(" + (MARGINS.left) + ", 0)")
        .call(yAxisParam)
        .attr("id", "ExecTimeAxis")
        .selectAll("text")
        .attr("fill", "rgb(0, 119, 181)");


    paramcontainer1.append("svg:g")
        .attr("class", "x axis")
        .attr("transform", "translate(0 ," + (HEIGHT - MARGINS.bottom) + ")")
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor","end")
        .attr("dx", "-.8em")
        .attr("dy", ".15em")
        .attr("transform","rotate(-25)");

    var lineGenParam = d3.svg.line()
        .x(function(d){ return xScale(d.createdTs); })
        .y(function(d) {if(Object.keys(d.suggestedParameters).length<= i){return 0;}
        else
        { return yParam(d.suggestedParameters[i].parameterValue);}
        });


    paramcontainer1.append('svg:path')
        .attr('d', lineGenParam(data))
        .attr('stroke', 'blue')
        .attr('stroke-width', 1)
        .attr('fill', 'none');
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

// /* Update execution table with time in user timezone */
// function updateExecTimezone(data) {
//   var parse = d3.time.format("%b %d, %Y %I:%M %p");
//   var time = document.querySelectorAll('.exectime');
//   for (var i = time.length - 1; i >= 0; i--) {
//     time[i].innerHTML = parse(new Date(data[time.length - 1 - i].flowtime));
//   }
// }