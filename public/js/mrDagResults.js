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

var network = null;
var nodes = new vis.DataSet();
var edges = new vis.DataSet();
var g = {nodes: nodes, edges: edges}
var edge_colour_default= '#808080';   //gray colour for now

function draw(adjMatrixWrapper) {
  for (var i in adjMatrixWrapper) {
    var info = adjMatrixWrapper[i];
    var o = {id: i.toString(), label: info["label"], x: 0, y: 0, size: 10, color: info["colour"]};
    nodes.add(o);
    for (var j in info["row"]) {

      var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: edge_colour_default};
      edges.add(e);

    }
  }

  container = document.getElementById("mr_dag_container");
  options = {
    edges: {arrows: 'middle'},
    nodes: {shape: 'dot', scaling: {min: 5, max: 30}, font: {size: 12, face: "Tahoma"}},
    layout: {hierarchical: {enabled: true, sortMethod: 'directed'}}
  }
  network = new vis.Network(container, g, options);

}

$(document).ready(function () {

  $.getJSON('/rest/mrdaggraphdata?id=' + queryString()['job-exec-id'], function (data) {
    if (data.size != 0) {
      draw(data);
    }
  });
});