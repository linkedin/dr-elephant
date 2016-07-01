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

function draw(adjMatrixKaPapa) {
    for(var i in adjMatrixKaPapa){
        var info= adjMatrixKaPapa[i];
        var o = {id: i.toString(), label: info["label"], x: 0, y: 0, size: info["time"],color: info["colour"], originalColor: 'blue'};
        nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: '#808080'};
            edges.add(e);

        }
    }

    container = document.getElementById("dag_container");
    options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'dot', scaling: {min:5, max:30}, font: {size: 12, face: "Tahoma"}}
        // physics: {hierarchicalRepulsion: {springConstant: 0.01}},
        //physics: {barnesHut: {centralGravity: 0.1, avoidOverlap: 0.5, gravitationalConstant: -2500}, maxVelocity: 25}
        // layout:{hierarchical: {enabled: true}}
    }
    network = new vis.Network(container, g, options);
}

$(document).ready(function(){


    $.getJSON('/rest/daggraphdata?id=' + queryString()['flow-exec-id'], function(data) {
        if (data.size != 0) {
            draw(data);
            network.redraw();
        }
    });
});