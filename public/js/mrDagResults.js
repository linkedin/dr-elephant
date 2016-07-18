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
//var nodes = new vis.DataSet();
//var edges = new vis.DataSet();
//var g = {nodes: nodes, edges: edges}

function draw(adjMatrixKaPapa) {
   /* for(var i in adjMatrixKaPapa){
        var info= adjMatrixKaPapa[i];
        var o = {id: i.toString(), label: info["label"], x: 0, y: 0, size: 10,color: info["colour"], originalColor: 'blue'};
        nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: '#808080'};
            edges.add(e);

        }
    }

    console.log(nodes);
    console.log(edges);

    container = document.getElementById("mr_dag_container");
    options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'dot', scaling: {min:5, max:30}, font: {size: 12, face: "Tahoma"}}
        // physics: {hierarchicalRepulsion: {springConstant: 0.01}},
        //physics: {barnesHut: {centralGravity: 0.1, avoidOverlap: 0.5, gravitationalConstant: -2500}, maxVelocity: 25}
        // layout:{hierarchical: {enabled: true}}
    }
    network = new vis.Network(container, g, options);*/
    /*var nodes = new vis.DataSet([
        {id: 1, label: 'Node 1', title: 'I have a popup!'},
        {id: 2, label: 'Node 2', title: 'I have a popup!'},
        {id: 3, label: 'Node 3', title: 'I have a popup!'},
        {id: 4, label: 'Node 4', title: 'I have a popup!'},
        {id: 5, label: 'Node 5', title: 'I have a popup!'}
    ]);

    // create an array with edges
    var edges = new vis.DataSet([
        {from: 1, to: 3},
        {from: 1, to: 2},
        {from: 2, to: 4},
        {from: 2, to: 5}
    ]);

    // create a network
    var container = document.getElementById('mr_dag_container');
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {interaction:{hover:true}};
    network = new vis.Network(container, data, options);*/



    // create an array with nodes
    var nodes = new vis.DataSet([
        {id: 1, label: 'Node 1', title: 'I have a popup!'},
        {id: 2, label: 'Node 2', title: 'I have a popup!'},
        {id: 3, label: 'Node 3', title: 'I have a popup!'},
        {id: 4, label: 'Node 4', title: 'I have a popup!'},
        {id: 5, label: 'Node 5', title: 'I have a popup!'}
    ]);

    // create an array with edges
    var edges = new vis.DataSet([
        {from: 1, to: 3},
        {from: 1, to: 2},
        {from: 2, to: 4},
        {from: 2, to: 5}
    ]);

    // create a network
    var container = document.getElementById('mr_dag_container');
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {interaction:{hover:true}};
    var network = new vis.Network(container, data, options);

    network.on("click", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>Click event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("doubleClick", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>doubleClick event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("oncontext", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>oncontext (right click) event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("dragStart", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>dragStart event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("dragging", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>dragging event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("dragEnd", function (params) {
        params.event = "[original event]";
        document.getElementById('eventSpan').innerHTML = '<h2>dragEnd event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("zoom", function (params) {
        document.getElementById('eventSpan').innerHTML = '<h2>zoom event:</h2>' + JSON.stringify(params, null, 4);
    });
    network.on("showPopup", function (params) {
        console.log("POPUP EVENT HAPPENINg");
        //document.getElementById('eventSpan').innerHTML = '<h2>showPopup event: </h2>' + JSON.stringify(params, null, 4);
    });
    network.on("hidePopup", function () {
        console.log('hidePopup Event');
    });
    network.on("select", function (params) {
        console.log('select Event:', params);
    });
    network.on("selectNode", function (params) {
        console.log('selectNode Event:', params);
    });
    network.on("selectEdge", function (params) {
        console.log('selectEdge Event:', params);
    });
    network.on("deselectNode", function (params) {
        console.log('deselectNode Event:', params);
    });
    network.on("deselectEdge", function (params) {
        console.log('deselectEdge Event:', params);
    });
    network.on("hoverNode", function (params) {
        console.log('hoverNode Event:', params);
    });
    network.on("hoverEdge", function (params) {
        console.log('hoverEdge Event:', params);
    });
    network.on("blurNode", function (params) {
        console.log('blurNode Event:', params);
    });
    network.on("blurEdge", function (params) {
        console.log('blurEdge Event:', params);
    });



}

$(document).ready(function(){


    $.getJSON('/rest/mrdaggraphdata?id=' + queryString()['job-exec-id'], function(data) {
        console.log(data);
        if (data.size != 0) {
            draw(data);
            network.redraw();
        }
    });
});