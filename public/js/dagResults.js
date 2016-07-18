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
var network_nested= null;
var nodes = new vis.DataSet();
var edges = new vis.DataSet();
var nested_nodes = new vis.DataSet();
var nested_edges = new vis.DataSet();
var new_nodes = new vis.DataSet();
var new_edges = new vis.DataSet();
var g = {nodes: nodes, edges: edges}
var g1 = {nodes: nested_nodes, edges: nested_edges}
var g2= {nodes: new_nodes, edges: new_edges}
var flowExecId;
var adjMatrix;


function random_dag(){

    var optR= options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'dot', scaling: {min:5, max:20}, font: {size: 12, face: "Tahoma"}},
        physics: {hierarchicalRepulsion: {springConstant: 0.0}},
        interaction: {hover: true, tooltipDelay: 50}
    };
    var network_new = new vis.Network(document.getElementById("dag_container"), g, optR);
    network_new.on("click", function (params) {

        document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
        nested_nodes = new vis.DataSet();
        nested_edges = new vis.DataSet();
        g1 = {nodes: nested_nodes, edges: nested_edges};
        network_nested= null;
        //params.event = "[original event]";
        var selNodes= null;
        var selNodes = network_new.getSelectedNodes();
        selectedNode = nodes.get(selNodes);

        var nested_container= document.getElementById("nested_dag_container");

        document.getElementById("mr_header").innerHTML= ("MR DAG FOR JOB : "+ selectedNode[0]["label"]);


        $.getJSON('/rest/nesteddagdata?id='+flowExecId+'&name='+selectedNode[0]["label"], function(nested_data) {
            console.log(nested_data);
            if (nested_data.size != 0) {
                drawNested(nested_data);
                network_nested.redraw();
                //listenForEvents();
            }
            else{
                drawNested(null);
            }
        });

    });

    network_new.redraw();

}


function rectangles(){
    new_nodes = new vis.DataSet();
    new_edges = new vis.DataSet();
    g2 = {nodes: new_nodes, edges: new_edges};
    network_new= null;
    var height;
    for(var i in adjMatrix){
        var heightStr= "\n";
        var info= adjMatrix[i];
        // console.log(info["title"]);
        height= info["time"];
        console.log(height);
        height= height/2;
        while(height>=0){
            heightStr= heightStr+" \n";
            height= height-1;
        }
        var astr= info["label"];
        var mod_label= astr;
        if(astr.length>100){
            var mod_label= astr.substring(0, 100)+"</br>"+astr.substring(101, astr.length);
        }
        if(astr.length>200) {
            var mod_label = astr.substring(0, 100) + "</br>" + astr.substring(101, 200) + "</br>" + astr.substring(201, astr.length);
        }


        // console.log("this is the string: "+ heightStr);

        var o = {id: i.toString(), label: heightStr, size: info["time"], title: mod_label+"</br>"+info["title"], color: info["colour"], originalColor: 'blue'};
        new_nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: '#808080'};
            new_edges.add(e);

        }
        //   $(i.toString()).addEventListener("click", someFunction);

    }
    var optR= options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'box'},
        physics: {hierarchicalRepulsion: {springConstant: 0.0}},
        interaction: {hover: true},
        layout:{hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 300}}
    };

    var network_new = new vis.Network(document.getElementById("dag_container"), g2, optR);

    network_new.on("click", function (params) {

        document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
        nested_nodes = new vis.DataSet();
        nested_edges = new vis.DataSet();
        g1 = {nodes: nested_nodes, edges: nested_edges};
        network_nested= null;
        var selNodes= null;
        var selNodes = network_new.getSelectedNodes();
        selectedNode = nodes.get(selNodes);

        var nested_container= document.getElementById("nested_dag_container");

        document.getElementById("mr_header").innerHTML= ("MR DAG FOR JOB : "+ selectedNode[0]["label"]);


        $.getJSON('/rest/nesteddagdata?id='+flowExecId+'&name='+selectedNode[0]["label"], function(nested_data) {
            console.log(nested_data);
            if (nested_data.size != 0) {
                drawNested(nested_data);
                network_nested.redraw();
                //listenForEvents();
            }
            else{
                drawNested(null);
            }
        });

    });

    network_new.redraw();

}


function left_right_dag(){
    new_nodes = new vis.DataSet();
    new_edges = new vis.DataSet();
    g2 = {nodes: new_nodes, edges: new_edges};
    network_new= null;
    var height;
    for(var i in adjMatrix){
        var heightStr= " ";
        var info= adjMatrix[i];
        // console.log(info["title"]);
        height= info["time"];
        console.log(height);
        while(height>=0){
            heightStr= heightStr+"  ";
            height= height-1;
        }
        // console.log("this is the string: "+ heightStr);
        var astr= info["label"];
        var mod_label= astr;
        if(astr.length>100){
            var mod_label= astr.substring(0, 100)+"</br>"+astr.substring(101, astr.length);
        }
        if(astr.length>200) {
            var mod_label = astr.substring(0, 100) + "</br>" + astr.substring(101, 200) + "</br>" + astr.substring(201, astr.length);
        }

        var o = {id: i.toString(), label: heightStr, size: info["time"], title: mod_label+"</br>"+info["title"], color: info["colour"], originalColor: 'blue'};
        new_nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: '#808080'};
            new_edges.add(e);

        }
        //   $(i.toString()).addEventListener("click", someFunction);

    }
    var optR= options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'box'},
        // physics: {hierarchicalRepulsion: {springConstant: 0.01}},
        // physics: {barnesHut: {centralGravity: 0.1, avoidOverlap: 0.5, gravitationalConstant: -2500}, maxVelocity: 25},
        physics: {hierarchicalRepulsion: {springConstant: 0.0}},
        interaction: {hover: true},
        layout:{hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 350, direction: 'LR'}}
    };

    var network_new = new vis.Network(document.getElementById("dag_container"), g2, optR);

    network_new.on("click", function (params) {

        document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
        nested_nodes = new vis.DataSet();
        nested_edges = new vis.DataSet();
        g1 = {nodes: nested_nodes, edges: nested_edges};
        network_nested= null;
        //params.event = "[original event]";
        var selNodes= null;
        var selNodes = network_new.getSelectedNodes();
        selectedNode = nodes.get(selNodes);

        var nested_container= document.getElementById("nested_dag_container");

        document.getElementById("mr_header").innerHTML= ("MR DAG FOR JOB : "+ selectedNode[0]["label"]);


        $.getJSON('/rest/nesteddagdata?id='+flowExecId+'&name='+selectedNode[0]["label"], function(nested_data) {
            console.log(nested_data);
            if (nested_data.size != 0) {
                drawNested(nested_data);
                network_nested.redraw();
                //listenForEvents();
            }
            else{
                drawNested(null);
            }
            //console.log(nested_data);
        });

        //document.getElementById("nested_dag_container").innerHTML = '<h1>event.target.id;</h1>'+JSON.stringify(params, null, 4)+params["pointer"]["DOM"]["x"]  +JSON.stringify(selectedNode)+ selectedNode[0]["label"] + flowExecId;

    });

    network_new.redraw();

}


function drawNested(nestedAdjMatrix) {
    //alert ("Hello World!")
    for(var i in nestedAdjMatrix){
        var info= nestedAdjMatrix[i];
        //console.log(info["title"]);

        var o = {id: 'MR'+i.toString(), label: info["label"], size: info["size"], color: info["colour"], originalColor: 'blue', title: info["title"]};
        nested_nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: ('MR'+i.toString()) + "_" + ('MR'+j.toString()), from: ('MR'+i.toString()), to: ('MR'+j.toString()), color: '#808080'};
            nested_edges.add(e);

        }
        //   $(i.toString()).addEventListener("click", someFunction);
    }

    //console.log(nodes);
    //console.log(edges);
    nested_container = document.getElementById("nested_dag_container");
    options = {
        edges: {arrows: 'middle'},
        nodes: {shape: 'dot', scaling: {min:5, max:20}, font: {size: 12, face: "Tahoma"}},
        // physics: {hierarchicalRepulsion: {springConstant: 0.01}},
        // physics: {barnesHut: {centralGravity: 0.1, avoidOverlap: 0.5, gravitationalConstant: -2500}, maxVelocity: 25},
        physics: {hierarchicalRepulsion: {springConstant: 0.0}},
        interaction: {hover: true, tooltipDelay: 50},
        layout:{hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 300}}
    };
    network_nested = new vis.Network(nested_container, g1, options);

}

function draw(adjMatrixKaPapa) {

    adjMatrix= adjMatrixKaPapa;
    for(var i in adjMatrixKaPapa){
        var info= adjMatrixKaPapa[i];
        // console.log(info["title"]);

        var o = {id: i.toString(), label: info["label"], size: info["time"], title: info["title"], color: info["colour"], originalColor: 'blue'};
        nodes.add(o);
        for (var j in info["row"]) {

            var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: '#808080'};
            edges.add(e);

        }

    }
    rectangles();

}


$(document).ready(function(){
    flowExecId= queryString()['flow-exec-id'];
    $.getJSON('/rest/daggraphdata?id=' + flowExecId, function(data) {
        if (data.size != 0) {
            draw(data);
        }
    });
});