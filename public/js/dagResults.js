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
var network_nested = null;
var nodes = new vis.DataSet();
var edges = new vis.DataSet();
var nested_mr_nodes = new vis.DataSet();
var nested_mr_edges = new vis.DataSet();
var box_nodes = new vis.DataSet();
var box_edges = new vis.DataSet();
var items = new vis.DataSet();
var g = {nodes: nodes, edges: edges}
var g1 = {nodes: nested_mr_nodes, edges: nested_mr_edges}
var g2 = {nodes: box_nodes, edges: box_edges}
var flowExecId;
var adjMatrix;
var url_str = "/search?id=";
var edge_colour_default = '#808080';    //gray colour for now

/**
 * function for generating a timeline view of the jobs
 */

function view_timeline() {
  var items = new vis.DataSet();
  $.getJSON('/rest/timeline?id=' + flowExecId, function (timeline_data) {
    if (timeline_data.size != 0) {
      for (var i in timeline_data) {
        var info = timeline_data[i];
        container = document.getElementById("dag_container");
        container.innerHTML = "";
        var o = {id: i.toString(), content: info["name"], start: info["sTime"], end: info["fTime"]};
        items.add(o);
        var options = {};
        var timeline = new vis.Timeline(container, items, options);
      }
    }
  });
}

/**
 * function for generating a random DAG
 */
function random_dag() {

  nodes = new vis.DataSet();
  edges = new vis.DataSet();
  g = {nodes: nodes, edges: edges}

  for (var i in adjMatrix) {
    var info = adjMatrix[i];

    var temp_label = info["label"];
    temp_label = temp_label.replace("<b>", "").replace("</b>", "").replace("</br>", "");

    var o = {
      id: i.toString(),
      label: temp_label,
      size: info["time"],
      title: info["title"],
      color: info["colour"],
      originalColor: 'blue'
    };

    nodes.add(o);

    for (var j in info["row"]) {

      var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: edge_colour_default};
      edges.add(e);

    }

  }

  var optR = options = {
    edges: {arrows: 'to', hoverWidth: 5},
    nodes: {shape: 'dot', scaling: {min: 5, max: 20}, font: {size: 12, face: "Tahoma"}},
    physics: {hierarchicalRepulsion: {springConstant: 0.0}},
    interaction: {hover: true, tooltipDelay: 50}
  };
  var network_new = new vis.Network(document.getElementById("dag_container"), g, optR);
  network_new.on("click", function (params) {

    document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
    nested_mr_nodes = new vis.DataSet();
    nested_mr_edges = new vis.DataSet();
    g1 = {nodes: nested_mr_nodes, edges: nested_mr_edges};
    network_nested = null;
    var selNodes = null;
    var selNodes = network_new.getSelectedNodes();
    selectedNode = nodes.get(selNodes);

    var nested_container = document.getElementById("nested_dag_container");

    document.getElementById("mr_header").innerHTML = ("MR DAG for job : " + selectedNode[0]["label"]);

    $.getJSON('/rest/nesteddagdata?id=' + flowExecId + '&name=' + adjMatrix[selectedNode[0]["id"]]["jobname"],
        function (nested_data) {
          if (nested_data.size != 0) {
            drawNested(nested_data);
            network_nested.on("click", function (params) {
              var selNodesNested = null;
              var selNodesNested = network_nested.getSelectedNodes();
              var selectedNodeNested = nested_mr_nodes.get(selNodesNested);
              var job_url = url_str + selectedNodeNested[0]["label"].replace("job", "application");
              window.open(url_str + selectedNodeNested[0]["label"].replace("job", "application"));
            });
            network_nested.redraw();
          } else {
            drawNested(null);
          }
        });

  });

  network_new.redraw();

}

/**
 * function for generating a top-down view of the graph
 */

function top_down_dag() {
  box_nodes = new vis.DataSet();
  box_edges = new vis.DataSet();
  g2 = {nodes: box_nodes, edges: box_edges};
  network_new = null;
  var height;
  for (var i in adjMatrix) {
    //for each JSON Object (one object corresponding to one job) in the JSON Array, we are fetching information and plotting the graph
    var heightStr = "\n";
    var info = adjMatrix[i];
    height = info["time"];
    height = height / 2;
    while (height >= 0) {
      heightStr = heightStr + " \n";
      height = height - 1;
    }
    var workflow_job_label = info["label"];
    var mod_label = workflow_job_label;
    //moving name to a new line if the length is big.
    if (workflow_job_label.length > 100) {
      var mod_label = workflow_job_label.substring(0, 100) + "</br>" + workflow_job_label.substring(101,
              workflow_job_label.length);
    }
    if (workflow_job_label.length > 200) {
      var mod_label = workflow_job_label.substring(0, 100) + "</br>" + workflow_job_label.substring(101, 200) + "</br>"
          + workflow_job_label.substring(201, workflow_job_label.length);
    }

    var job_title = info["title"];
    if (info["title"] == "Not a MapReduce job") {
      job_title = "";

    }

    var o = {
      id: i.toString(),
      label: heightStr,
      size: info["time"],
      title: mod_label + "</br>" + job_title,
      color: info["colour"],
      originalColor: 'blue'
    };
    box_nodes.add(o);
    for (var j in info["row"]) {

      var e = {
        id: i.toString() + "_" + j.toString(),
        from: i.toString(),
        to: j.toString(),
        color: edge_colour_default,
        hoverWidth: 5
      };
      box_edges.add(e);

    }

  }
  var optR = options = {
    edges: {arrows: 'to', hoverWidth: 5},
    nodes: {shape: 'box'},
    physics: {hierarchicalRepulsion: {springConstant: 0.0}},
    interaction: {hover: true},
    layout: {hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 300}}
  };

  var network_new = new vis.Network(document.getElementById("dag_container"), g2, optR);

  network_new.on("click", function (params) {

    document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
    nested_mr_nodes = new vis.DataSet();
    nested_mr_edges = new vis.DataSet();
    g1 = {nodes: nested_mr_nodes, edges: nested_mr_edges};
    network_nested = null;
    var selNodes = null;
    var selNodes = network_new.getSelectedNodes();
    selectedNode = nodes.get(selNodes);

    var nested_container = document.getElementById("nested_dag_container");

    document.getElementById("mr_header").innerHTML = ("MR DAG for job : " + selectedNode[0]["label"]);

    $.getJSON('/rest/nesteddagdata?id=' + flowExecId + '&name=' + adjMatrix[selectedNode[0]["id"]]["jobname"],
        function (nested_data) {
          if (nested_data.size != 0) {
            drawNested(nested_data);
            network_nested.on("click", function (params) {
              var selNodesNested = null;
              var selNodesNested = network_nested.getSelectedNodes();
              var selectedNodeNested = nested_mr_nodes.get(selNodesNested);
              var job_url = url_str + selectedNodeNested[0]["label"].replace("job", "application");
              window.open(job_url);
            });
            network_nested.redraw();
          } else {
            drawNested(null);
          }
        });

  });

  network_new.redraw();

}

/**
 * function for generating a left-right view of the graph
 */
function left_right_dag() {
  box_nodes = new vis.DataSet();
  box_edges = new vis.DataSet();
  g2 = {nodes: box_nodes, edges: box_edges};
  network_new = null;
  var height;
  for (var i in adjMatrix) {
    var heightStr = " ";
    var info = adjMatrix[i];
    height = info["time"];
    while (height >= 0) {
      heightStr = heightStr + "  ";
      height = height - 1;
    }
    var workflow_job_label = info["label"];
    var mod_label = workflow_job_label;
    if (workflow_job_label.length > 100) {
      var mod_label = workflow_job_label.substring(0, 100) + "</br>" + workflow_job_label.substring(101,
              workflow_job_label.length);
    }
    if (workflow_job_label.length > 200) {
      var mod_label = workflow_job_label.substring(0, 100) + "</br>" + workflow_job_label.substring(101, 200) + "</br>"
          + workflow_job_label.substring(201, workflow_job_label.length);
    }

    var job_title = info["title"];
    if (info["title"] == "Not a MapReduce job") {
      job_title = "";
    }
    var o = {
      id: i.toString(),
      label: heightStr,
      size: info["time"],
      title: mod_label + "</br>" + job_title,
      color: info["colour"],
      originalColor: 'blue'
    };
    box_nodes.add(o);
    for (var j in info["row"]) {

      var e = {id: i.toString() + "_" + j.toString(), from: i.toString(), to: j.toString(), color: edge_colour_default};
      box_edges.add(e);

    }

  }
  var optR = options = {
    edges: {arrows: 'to', hoverWidth: 5},
    nodes: {shape: 'box'},
    physics: {hierarchicalRepulsion: {springConstant: 0.0}},
    interaction: {hover: true},
    layout: {hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 350, direction: 'LR'}}
  };

  var network_new = new vis.Network(document.getElementById("dag_container"), g2, optR);

  network_new.on("click", function (params) {

    document.getElementById("nested_dag_container").innerHTML = "No MR DAG available";
    nested_mr_nodes = new vis.DataSet();
    nested_mr_edges = new vis.DataSet();
    g1 = {nodes: nested_mr_nodes, edges: nested_mr_edges};
    network_nested = null;
    var selNodes = null;
    var selNodes = network_new.getSelectedNodes();
    selectedNode = nodes.get(selNodes);

    var nested_container = document.getElementById("nested_dag_container");

    document.getElementById("mr_header").innerHTML = ("MR DAG for job : " + selectedNode[0]["label"]);

    $.getJSON('/rest/nesteddagdata?id=' + flowExecId + '&name=' + adjMatrix[selectedNode[0]["id"]]["jobname"],
        function (nested_data) {
          if (nested_data.size != 0) {
            drawNested(nested_data);
            network_nested.on("click", function (params) {
              var selNodesNested = null;
              var selNodesNested = network_nested.getSelectedNodes();
              var selectedNodeNested = nested_mr_nodes.get(selNodesNested);
              var job_url = url_str + selectedNodeNested[0]["label"].replace("job", "application");
              window.open(job_url);
            });
            network_nested.redraw();
          } else {
            drawNested(null);
          }
        });
  });

  network_new.redraw();

}

/**
 * function for generating a MR DAG corresponding to a job.
 * @param nestedAdjMatrix: information of the jobs for the MR DAG to be constructed
 */
function drawNested(nestedAdjMatrix) {
  nested_mr_nodes = new vis.DataSet();
  nested_mr_edges = new vis.DataSet();
  g1 = {nodes: nested_mr_nodes, edges: nested_mr_edges};

  for (var i in nestedAdjMatrix) {
    var info = nestedAdjMatrix[i];

    var o = {
      id: 'MR' + i.toString(),
      label: info["label"],
      size: info["size"],
      color: info["colour"],
      originalColor: 'blue',
      title: info["title"]
    };
    nested_mr_nodes.add(o);
    for (var j in info["row"]) {

      var e = {
        id: ('MR' + i.toString()) + "_" + ('MR' + j.toString()),
        from: ('MR' + i.toString()),
        to: ('MR' + j.toString()),
        color: edge_colour_default
      };
      nested_mr_edges.add(e);

    }
  }

  nested_container = document.getElementById("nested_dag_container");
  options = {
    edges: {arrows: 'to', hoverWidth: 5},
    nodes: {shape: 'dot', scaling: {min: 5, max: 20}, font: {size: 12, face: "Tahoma"}},
    physics: {hierarchicalRepulsion: {springConstant: 0.0}},
    interaction: {hover: true, tooltipDelay: 50},
    layout: {hierarchical: {enabled: true, sortMethod: 'directed', levelSeparation: 300}}
  };
  network_nested = new vis.Network(nested_container, g1, options);

}

/**
 * This function is called for constructing the graph for the very first time. For now the default state is the top_down dag, and hence this function calls that function only.
 * @param adjMatrixWrapper: a JSON object containing information about the jobs for the DAG
 */
function draw(adjMatrixWrapper) {
  adjMatrix = adjMatrixWrapper;
  top_down_dag();

}

$(document).ready(function () {
  flowExecId = queryString()['flow-exec-id'];
  $.getJSON('/rest/daggraphdata?id=' + flowExecId, function (data) {
    if (data.size != 0) {
      draw(data);
    }
  });
});