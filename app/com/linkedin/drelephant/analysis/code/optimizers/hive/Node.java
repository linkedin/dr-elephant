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


package com.linkedin.drelephant.analysis.code.optimizers.hive;

import com.linkedin.drelephant.analysis.code.dataset.Statement;
import com.linkedin.drelephant.analysis.code.util.Constant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.json.JSONException;
import org.apache.hadoop.hive.ql.parse.ASTNode;


/**
 * This class represent the Node in the DAG
 */
public class Node {
  private String outputTable = null;
  private String query = null;
  private List<Node> parents = null;
  private List<Node> children = null;
  private Statement statement = null;
  private int nodeWeight = 0;
  private int nodeComplexity = 0;

  public int getNodeComplexity() {
    return nodeComplexity;
  }

  /**
   * Generate the complexity of the Node , based on the number of shuffle operation.
   * @throws ParseException , Can throws parse exception and JSON exception
   * @throws JSONException
   */
  public void generateNodeComplexity() throws ParseException, JSONException {
    String queryLevelFeatures = QueryLevelFeature.generateFeatures((ASTNode) statement.getBaseTree());
    int numberOfShuffleOperation = Integer.parseInt(queryLevelFeatures.split(QueryLevelFeature.DELIMITER)[0]);
    if (numberOfShuffleOperation == Constant.ShuffleOperationThreshold.FIRST_THRESHOLD.getValue()) {
      this.nodeComplexity = Constant.NodeComplexity.LOW.getValue();
    } else if (numberOfShuffleOperation == Constant.ShuffleOperationThreshold.SECOND_THRESHOLD.getValue()) {
      this.nodeComplexity = Constant.NodeComplexity.MEDIUM.getValue();
    } else if (numberOfShuffleOperation > Constant.ShuffleOperationThreshold.SECOND_THRESHOLD.getValue()) {
      this.nodeComplexity = Constant.NodeComplexity.HIGH.getValue();
    }
  }

  public Node(String outputTable, Statement statement) {
    this.outputTable = outputTable;
    this.statement = statement;
    parents = new ArrayList<Node>();
    children = new ArrayList<Node>();
    if (statement != null) {
      query = statement.getFrameWorkParsableQuery();
    }
  }

  public void addChild(Node node) {
    this.children.add(node);
  }

  public List<Node> getChildren() {
    return this.children;
  }

  public void addParent(Node node) {
    this.parents.add(node);
  }

  public List<Node> getParent() {
    return this.parents;
  }

  public Integer getNodeWeight() {
    return nodeWeight;
  }

  public void setNodeWeight(Integer nodeWeight) {
    this.nodeWeight = nodeWeight;
  }

  public String getQuery() {
    return query;
  }

  public Statement getStatement() {
    return statement;
  }

  /**
   * Currently not is us , but will be helpful for DAG analysis.
   * @return
   */
  public int getINDegree() {
    return this.getParent().size();
  }

  /**
   * Currently not is us , but will be helpful for DAG analysis.
   * @return
   */
  public int getOUTDegree() {
    return this.getChildren().size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Node node = (Node) o;
    return Objects.equals(outputTable, node.outputTable);
  }

  @Override
  public int hashCode() {

    return Objects.hash(outputTable);
  }

  @Override
  public String toString() {
    if (this.getStatement() == null) {
      return "Node{" + "outputTable='" + outputTable + '\'' + ", parents=" + parents.size() + ", children="
          + children.size() + ", nodeWeight=" + nodeWeight + ", nodeComplexity=" + nodeComplexity + " Checkpoinint="
          + "false" + '}';
    } else {
      return "Node{" + "outputTable='" + outputTable + '\'' + ", parents=" + parents.size() + ", children="
          + children.size() + ", nodeWeight=" + nodeWeight + ", nodeComplexity=" + nodeComplexity + " Checkpoinint="
          + this.getStatement().getIsCheckpoint() + '}';
    }
  }
}