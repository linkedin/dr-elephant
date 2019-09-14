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
import com.linkedin.drelephant.analysis.code.util.Helper;
import com.linkedin.drelephant.exceptions.util.ExceptionUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.apache.hadoop.hive.ql.parse.ASTNode;


/***
 * This class is responsible for creating DAG . It will take List<Statement>
 * and create DAG . This is Specifically for Hive
 */
public class DAG {
  private static final Logger logger = Logger.getLogger(DAG.class);
  private Map<String, Node> outputTableMap = null;
  private List<Node> rootNodes = null;
  private int totalNumberNodesInDAG = 0;

  public DAG() {
    outputTableMap = new HashMap<String, Node>();
  }

  public Node getNodeGivenOutputTable(String outputTable) {
    return outputTableMap.get(outputTable);
  }

  /**
   *  Main methods , which should be called in Rule  , which requires to create DAG.
   * @param statemens : Source code is parsed to List<Statements>, which is passed here to create DAG
   * @throws ParseException : Throws ParseException
   * @throws JSONException : Json EXception
   * @throws IOException : IO Exception
   */
  public void generateDAG(List<Statement> statemens) throws ParseException, JSONException, IOException {
    if (statemens != null && statemens.size() > 0) {
      for (Statement statement : statemens) {
        if (((ASTNode) statement.getBaseTree()).dump().contains("TOK_SELEXPR")) {
          List<String> inputTables = statement.getInputSources();
          List<String> outputTables = statement.getOutputSinks();
          for (String outputTable : outputTables) {
            Node node = new Node(outputTable, statement);
            addParents(node, inputTables);
            node.generateNodeComplexity();
            outputTableMap.put(outputTable, node);
          }
        }
      }
      generateNodeWeights();
      generateCheckPoint();
    }
  }

  /**
   * Adding parent to list of input tables.
   * @param node
   * @param inputTables
   */
  private void addParents(Node node, List<String> inputTables) {
    for (String inputTable : inputTables) {
      Node parentNode = outputTableMap.get(inputTable);
      if (parentNode == null) {
        parentNode = new Node(inputTable, null);
        outputTableMap.put(inputTable, parentNode);
      }
      parentNode.addChild(node);
      node.addParent(parentNode);
    }
  }

  public int getTotalNodesinGraph() {
    return outputTableMap.size();
  }

  /**
   * Generate Node Weights , Node weights are different from Node Complexity.
   * Node Complexity defines about the complexity of the query . Node Weight
   * defines the weight of the node ,so far , traversing from the source
   */
  private void generateNodeWeights() {
    List<Node> roots = getRootNodes();
    for (Node root : roots) {
      addWeightstoNode(root, 0);
    }
  }

  /**
   * DFS to traverse ,the DAG and add weights to the node.
   * @param node
   * @param weightSoFar
   */
  private void addWeightstoNode(Node node, int weightSoFar) {
    if (node == null) {
      return;
    } else {
      int nodeComplexity = node.getNodeComplexity();
      if (nodeComplexity + weightSoFar >= node.getNodeWeight()) {
        node.setNodeWeight(nodeComplexity + weightSoFar);
      }
      for (Node child : node.getChildren()) {
        addWeightstoNode(child, node.getNodeWeight());
      }
    }
  }

  /**
   * This is used to print information about DAG .
   * This also traverses the DAG and print it .
   */
  void dumpGraph() {
    rootNodes = getRootNodes();
    totalNumberNodesInDAG = getTotalNodesinGraph();
    logger.info(" Number of Root Nodes " + rootNodes.size());
    logger.info(" Root Nodes " + rootNodes);
    logger.info(" Total Nodes in graph " + totalNumberNodesInDAG);
    for (Node root : rootNodes) {
      traverseGraph(root, 0, 0);
    }
  }

  /**
   *  Get the list of root nodes
   * @return Root nodes list
   */
  public List<Node> getRootNodes() {
    List<Node> roots = new ArrayList<Node>();
    for (String table : outputTableMap.keySet()) {
      Node node = outputTableMap.get(table);
      if (node.getQuery() == null) {
        roots.add(node);
      }
    }
    return roots;
  }

  /**
   *  This will traverse the DAG and is useful prininting.
   *  DFS to traver the DAG
   * @param node : Root Node
   * @param space : How much space is required to print
   * @param level : Level of node
   */
  private void traverseGraph(Node node, int space, int level) {
    if (node == null) {
      return;
    } else {
      StringBuilder spaces = new StringBuilder();
      for (int i = 0; i < space; i++) {
        spaces.append("\t");
      }
      logger.debug("L" + level + "\t" + spaces + " Node " + node);
      space++;
      level++;

      for (Node child : node.getChildren()) {

        traverseGraph(child, space, level);
      }
      space--;
      level--;
    }
  }

  /**
   * Traverse the graph from all possible root nodes , and if the node weight
   * cross threshold , then checkpoint that query (means don't recommend to convert to View)
   */
  private void generateCheckPoint() {
    List<Node> rootNodes = getRootNodes();
    for (Node root : rootNodes) {
      addCheckPoint(root, 0);
    }
  }

  /**
   * todo : Move this method to action transformation rule . As checkpointing is intrinsic to action
   * transformation rule and not to DAG.
   *
   */
  private void addCheckPoint(Node node, int previousCheckpointWeight) {
    if (node == null) {
      return;
    } else {
      int weightNode = node.getNodeWeight();
      if (weightNode - previousCheckpointWeight
          >= Helper.ConfigurationBuilder.THRESHOLD_FOR_CHECKPOINT_IN_ACTION_TRANSFORMATION_RULE.getValue()) {
        if (!node.getStatement().getIsCheckpoint()) {
          logger.debug("Checkpointing " + node.getStatement().getOutputSinks());
        }
        node.getStatement().setIsCheckpoint(true);
        logger.debug("Previous Checkpoint " + previousCheckpointWeight + "," + weightNode);
        previousCheckpointWeight = weightNode;
      }
      for (Node child : node.getChildren()) {
        addCheckPoint(child, previousCheckpointWeight);
      }
    }
  }
}
