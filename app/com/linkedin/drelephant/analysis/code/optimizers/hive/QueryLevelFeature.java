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

import com.linkedin.drelephant.analysis.code.util.Helper;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.log4j.Logger;


/**
 * This class generates Query level features , which will be used
 * to define the node complexity. Features , number of shuffle operations,depth of tree,
 * total branches
 */
public class QueryLevelFeature {

  public static String DELIMITER = "<FEATURE_DELIMETER>";
  private static final Logger logger = Logger.getLogger(QueryLevelFeature.class);

  public static String generateFeatures(ASTNode node) {
    if (node == null) {
      return "0";
    }
    Set<String> shuffleOperation = new HashSet<String>();
    Collections.addAll(shuffleOperation, Helper.ConfigurationBuilder.SHUFFLE_OPERATIONS_IN_HIVE.getValue());
    int numberofShuffleOperations = traverseTree(node, 0, shuffleOperation);
    int lengthOFASTTree = depthOfTree(node);
    int branchesinASTTree = totalBranches(node, 0);
    return numberofShuffleOperations + DELIMITER + lengthOFASTTree + DELIMITER + branchesinASTTree;
  }

  /**
   * Traverse the AST tree , with DFS and count number of shuffle operations.
   * @param node : AST Node
   * @param operation : Number of shuffle oer
   * @return : Total number of shuffle operation
   */
  private static int traverseTree(org.apache.hadoop.hive.ql.lib.Node node, int operation,
      Set<String> shuffleOperation) {
    if (node == null) {
      return operation;
    } else {
      if (node.getChildren() == null) {
        return operation;
      }

      for (Node rootNode : node.getChildren()) {
        logger.debug(" Node operation " + rootNode.toString());
        if (shuffleOperation.contains(rootNode.toString())) {
          operation = operation + 1;
        }
        operation = traverseTree(rootNode, operation, shuffleOperation);
      }
    }
    return operation;
  }

  private static int totalBranches(org.apache.hadoop.hive.ql.lib.Node node, int branch) {
    if (node.getChildren() == null) {
      System.out.println(node.toString());
      return branch + 1;
    } else {
      for (Node rootNode : node.getChildren()) {
        branch = totalBranches(rootNode, branch);
      }
      return branch;
    }
  }

  private static int depthOfTree(org.apache.hadoop.hive.ql.lib.Node node) {
    if (node.getChildren() == null) {
      return 0;
    } else {
      int maxDepth = 0;
      for (Node rootNode : node.getChildren()) {
        int depth = depthOfTree(rootNode);
        if (depth > maxDepth) {
          maxDepth = depth;
        }
      }
      return maxDepth + 1;
    }
  }
}

