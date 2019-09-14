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

import com.linkedin.drelephant.analysis.code.dataset.Code;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;
import com.linkedin.drelephant.analysis.code.CodeOptimizationRule;
import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.dataset.Script;
import com.linkedin.drelephant.analysis.code.dataset.Statement;
import com.linkedin.drelephant.analysis.code.util.HiveStatementSplitter;
import java.util.ArrayList;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.antlr.runtime.NoViableAltException;

public class HiveCodeOptimizer implements CodeOptimizer {

  private static final Logger logger = Logger.getLogger(HiveCodeOptimizer.class);
  private static final String HIVE_COMMENT_TRIGGER = "--";
  private static final String NEWLINE = "\n";
  private static final String SHELL_VAR_PATTERN = "\\$[^\\$\\{\\}\\s\\'\"\u0020\\;]+";
  private static final String HIVE_VAR_PATTERN = "\\$\\{[^\\}\\$\u0020\\;]+\\}";
  private static Pattern shellVariblePattern = Pattern.compile(SHELL_VAR_PATTERN);
  private static Pattern hiveVariablePattern = Pattern.compile(HIVE_VAR_PATTERN);
  private static final String HIVECONF_PREFIX = "hiveconf:";
  private static final String HIVEVAR_PREFIX = "hivevar:";
  private static ParseDriver pd = new ParseDriver();
  private static final String HIVE_NOT_PARSED = "HIVE_NOT_ABLE_TO_PARSE_QUERY";
  private static final String DUMMY_QUERY = "select * from " + HIVE_NOT_PARSED;
  private static String scriptSeverity = null;

  @Override
  public Script execute(String sourceCode) {
    Script script = null;
    if (sourceCode != null) {
      script = new Script(sourceCode);
      Code code = generateCode(sourceCode);
      script.setCode(code);
      List<CodeOptimizationRule> rules = new ArrayList<CodeOptimizationRule>();
      rules.add(new ActionTransformationRule("Convert Temp Table To Views"));
      // todo : Apply caching Rule
      //rules.add(new CachingRule("Caching Recommendation ", statements));
      optimizationEngine(script, rules);
      logger.info(" Following is an optimizing comment " + script.getOptimizationComment());
    }
    return script;
  }

  @Override
  public Code generateCode(String sourceCode) {
    Code code = null;
    if (sourceCode != null) {
      code = new Code();
      List<Statement> statements = new ArrayList<Statement>();
      List<String> inputStatement = HiveStatementSplitter.splitStatements(sourceCode);
      int sequenceNumber = 0;
      for (String input : inputStatement) {
        Statement statement = new Statement(sequenceNumber, input);
        if (parseStatement(statement)) {
          statements.add(statement);
          sequenceNumber++;
        } else {
          logger.info("Unable to parse following input command " + input);
        }
      }
      code.setStatements(statements);
    }
    return code;
  }

  /**
   *  Parse each HiveQuery ,convert it into Statement.
   *  1) Handle comments in the query
   *  2) Handle the parameters
   *  3) Create Framework Parsebale query
   *  4) Get metadata , for e.g in case of hive , get input and output tables.
   *
   * @param statement : Statement to be filled
   * @return : If not able to parse it correctly then return false
   */
  private boolean parseStatement(Statement statement) {
    try {
      String orginalQuery = queryCommentHandling(statement);
      String hiveParsableQuery = hiveParametersHandler(orginalQuery);
      statement.setFrameWorkParsableQuery(hiveParsableQuery);
      metaDataHandling(statement, hiveParsableQuery);
      return true;
    } catch (Exception e) {
      //Catching blank exception ,since even if unable to parse one of the statement
      // Still should be able to parse other statements .
      logger.error(" Unable to generate framework parseable query " + statement.getOriginalStatement(),
          new CodeAnalyzerException(e));
      return false;
    }
  }

  /**
   *  Create AST and fill inputTable and output Tables (to Create DAG)
   * @param statement :
   * @param hiveParsableQuery : Query which can be parsed with Hive Parse
   * Catching runtime exception since this exception can be thrown
   * org.antlr.v4.runtime.NoViableAltException ,if query is not parasable.
   */
  private void metaDataHandling(Statement statement, String hiveParsableQuery) {
    try {
      ASTNode node = pd.parse(hiveParsableQuery);
      LineageInfo lep = new LineageInfo();
      lep.getLineageInfo(hiveParsableQuery);
      statement.setBaseTree(node);
      List<String> inputSources = new ArrayList<String>();
      List<String> outputSinks = new ArrayList<String>();
      for (String inputTable : lep.getInputTableList()) {
        inputSources.add(inputTable);
      }
      for (String outputTable : lep.getOutputTableList()) {
        outputSinks.add(outputTable);
      }
      enrichOutputTablesWithCreateTable(node, outputSinks);
      statement.setInputSources(inputSources);
      statement.setOutputSinks(outputSinks);
    } catch (ParseException | SemanticException  e) {
      logger.error(" Unable to parse ,following query " + hiveParsableQuery);
      try {
        statement.setBaseTree(pd.parse(DUMMY_QUERY));
      } catch (ParseException e1) {
        logger.error(" Unable to parse ,dummy query " + DUMMY_QUERY);
      }
    }
  }

  /*
    Hive Parser gives null as Output table if its create Table / create external table .
    It works only on insert overwrite . To get list of output table which are created
    through create table / create external table , parsing the tree.
   */
  private void enrichOutputTablesWithCreateTable(ASTNode node, List<String> outputSinks) {
    for (Node rootNode : node.getChildren()) {
      if (rootNode.toString().equals("TOK_CREATETABLE")) {
        for (Node child : rootNode.getChildren()) {
          if (child.toString().equals("TOK_TABNAME")) {
            for (Node secondChild : child.getChildren()) {
              outputSinks.add(secondChild.toString());
            }
          }
        }
      }
    }
  }

  /**
   * Handle the comments of the query and return the query without comment
   * @param statement
   * @return
   */
  private String queryCommentHandling(Statement statement) {
    String lines[] = statement.getOriginalStatement().split(NEWLINE);
    StringBuilder queryWithoutComments = new StringBuilder();
    StringBuilder comments = new StringBuilder();
    for (String line : lines) {
      if (line.trim().startsWith(HIVE_COMMENT_TRIGGER)) {
        comments.append(line).append(NEWLINE);
      } else {
        queryWithoutComments.append(line).append(NEWLINE);
      }
    }
    logger.debug(" Following are the comments of the query " + comments);
    return queryWithoutComments.toString();
  }

  /**
   * Handles the parameters in query , since for parsing the query into AST ,
   * Parameters has to be handle .
   * @param originalQuery : Query with parameter
   * @return Query with handling parameter handler
   * This is protected to test it .
   */
  public String hiveParametersHandler(String originalQuery) {
    String temperoraryQuery = processForShellVariable(originalQuery);
    String queryWithParmeterHandler = processForHiveVariable(temperoraryQuery);
    logger.debug(" Query before parameter handler " + originalQuery);
    logger.debug(" Query after parameter handler " + queryWithParmeterHandler);
    return queryWithParmeterHandler;
  }

  private String processForShellVariable(String originalQuery) {
    Matcher match = shellVariblePattern.matcher(originalQuery);
    while (match.find()) {
      String matchVariable = match.group();
      String actualVariable = matchVariable.substring(1);
      originalQuery = match.replaceFirst(actualVariable);
      match = shellVariblePattern.matcher(originalQuery);
    }
    return originalQuery;
  }

  private String processForHiveVariable(String originalQuery) {
    Matcher match = hiveVariablePattern.matcher(originalQuery);
    while (match.find()) {
      String group = match.group();
      String actualString = group.substring(2, group.length() - 1);
      actualString = processForHiveTriggers(actualString);
      originalQuery = match.replaceFirst(actualString);
      match = hiveVariablePattern.matcher(originalQuery);
    }
    return originalQuery;
  }

  private String processForHiveTriggers(String actualString) {
    if (actualString.startsWith(HIVECONF_PREFIX)) {
      actualString = actualString.substring(HIVECONF_PREFIX.length());
    } else if (actualString.startsWith(HIVEVAR_PREFIX)) {
      actualString = actualString.substring(HIVEVAR_PREFIX.length());
    }
    return actualString;
  }

  @Override
  public void optimizationEngine(Script script, List<CodeOptimizationRule> rules) {
    if (script != null && rules != null && rules.size() > 0) {
      try {
        for (CodeOptimizationRule rule : rules) {
          rule.processRule(script);
          scriptSeverity = rule.getSeverity();
          logger.info("Severity at the rule level " + scriptSeverity);
        }
      } catch (CodeAnalyzerException e) {
        logger.error(" Unable to apply rules ", e);
      }
    }
  }

  @Override
  public String getSeverity() {
    return scriptSeverity;
  }
}
