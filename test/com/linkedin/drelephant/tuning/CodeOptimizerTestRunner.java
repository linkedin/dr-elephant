package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.analysis.code.CodeOptimizationRule;
import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.dataset.Code;
import com.linkedin.drelephant.analysis.code.dataset.Script;
import com.linkedin.drelephant.analysis.code.dataset.Statement;
import com.linkedin.drelephant.analysis.code.optimizers.CodeOptimizerFactory;
import com.linkedin.drelephant.analysis.code.optimizers.hive.ActionTransformationRule;
import com.linkedin.drelephant.analysis.code.optimizers.hive.HiveCodeOptimizer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;


public class CodeOptimizerTestRunner implements Runnable {
  @Override
  public void run() {
    testHiveCodeOptimizer();
    testHiveCodeOptimizerParameterSubstitution();
  }

  private void testHiveCodeOptimizer() {
    String sourceCode = CodeTestConstant.SOURCE_CODE;


    CodeOptimizer codeOptimizer = CodeOptimizerFactory.getCodeOptimizer(".sql");
    Script script = codeOptimizer.execute(sourceCode);
    testScript(script);
    Code code = script.getCode();
    testCode(code);
    Statement statement = code.getStatements().get(7);
    testStatement(statement);
    assertTrue("Severity should be Critical , since many tables can be converted into Views ",
        codeOptimizer.getSeverity().equals("Critical"));
    assertTrue("Code should be null , since passed is value is null", codeOptimizer.generateCode(null) == null);
    assertTrue("Since its not valid hive statement , dummy statement would be parsed and AST tree would be nil  ",
        codeOptimizer.generateCode("hdsfhjs").getStatements().get(0).getBaseTree().toString().equals("nil"));
    Script tempScript = new Script("");
    List<CodeOptimizationRule> ruleList = new ArrayList<CodeOptimizationRule>();
    ruleList.add(new ActionTransformationRule("Convert Temp Table To Views"));
    codeOptimizer.optimizationEngine(tempScript, ruleList);
    assertTrue(" Severity is ", codeOptimizer.getSeverity().equals("None"));
  }

  private void testHiveCodeOptimizerParameterSubstitution() {
    HiveCodeOptimizer hiveCodeOptimizer = new HiveCodeOptimizer();
    assertTrue("Value after parameter handler ", hiveCodeOptimizer.hiveParametersHandler(
        "select * from ${hiveconf:abcd} '${hiveconf:end_date_hour}' '${hivevar:dfd}' ${eeee} '${gggg}' $ad}fg; '$hghd'")
        .equals("select * from abcd 'end_date_hour' 'dfd' eeee 'gggg' ad}fg; 'hghd'"))  ;
  }

  private void testScript(Script script) {
    assertTrue("Script should not be null ", script != null);
    assertTrue("Script , optimization comment  ", script.getOptimizationComment()
        .toString()
        .replaceAll("\n", "@")
        .replace("\\W", "")
        .equals("table1@table3@table4@table5@"));
  }

  private void testCode(Code code) {
    assertTrue("Number of statements are should be 14 " + code.getStatements().size(),
        code.getStatements().size() == 14);
  }

  private void testStatement(Statement statement) {
    assertTrue("Original Statement is as passed originally " + statement.getOriginalStatement(),
        statement.getOriginalStatement()
            .equals("CREATE TEMPORARY TABLE table1 AS \n" + "SELECT \n" + "  rome.id AS id, \n"
                + "  rome.member_id AS member_id\n" + "FROM \n" + "  $table7 rome \n"
                + "LEFT JOIN (SELECT * FROM $table2 WHERE member_id IS NOT NULL) member_data \n"
                + "  ON rome.member_id = member_data.member_id \n"
                + " AND COALESCE(rome.merge_source,'#N/A') = COALESCE(member_data.merge_source,'#N/A')"));

    assertTrue(
        "Framework Parsable Query should be after parameter substitution " + statement.getFrameWorkParsableQuery(),
        statement.getFrameWorkParsableQuery().contains("table7") && !statement.getFrameWorkParsableQuery()
            .contains("\\$table7") && statement.getFrameWorkParsableQuery().contains("table2")
            && !statement.getFrameWorkParsableQuery().contains("\\$table2"));

    assertTrue("Number of input tables  " + statement.getInputSources().size(),
        statement.getInputSources().size() == 2);
    assertTrue("First input table is table2", statement.getInputSources().get(0).equals("table2"));
    assertTrue("Second input  table is table7", statement.getInputSources().get(1).equals("table7"));
    assertTrue("Output  table is table1", statement.getOutputSinks().get(0).equals("table1"));
    assertTrue("Checkpoint should be false", !statement.getIsCheckpoint());
  }
}
