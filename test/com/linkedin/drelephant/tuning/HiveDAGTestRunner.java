package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.analysis.code.CodeOptimizer;
import com.linkedin.drelephant.analysis.code.dataset.Script;
import com.linkedin.drelephant.analysis.code.dataset.Statement;
import com.linkedin.drelephant.analysis.code.optimizers.CodeOptimizerFactory;
import com.linkedin.drelephant.analysis.code.optimizers.hive.DAG;
import com.linkedin.drelephant.analysis.code.optimizers.hive.Node;
import com.linkedin.drelephant.analysis.code.optimizers.hive.QueryLevelFeature;
import com.linkedin.drelephant.analysis.code.util.HiveStatementSplitter;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static org.junit.Assert.*;
import static play.test.Helpers.*;


/**
 * This class if for the testing of
 * DAG Class
 * Node class
 * QueryLevelFeature
 * HiveStatementSplitter
 */
public class HiveDAGTestRunner implements Runnable {
  @Override
  public void run() {
    testHiveDAGCreation();
    testQueryLevelFeature();
    testHiveStatementSplitter();
  }

  private void testHiveDAGCreation() {
    String sourceCode = CodeTestConstant.SOURCE_CODE;
    CodeOptimizer codeOptimizer = CodeOptimizerFactory.getCodeOptimizer(".sql");
    Script script = codeOptimizer.execute(sourceCode);
    DAG dag = new DAG();
    try {
      dag.generateDAG(script.getCode().getStatements());
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionAsString = sw.toString();
      assertTrue("Error while creating DAG  " + exceptionAsString, false);
    }
    assertTrue("Total number of root nodes , which are source tables (table2,stg_nova_roma,table7,table6) "
        + dag.getRootNodes(), dag.getRootNodes().size() == 4);
    assertTrue("Total number of root nodes  " + dag.getTotalNodesinGraph(), dag.getTotalNodesinGraph() == 9);

    assertTrue("Number of parents 2", dag.getNodeGivenOutputTable("table5").getParent().size() == 2);
    assertTrue("Number of Children", dag.getNodeGivenOutputTable("table5").getChildren().size() == 1);
    assertTrue("Number of Children", dag.getNodeGivenOutputTable("table5").getNodeComplexity() == 1);
    assertTrue("Number of Children", dag.getNodeGivenOutputTable("table5").getNodeWeight() == 3);

    Statement statement = script.getCode().getStatements().get(10);
    Node node = new Node(statement.getOutputSinks().get(0), statement);
    try {
      node.generateNodeComplexity();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionAsString = sw.toString();
      assertTrue("Error while generating code complexity  " + exceptionAsString, false);
    }
  }

  private void testQueryLevelFeature() {
    try {
      ParseDriver pd = new ParseDriver();
      assertTrue("Number of shuffle operations are zero ", Integer.parseInt(
          QueryLevelFeature.generateFeatures(pd.parse("select * from table")).split(QueryLevelFeature.DELIMITER)[0])
          == 0);
      assertTrue("Number of shuffle operations are 1 ", Integer.parseInt(
          QueryLevelFeature.generateFeatures(pd.parse("select c1,max(c2) from table group by c1"))
              .split(QueryLevelFeature.DELIMITER)[0]) == 1);
      assertTrue("Number of shuffle operations are 1 ", Integer.parseInt(
          QueryLevelFeature.generateFeatures(pd.parse("select c1 from table1 join table2 on (table1.c1=table2.c1)"))
              .split(QueryLevelFeature.DELIMITER)[0]) == 1);
      assertTrue("Number of shuffle operations are 2 ", Integer.parseInt(QueryLevelFeature.generateFeatures(pd.parse(
          "SELECT u.id, actions.date\n" + "FROM (\n" + "    SELECT av.uid AS uid\n" + "    FROM action_video av\n"
              + "    WHERE av.date = '2008-06-03'\n" + "    UNION ALL\n" + "    SELECT ac.uid AS uid\n"
              + "    FROM action_comment ac\n" + "    WHERE ac.date = '2008-06-03'\n"
              + " ) actions JOIN users u ON (u.id = actions.uid)")).split(QueryLevelFeature.DELIMITER)[0]) == 2);

      assertTrue("Number of shuffle operations are 0 ",
          Integer.parseInt(QueryLevelFeature.generateFeatures(null).split(QueryLevelFeature.DELIMITER)[0]) == 0);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionAsString = sw.toString();
      assertTrue("Error while generating feature for the query  " + exceptionAsString, false);
    }
  }

  private void testHiveStatementSplitter() {
    assertTrue("Number of queries are 2  ", HiveStatementSplitter.splitStatements(
        "SELECT *\n" + "FROM (\n" + "  select_statement\n" + "  UNION ALL\n" + "  select_statement\n" + ") unionResult;"
            + "SELECT u.id, actions.date\n" + "FROM (\n" + "    SELECT av.uid AS uid\n" + "    FROM action_video av\n"
            + "    WHERE av.date = '2008-06-03'\n" + "    UNION ALL\n" + "    SELECT ac.uid AS uid\n"
            + "    FROM action_comment ac\n" + "    WHERE ac.date = '2008-06-03'\n"
            + " ) actions JOIN users u ON (u.id = actions.uid)").size() == 2);
    assertTrue("Number of queries are null  ", HiveStatementSplitter.splitStatements(null) == null);
    assertTrue("Number of queries are zero  " + HiveStatementSplitter.splitStatements(""),
        HiveStatementSplitter.splitStatements("").size() == 0);
  }
}
