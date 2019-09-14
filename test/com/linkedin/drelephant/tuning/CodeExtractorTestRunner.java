package com.linkedin.drelephant.tuning;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.code.CodeExtractor;
import com.linkedin.drelephant.analysis.code.dataset.JobCodeInfoDataSet;
import com.linkedin.drelephant.analysis.code.extractors.AzkabanJarvisCodeExtractor;
import com.linkedin.drelephant.analysis.code.optimizers.hive.HiveCodeOptimizer;
import com.linkedin.drelephant.analysis.code.util.CodeAnalysisConfiguration;
import com.linkedin.drelephant.analysis.code.util.CodeAnalyzerException;
import com.linkedin.drelephant.analysis.code.util.Constant;
import com.linkedin.drelephant.analysis.code.util.Helper;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import models.AppResult;

import static com.linkedin.drelephant.analysis.code.util.Constant.*;
import static common.DBTestUtil.*;

import static org.junit.Assert.*;
import static play.test.Helpers.*;
import static common.DBTestUtil.*;

import models.TuningJobDefinition;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 * This class is used to test CodeExtractor concrete implementation
 * AzkabanJarvisCodeExtractor
 */
public class CodeExtractorTestRunner implements Runnable {

  private void populateTestData() {
    try {
      initDBIPSO();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    populateTestData();
    testAzkabanJarvisCodeExtractor();
  }

  private void testAzkabanJarvisCodeExtractor() {
    Configuration configuration = ElephantContext.instance().getAutoTuningConf();
    Helper.ConfigurationBuilder.buildConfigurations(configuration);
    AppResult _appResult = AppResult.find.byId("application_1458194917883_1453361");
    CodeExtractor codeExtractor = new AzkabanJarvisCodeExtractor() {
      @Override
      protected JSONObject parseURL(String jsonString) throws IOException, JSONException {
        return new JSONObject("{\n" + "  \"paths\" : [ {\n"
            + "    \"filePath\" : \"dfjenkins-metric-defs/metric-defs-provider/src/email_v2/bounces.hql\",\n"
            + "    \"repoName\" : \"multiproducts/dfjenkins-metric-defs\",\n" + "    \"scm\" : \"git\"\n" + "  }, {\n"
            + "    \"filePath\" : \"dfjenkins-metric-defs/metric-defs-provider/conf/jobs/email_v2_bounces.hql\",\n"
            + "    \"repoName\" : \"multiproducts/dfjenkins-metric-defs\",\n" + "    \"scm\" : \"git\"\n" + "  } ]\n"
            + "}\n");
      }

      @Override
      protected String getCodeFileNameFromSchduler(AppResult appResult, String modifiedURL)
          throws MalformedURLException {
        return "src/bounces.hql";
      }

      @Override
      protected String getSourceCode(JSONObject jsonJobInfo) throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject("{\n" + "  \"fileInfo\" : {\n"
            + "    \"headUrl\" : \"https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/dfjenkins-metric-defs/+/master/metric-defs-provider/src/email_v2/bounces.pig\",\n"
            + "    \"historyUrl\" : \"https://git.corp.linkedin.com:1367/a/plugins/gitiles/multiproducts/dfjenkins-metric-defs/+log/master/metric-defs-provider/src/email_v2/bounces.pig\",\n"
            + "    \"sourceCode\" : \"%DECLARE reducers\"\n" + "}\n" + "}");

        String sourceCode = jsonObject.getJSONObject(
            com.linkedin.drelephant.analysis.code.util.Constant.CodeLocationJSONKey.FILE_INFO.getJSONKey())
            .getString(Constant.CodeLocationJSONKey.SOURCE_CODE.getJSONKey());
        return sourceCode;
      }
    };

    Helper.ConfigurationBuilder.QUEUE_NAMES_VALID_FOR_CODE_NAME_EXTRACTION =
        new CodeAnalysisConfiguration<String[]>().setValue(new String[]{"ump_hp", "ump_normal"});

    try {
      JobCodeInfoDataSet jobCodeInfoDataSet = codeExtractor.execute(_appResult);
      assertTrue("JobCodeInfoDataSet should be null ,since its not valid queue ", jobCodeInfoDataSet == null);

      _appResult.queueName = "ump_hp";

      jobCodeInfoDataSet = codeExtractor.execute(_appResult);
      assertTrue("Expected FileName", jobCodeInfoDataSet.getFileName()
          .equals("dfjenkins-metric-defs%2Fmetric-defs-provider%2Fsrc%2Femail_v2%2Fbounces.hql"));
      assertTrue("Expected Reponame ",
          jobCodeInfoDataSet.getRepoName().equals("multiproducts%2Fdfjenkins-metric-defs"));
      assertTrue("Expected SCM ", jobCodeInfoDataSet.getScmType().equals("git"));
      assertTrue("URL TO GET CODE LOCATION" + jobCodeInfoDataSet.getMetaData().get("URL TO GET CODE LOCATION"),
          jobCodeInfoDataSet.getMetaData()
              .get("URL TO GET CODE LOCATION")
              .equals("http://abcd/efgh/api/v1/filepaths?query=bounces.hql"));

      assertTrue("URL TO GET SCRIPT NAME" + jobCodeInfoDataSet.getMetaData().get("URL TO GET SCRIPT NAME"),
          jobCodeInfoDataSet.getMetaData()
              .get("URL TO GET SCRIPT NAME")
              .equals(
                  "https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flowName=reminder&jobName=overwriter-reminder2"));

      assertTrue("URL TO GET SOURCE CODE", jobCodeInfoDataSet.getMetaData()
          .get("URL TO GET SOURCE CODE")
          .equals(
              "http://abcd/efgh/api/v1/file/git/multiproducts%2Fdfjenkins-metric-defs/dfjenkins-metric-defs%2Fmetric-defs-provider%2Fsrc%2Femail_v2%2Fbounces.hql"));

      assertTrue("Hive Optimizer should be return", jobCodeInfoDataSet.getCodeOptimizer() instanceof HiveCodeOptimizer);


    _appResult.queueName = "abcd";

    assertTrue("Should return false,since app result is null ", !codeExtractor.arePrerequisiteMatched(null));
    assertTrue("Should return false , since queue name is not valid",
        !codeExtractor.arePrerequisiteMatched(_appResult));
    _appResult.queueName = "ump_hp";
    assertTrue("Should return true , since queue name is valid", codeExtractor.arePrerequisiteMatched(_appResult));

    try {
      assertTrue("Code file Name should be null ,since app result is null  ",
          codeExtractor.getCodeFileName(null) == null);
      assertTrue("Code file Name should be src/bounces.hql  ",
          codeExtractor.getCodeFileName(_appResult).equals("src/bounces.hql"));
    } catch (CodeAnalyzerException e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionAsString = sw.toString();
      assertTrue("Error while passing test cases " + exceptionAsString, false);
    }

      CodeExtractor newCodeExtractor = new AzkabanJarvisCodeExtractor();
      newCodeExtractor.processCodeLocationInformation(null);
      assertTrue(" JobCodeInfoDataSet should be null ,since null is passed    ",
          newCodeExtractor.getJobCodeInfoDataSet() == null);

      try {
        newCodeExtractor.processCodeLocationInformation("{");
      } catch (Exception e) {
        assertTrue(" Exception should be thrown ,since invalid information is passed  " ,
            e instanceof MalformedURLException);
      }
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String exceptionAsString = sw.toString();
      assertTrue("Error while passing test cases " + exceptionAsString, false);
    }


    /*


     */

  }
}
