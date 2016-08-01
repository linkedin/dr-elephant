package com.linkedin.drelephant.exceptions;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class AzkabanFlowLogAnalyzerTest {
  @Test
  public void getFailedSubEventsTest(){
    String rawAzkabanFlowLog ="10-05-2016 23:54:57 PDT postalCodeFlow INFO - Assigned executor : ltx1-hcl0068.grid.linkedin.com:12322\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Running execid:246116 flow:postalCodeFlow project:1525 version:1\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Update active reference\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Updating initial flow directory.\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Fetching job and shared properties.\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Starting flows\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Running flow 'postalCodeFlow'.\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Configuring Azkaban metrics tracking for jobrunner object\n"
        + "10-05-2016 23:54:57 PDT postalCodeFlow INFO - Submitting job 'postalCodeFlow_postalCode' to run.\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Job postalCodeFlow_postalCode finished with status FAILED in 2 seconds\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Setting postalCodeFlow to FAILED_FINISHING\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Cancelling 'postalCodeFlow' due to prior errors.\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Setting flow '' status to FAILED in 2 seconds\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - No attachment file for job postalCodeFlow_postalCode written.\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Finishing up flow. Awaiting Termination\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Finished Flow\n"
        + "10-05-2016 23:55:00 PDT postalCodeFlow INFO - Setting end time for flow 246116 to 1462949700164";
    AzkabanFlowLogAnalyzer analyzedLog = new AzkabanFlowLogAnalyzer(rawAzkabanFlowLog);
    assertEquals(analyzedLog.getFailedSubEvents().size(), 1);
    assertTrue(analyzedLog.getFailedSubEvents().iterator().next().equals("postalCodeFlow_postalCode"));;
  }

}
