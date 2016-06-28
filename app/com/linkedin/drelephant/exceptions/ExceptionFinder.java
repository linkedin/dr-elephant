package com.linkedin.drelephant.exceptions;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

public class ExceptionFinder {
    private final Logger logger = Logger.getLogger(ExceptionFinder.class);
    private String url;
    private Map<String, List<HadoopException>> Exceptions;
    private AzkabanClient client;
    final String jhistoryAddr = new Configuration().get("mapreduce.jobhistory.webapp.address");

    public ExceptionFinder (String url) {
        this.url = url;
        this.Exceptions = new HashMap<String, List<HadoopException>>();
        client = new AzkabanClient(url);
        client.azkabanLogin("", ""); //login using your credentials!
        fetchAndAnalyzeLogs();
    }

    public void fetchAndAnalyzeLogs() {
        AnalyzedLog flowLog = analyzeAzkabanFlowLog();
        Set<String> failedAzkabanJobs = flowLog.getList();
        for (String azkabanJob : failedAzkabanJobs) {
            AnalyzedLog azlog = analyzeAzkabanJobLog(azkabanJob);
            List<HadoopException> exceptions = new ArrayList<HadoopException>();      // azkabanJob exceptions

            Set<String> hadoopJobs = azlog.getList();
            for (String hadoopJob : hadoopJobs) {
                String jhsURL =   "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + hadoopJob;
                AnalyzedLog mrlog = analyzeHadoopJobOverview(jhsURL);
                if (mrlog != null) {
                    HadoopException mrException = new HadoopException();
                    if (mrlog.getException().getExceptionChain()!= null) { //mr job failed for reason other than task failure
                        ExceptionLoggingEvent mrJobException = mrlog.getException();
                        mrJobException.setType("mr");
                        mrJobException.setId(hadoopJob);
                        //ExceptionLoggingEventToAvro test = new ExceptionLoggingEventToAvro();
                        //logger.info("avro: "+ test.convert(mrJobException));
                        mrException.addException(mrJobException);
                    }
                    Set<String> failedHadoopTasks = mrlog.getList();
                    for (String hadoopTask : failedHadoopTasks) {
                        jhsURL = "http://" + jhistoryAddr + "/ws/v1/history/mapreduce/jobs/" + hadoopJob + "/tasks/" + hadoopTask+ "/attempts";
                        ExceptionLoggingEvent mrTaskException = analyzeHadoopTaskDiagnostic(jhsURL);
                        mrTaskException.setType("task");
                        mrTaskException.setId(hadoopTask);
                        //ExceptionLoggingEventToAvro test2 = new ExceptionLoggingEventToAvro();
                        //logger.info("avro: "+ test2.convert(mrTaskException));
                        mrException.addException(mrTaskException);
                    }
                    exceptions.add(mrException);
                }
            }
            if (exceptions.isEmpty() && azlog.getException() != null) { // Azkaban job failed for reason other than mr job failure
                HadoopException azException = new HadoopException();
                azException.addException(azlog.getException());
                exceptions.add(azException);
            }
            Exceptions.put(azkabanJob, exceptions);
        }
    }


    public AnalyzedLog analyzeAzkabanFlowLog(){
        String response = client.getExecutionLog("0","99999999");
        AnalyzedLog analyzedLog = new AnalyzedLog(response);
        //logger.info("analyzeAzkabanFlowLog " + analyzedLog.getState()+ "\n" + analyzedLog.getException()+ "\n" + analyzedLog.getList());
        return analyzedLog;
    }

   /* public List<String> fetchAzkabanJobs(){ //TO DO, Gives the list of all the azkaban jobs in a flow, not required now
        //JSONObject response = client.getExecutionInfo(executionId);
        //Parse to return the String array of azkaban job list
        List<String> test = new ArrayList<String>();
        return test; //Just for testing purpose
    }*/

    public AnalyzedLog analyzeAzkabanJobLog(String azkabanJob){
        String response = client.getJobLog(azkabanJob,"0","99999999");
        AnalyzedLog analyzedLog = new AnalyzedLog(response);
        logger.info("analyzeAzkabanJobLog " + azkabanJob + "\n" + analyzedLog.getState() + "\n" + analyzedLog.getException() + "\n" + analyzedLog.getList());
        return analyzedLog;
    }


    public JsonNode readJson(URL url){
        AuthenticatedURL.Token token= new AuthenticatedURL.Token();
        AuthenticatedURL authenticatedURL = new AuthenticatedURL();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            HttpURLConnection conn = authenticatedURL.openConnection(url, token);
            return objectMapper.readTree(conn.getInputStream());
        }
        catch (AuthenticationException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }


    public AnalyzedLog analyzeHadoopJobOverview(String jobURL){
        try{
            JsonNode response = readJson(new URL(jobURL));
            logger.info("job interaction" + response.get("job").get("diagnostics").toString());
            if(response.get("job").get("state").toString() != "SUCCEEDED"){
                AnalyzedLog analyzedLog = new AnalyzedLog(response.get("job").get("diagnostics").getTextValue());
                logger.info("analyzeHadoopJobOverview " + analyzedLog.getState() + analyzedLog.getException() + analyzedLog.getList());
                return analyzedLog;
            }

        }
                catch (MalformedURLException e ){
            e.printStackTrace();
        }
        return null;//analyzedLog;
    }

    public ExceptionLoggingEvent analyzeHadoopTaskDiagnostic(String taskURL){ // Analyzes the last task attempt
        try{
            JsonNode response = readJson(new URL(taskURL));
            int attempts = response.get("taskAttempts").get("taskAttempt").size();
            int maxattempt = 0;
            int maxattemptid = 0;
            for(int i=0; i<attempts; i++){
                int attempt = Integer.parseInt(response.get("taskAttempts").get("taskAttempt").get(i).get("id").getTextValue().split("_")[5]);
                logger.info("ateempt "+ i + attempt + maxattempt + maxattemptid);
                if(attempt>maxattempt){
                    maxattemptid = i;
                    maxattempt = attempt;
                }
            }
            logger.info("task interaction" + maxattemptid);
            AnalyzedLog analyzedLog = new AnalyzedLog(response.get("taskAttempts").get("taskAttempt").get(maxattemptid).get("diagnostics").getTextValue());
            logger.info("analyzeHadoopTaskDiagnostic " + analyzedLog.getState() + analyzedLog.getException() + analyzedLog.getList());
            return analyzedLog.getException();

        }
        catch (MalformedURLException e ){
            e.printStackTrace();
        }

        return null;
    }

    public Map<String, List<HadoopException>> getExceptions(){
        return this.Exceptions;

    }
}