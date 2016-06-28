package com.linkedin.drelephant.exceptions;

//Given a log string, analyzes and returns the relevant information!
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzedLog {

        private static final Logger logger = Logger.getLogger(AnalyzedLog.class);
        private String state;
        private ExceptionLoggingEvent exception;
        private Set<String> list;
        private String log;


        public AnalyzedLog(String log){
                Matcher m;
                this.state = "";
                this.exception = new ExceptionLoggingEvent();
                this.list = new HashSet<String>();
                Set<String> allMatches = new HashSet<String>();
                this.log=log;

                if (match("Flow \\'\\' is set to SUCCEEDED in [0-9]+ seconds").find()) {                                //Successful Azkaban flow log
                        this.state = "SUCCEEDED";
                }

                else if (match("Setting flow \\'\\' status to FAILED in [0-9]+ seconds").find()) {                      //Failed Azkaban flow log
                                m = match("Job (.*) finished with status (?:FAILED|KILLED) in [0-9]+ seconds");
                                while(m.find()){
                                        allMatches.add(m.group(1));
                                }
                                this.state = "FAILED";
                                this.list = allMatches;

                }

                else if (match("Setting flow \'\' status to KILLED in [0-9]+ seconds").find()){                         //Killed Azkaban flow log
                                m = match("Job (.*) finished with status (?:FAILED|KILLED) in [0-9]+ seconds");
                                while(m.find()){
                                        allMatches.add(m.group(1));
                                }
                                this.state = "KILLED";
                                this.list = allMatches;
                }

                else if (match("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status SUCCEEDED").find()){        //Succeeded Azkaban Job log
                        this.state = "SUCCEEDED";
                }

                else if(match("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status FAILED").find()){            //Failed Azkaban Job log
                        m = match("job_[0-9]+_[0-9]+");
                        while(m.find()){
                                allMatches.add(m.group());
                        }
                        if(match("ERROR - Job run failed!").find()){
                                this.state = "SCRIPT FAILED";
                                this.exception.setType("script");
                                this.list = allMatches;
                                this.exception.setExceptionChain(matchExceptionChain(".+\\n(?:.+\\tat.+\\n)+(?:.+Caused by.+\\n(?:.*\\n)?(?:.+\\s+at.+\\n)*)*"));
                        }

                        else {
                                this.state ="Azkaban Fail";
                                this.exception.setType("azkaban");
                                m = match("\\d{2}[-/]\\d{2}[-/]\\d{4} \\d{2}:\\d{2}:\\d{2} PDT [^\\s]+ (?:ERROR|WARN|FATAL|Exception) .*\\n");
                                if (m.find()) {
                                        this.exception.addEventException(stringToExceptionEvent(m.group()));
                                }
                        }
                }

                else if(match("Finishing job [^\\s]+ attempt: [0-9]+ at [0-9]+ with status KILLED").find()){            // Killed Azkaban Job log
                        m = match("job_[0-9]+_[0-9]+");
                        while(m.find()){
                                allMatches.add(m.group());
                        }
                        this.state = "KILLED";
                        this.list= allMatches;
                        //**Incomplete**

                }


                else if (match("Job failed as tasks failed").find()){                                                   // Failed MR Job log
                        this.state = "FAILED";
                        m = match("Task failed (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
                        while(m.find()){
                                allMatches.add(m.group(1));
                        }
                        this.list = allMatches;
                        this.exception.setExceptionChain(matchExceptionChain(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*"));
                }

                else if (match("Job failed as tasks killed").find()){                                                   // Killed MR Job log
                        this.state = "KILLED";
                        m = match("Task killed (task_[0-9]+_[0-9]+_[mr]_[0-9]+)");
                        while(m.find()){
                                allMatches.add(m.group(1));
                        }
                        this.list = allMatches;
                        this.exception.setExceptionChain(matchExceptionChain(".*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*"));

                }

                else {                                                                                                  // Failed MR Task Task log
                        this.state="FAILED";
                        this.exception.setExceptionChain(matchExceptionChain("Error: (.*\\n(?:.*\\tat.+\\n)+(?:.*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*)*)"));
                }

        }


        private List<List<String>> matchExceptionChain (String pattern){
                Matcher m = Pattern.compile(pattern).matcher(log);
                List<List<String>> exceptionChain = new ArrayList<List<String>>();
                if(m.find()){

                        for (String exceptionString : stringToExceptionChain(m.group())) {
                                exceptionChain.add(stringToExceptionEvent(exceptionString));
                        }
                        return exceptionChain;
                }
                logger.info(exceptionChain);
                return null;
        }


        public Matcher match(String pattern){
                Matcher m;
                return m = Pattern.compile(pattern).matcher(log);
        }

        public List<String> stringToExceptionChain (String s){
               List<String> chain = new ArrayList<String>();
               Matcher matcher = Pattern.compile(".*^(?!Caused by).+\\n(?:.*\\tat.+\\n)+").matcher(s);
                while(matcher.find()){
                        chain.add(matcher.group());
                }
                matcher = Pattern.compile(".*Caused by.+\\n(?:.*\\n)?(?:.*\\s+at.+\\n)*").matcher(s);
                while(matcher.find()){
                        chain.add(matcher.group());
                }
                return chain;
        }

        public List<String> stringToExceptionEvent(String s){
                List<String> exceptionEvent = new ArrayList<String>();
                Matcher matcher = Pattern.compile(".*\\n").matcher(s);
                while(matcher.find()){
                        exceptionEvent.add(matcher.group());
                }
                return exceptionEvent;
        }

        public String getState(){
                return this.state;
        }

        public Set<String> getList(){
                return this.list;
        }

        public ExceptionLoggingEvent getException(){
                return this.exception;
        }
}