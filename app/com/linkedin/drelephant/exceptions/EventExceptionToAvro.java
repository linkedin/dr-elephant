package com.linkedin.drelephant.exceptions;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventExceptionToAvro {

    private static final Logger logger = Logger.getLogger(EventExceptionToAvro.class);
    private JSONObject eventException;
    private JSONObject stackTraceFrame;
    private String errorType;
    private String message;
    private int frameIndex;
    public EventExceptionToAvro(){

    }

    public JSONObject convert(List<String> exception, int exceptionIndex ){
        eventException = new JSONObject();        // this has been initialized here deliberately
        stackTraceFrame = new JSONObject();
        frameIndex = 0;
        Pattern stackTraceLine = Pattern.compile("^[\\\\t \\t]*at (.+)\\.(.+(?=\\())\\((.*)\\)"); //To do
        Pattern exceptionDetails = Pattern.compile("^([^() :]*): (.*)"); //To do

        for(String line : exception){
            Matcher match = stackTraceLine.matcher(line);
            if(match.find()){

                stackTraceFrame = StackTraceFrameToAvro(frameIndex,match.group(1),match.group(2),match.group(3));
                logger.info("stf " + stackTraceFrame + "\n");
                try{
                    eventException.accumulate("stackTrace",stackTraceFrame);
                }catch (JSONException e){
                    e.printStackTrace();
                }
                frameIndex +=1;
            } else{
                match = exceptionDetails.matcher(line);
                if(match.find()){
                    errorType = match.group(1);
                    message = match.group(2);
                }
            }
        }
        try{
            eventException.put("index", exceptionIndex);
            eventException.put("message",message);
            eventException.put("type",errorType);
        }
        catch (JSONException e){
            e.printStackTrace();
        }
        return eventException;
    }

    private JSONObject StackTraceFrameToAvro(int index, String source, String call, String fileDetails){
        JSONObject stackTraceFrame = new JSONObject();
        boolean nativeMethod = false;
        String fileName = "";
        String lineNumber = "0";
        Pattern file = Pattern.compile("(.*):(.*)");

        if(fileDetails == "Native Method"){
            nativeMethod = true;
            fileName = fileDetails;
        } else if(fileDetails == "Unknown Source"){
            fileName = fileDetails;
        }else{
            Matcher match = file.matcher(fileDetails);
            if(match.find()){
                fileName = match.group(1);
                lineNumber = match.group(2);
            }
        }
        try {
            stackTraceFrame.put("call", call);
            stackTraceFrame.put("columnNumber",0);
            stackTraceFrame.put("fileName", fileName);
            stackTraceFrame.put("index", index);
            stackTraceFrame.put("lineNumber", Integer.parseInt(lineNumber));
            stackTraceFrame.put("nativeMethod", nativeMethod);
            stackTraceFrame.put("source", source);
        }catch (JSONException e){
            e.printStackTrace();
        }
        return stackTraceFrame;
    }
}
