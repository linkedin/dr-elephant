package com.linkedin.drelephant.exceptions;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.util.List;

public class ExceptionLoggingEventToAvro {
    private final Logger logger = Logger.getLogger(ExceptionLoggingEventToAvro.class);
    private JSONObject loggingEvent;
    private JSONObject header;

    public ExceptionLoggingEventToAvro() {
        loggingEvent = new JSONObject();
        header = new JSONObject();
    }
    public JSONObject convert(ExceptionLoggingEvent exceptionLoggingEvent){
        int index = 0;
        EventExceptionToAvro eventExceptionToAvro = new EventExceptionToAvro();
        for(List<String> eventException : exceptionLoggingEvent.getExceptionChain()){
            try{
                loggingEvent.append("exceptionChain",eventExceptionToAvro.convert(eventException,index));
            }catch(JSONException e){
                e.printStackTrace();
            }
            index +=1;
        }
        try{
            header.put("service","");
            header.put("container","");
            header.put("environment","");
            header.put("eventType","");
            header.put("eventVersion","");
            header.put("guid","");
            header.put("instance","");
            header.put("nano","");
            header.put("server","");
            header.put("time","");
            header.put("version","");
            loggingEvent.put("header", header);
            loggingEvent.put("level","ERROR");
            loggingEvent.put("log","");
            loggingEvent.put("logger","");
            loggingEvent.put("loggingContext","");
            loggingEvent.put("message","");
            loggingEvent.put("requestId","");
            loggingEvent.put("serviceCallContext","");
            loggingEvent.put("thread","");
            loggingEvent.put("timestamp","");
            loggingEvent.put("treeId","");
        }
        catch (JSONException e){
            e.printStackTrace();
        }
        return loggingEvent;
    }

}

