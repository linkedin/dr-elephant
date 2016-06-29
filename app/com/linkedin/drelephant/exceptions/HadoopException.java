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

package com.linkedin.drelephant.exceptions;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class HadoopException {
  private final Logger logger = Logger.getLogger(HadoopException.class);
  //    private String message;
//    private String stackTrace;
//    private String type;
//
//    public String getMessage(){
//        return this.message;
//    }
//
//    public String getStackTrace(){
//        return this.stackTrace;
//    }
//
//    public String getType(){
//        return this.type;
//    }
//
//    public void setMessage(String message){
//        this.message = message;
//    }
//
//    public void setStackTrace(String stackTrace){
//        this.stackTrace = stackTrace;
//    }
//
//    public void setType(String type){
//        this.type = type;
//    }
    /*private List<List<String>> exceptionChain;
    private String type; //azkaban, script, hadoopjobid, hadooptaskid, other(joboverview)
    private String id;
    public List<List<String>> getExceptionChain() {
        return this.exceptionChain;
    }

    public void setExceptionChain(List<List<String>> s) {
        this.exceptionChain = s;
    }

    public void addException(List<String> s) {         // add exception to exception chain
        this.exceptionChain.add(s);
    }

    public void setType(String s){
        this.type=s;
    }
    public String getType(){
        return this.type;
    }

    public void setId(String s){
        this.id=s;
    }
    public String getId(){
        return this.id;
    }

    public ExceptionLoggingEvent(){
        this.exceptionChain = new ArrayList<List<String>>();
        this.type = "";
        this.id="";
    }*/ List<ExceptionLoggingEvent> _exceptionLoggingEvents;

  public HadoopException() {
    this._exceptionLoggingEvents = new ArrayList<ExceptionLoggingEvent>();
  }

  public void addExceptionLoggingEvent(ExceptionLoggingEvent e) {
    _exceptionLoggingEvents.add(e);
  }

  public List<ExceptionLoggingEvent> getExceptionLoggingEvents() {
    return _exceptionLoggingEvents;
  }
}
