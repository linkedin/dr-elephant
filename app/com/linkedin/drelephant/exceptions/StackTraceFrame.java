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

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class StackTraceFrame {

  private final Logger logger = Logger.getLogger(StackTraceFrame.class);
  private String _source;
  private String _fileName;
  private int _lineNumber;
  private String _call;
  private boolean _nativeMethod;
  private int _index;

  public StackTraceFrame(int index, String source, String call, String fileDetails) {
    this._source = source;
    this._call = call;
    this._index = index;
    getFileDetails(fileDetails);
  }

  private void getFileDetails(String fileDetails) {
    boolean nativeMethod = false;
    String fileName = fileDetails;
    String lineNumber = "0";
    Pattern file = Pattern.compile("(.*):(.*)");

    /**
     Example string: 'Assert.java:89'
     matches: ['Assert.java', '89']
     */

    if (fileDetails.equals("Native Method")) {
      nativeMethod = true;
    } else {
      Matcher match = file.matcher(fileDetails);
      if (match.find()) {
        fileName = match.group(1);
        lineNumber = match.group(2);
      }
    }
    this._fileName = fileName;
    this._lineNumber = Integer.parseInt(lineNumber); // To do: Can throw parseException
    this._nativeMethod = nativeMethod;
  }
}
