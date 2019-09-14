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

package com.linkedin.drelephant.analysis.code.dataset;

/**
 * This is the representation of the actual script . Script consist of two things
 * 1) Code (which is executable part)
 * 2) comment  (non executable part , user level comment , optimization comment)
 */
public class Script {
  private StringBuilder optimizationComment = new StringBuilder();
  private Code code = null;
  private String inputFileName = null;
  private String inputData = null;

  public Script(String inputFileName) {
    this.inputFileName = inputFileName;
  }

  public StringBuilder getOptimizationComment() {
    return optimizationComment;
  }

  public Code getCode() {
    return code;
  }

  public void setCode(Code code) {
    this.code = code;
  }

  public String getInputFileName() {
    return inputFileName;
  }

  public void setInputFileName(String inputFileName) {
    this.inputFileName = inputFileName;
  }

  public String getInputData() {
    return inputData;
  }

  public void setInputData(String inputData) {
    this.inputData = inputData;
  }
}
