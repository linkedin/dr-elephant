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


import java.util.ArrayList;
import java.util.List;


/**
 * Code consist of seperate executable statements . For e.g in Hive Script
 * each query will be a statement.
 *
 * This will be simillar to DAO and creation of this will be dependent on the specific framework
 * optimization.
 */

public class Code {
  private List<Statement> statements = null;
  public Code(){
    statements = new ArrayList<>();
  }

  public List<Statement> getStatements() {
    return statements;
  }

  public void setStatements(List<Statement> statements) {
    this.statements = statements;
  }

  @Override
  public String toString() {
    return "Code{" + "statements=" + statements + '}';
  }
}
