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

package com.linkedin.drelephant.exceptions.util;

/**
 * This class defines all the configuration
 * required for exception fingerprinting.
 * @param <T> : Type of the data
 */
public class EFConfiguration<T> {

  private String configurationName;
  private T value;
  private String doc;

  public String getConfigurationName() {
    return configurationName;
  }

  public EFConfiguration<T> setConfigurationName(String configurationName) {
    this.configurationName = configurationName;
    return this;
  }

  public T getValue() {
    return value;
  }

  public EFConfiguration<T> setValue(T value) {
    this.value = value;
    return this;
  }

  public String getDoc() {
    return doc;
  }

  public EFConfiguration<T> setDoc(String doc) {
    this.doc = doc;
    return this;
  }

  @Override
  public String toString() {
    return "EFConfiguration{" + "configurationName='" + configurationName + '\'' + ", value=" + value + ", doc='" + doc
        + '\'' + '}';
  }
}
