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


package com.linkedin.drelephant.configurations.scheduler;

import com.linkedin.drelephant.util.Utils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages the scheduler configurations
 */
public class SchedulerConfiguration {
  private List<SchedulerConfigurationData> _schedulerConfDataList;

  public SchedulerConfiguration(Element configuration) {
    parseSchedulerConfiguration(configuration);
  }

  public List<SchedulerConfigurationData> getSchedulerConfigurationData() {
    return _schedulerConfDataList;
  }

  private void parseSchedulerConfiguration(Element configuration) {
    _schedulerConfDataList = new ArrayList<SchedulerConfigurationData>();

    NodeList nodes = configuration.getChildNodes();
    int n = 0;
    for (int i = 0; i < nodes.getLength(); i++) {
      // Each scheduler node
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element schedulerElem = (Element) node;

        String className;
        Node classNameNode = schedulerElem.getElementsByTagName("classname").item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No tag 'classname' in scheduler " + n);
        }
        className = classNameNode.getTextContent();
        if (className.equals("")) {
          throw new RuntimeException("Empty tag 'classname' in scheduler " + n);
        }

        String schedulerName;
        Node schedulerNameNode = schedulerElem.getElementsByTagName("name").item(0);
        if (schedulerNameNode == null) {
          throw new RuntimeException("No tag 'name' in scheduler " + n + " classname " + className);
        }
        schedulerName = schedulerNameNode.getTextContent();
        if (schedulerName.equals("")) {
          throw new RuntimeException("Empty tag 'name' in scheduler " + n + " classname " + className);
        }

        // Check if parameters are defined for the scheduler
        Map<String, String> paramsMap = Utils.getConfigurationParameters(schedulerElem);

        SchedulerConfigurationData schedulerData = new SchedulerConfigurationData(schedulerName, className, paramsMap);
        _schedulerConfDataList.add(schedulerData);

      }
    }
  }

}
