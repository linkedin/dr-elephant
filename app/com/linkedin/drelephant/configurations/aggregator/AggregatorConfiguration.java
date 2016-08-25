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

package com.linkedin.drelephant.configurations.aggregator;

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.List;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.ApplicationType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class AggregatorConfiguration {

  private static final Logger logger = Logger.getLogger(AggregatorConfiguration.class);
  private List<AggregatorConfigurationData> _aggregatorsConfDataList;

  public AggregatorConfiguration(Element configuration) {
    parseAggregatorConfiguration(configuration);
  }

  /**
   * Returns the list of Aggregators along with their Configuration Information
   *
   * @return A list of Configuration Data for the aggregators
   */
  public List<AggregatorConfigurationData> getAggregatorsConfigurationData() {
    return _aggregatorsConfDataList;
  }

  /**
   * Parses the Aggregator configuration file and loads the Aggregator Information to a list of AggregatorConfigurationData
   *
   * @param configuration The dom Element to be parsed
   */
  private void parseAggregatorConfiguration(Element configuration) {
    _aggregatorsConfDataList = new ArrayList<AggregatorConfigurationData>();

    NodeList nodes = configuration.getChildNodes();
    int n = 0;
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element aggregatorNode = (Element) node;

        String className;
        Node classNameNode = aggregatorNode.getElementsByTagName("classname").item(0);
        if (classNameNode == null) {
          throw new RuntimeException("No tag 'classname' in aggregator " + n);
        }
        className = classNameNode.getTextContent();
        if (className.equals("")) {
          throw new RuntimeException("Empty tag 'classname' in aggregator " + n);
        }

        Node appTypeNode = aggregatorNode.getElementsByTagName("applicationtype").item(0);
        if (appTypeNode == null) {
          throw new RuntimeException(
              "No tag or invalid tag 'applicationtype' in aggregator " + n + " classname " + className);
        }
        String appTypeStr = appTypeNode.getTextContent();
        if (appTypeStr == null) {
          logger.error("Application type is not specified in aggregator " + n + " classname " + className
              + ". Skipping this configuration.");
          continue;
        }
        ApplicationType appType = new ApplicationType(appTypeStr);
        // Check if parameters are defined for the heuristic
        Map<String, String> paramsMap = Utils.getConfigurationParameters(aggregatorNode);

        AggregatorConfigurationData aggregatorData = new AggregatorConfigurationData(className, appType, paramsMap);

        _aggregatorsConfDataList.add(aggregatorData);

      }
    }
  }

}
