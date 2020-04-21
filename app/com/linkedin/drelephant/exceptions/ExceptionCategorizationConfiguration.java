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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * This class parsed EFCategorizationConf.xml and create List<ExceptionCategorizationData>
 */

public class ExceptionCategorizationConfiguration {
  private static final Logger logger = Logger.getLogger(ExceptionCategorizationConfiguration.class);
  private Map<String, List<ExceptionCategorizationData>> applicationTypeExceptionCategorizationData;
  boolean debugEnabled = logger.isDebugEnabled();

  private enum ClassificationTag {APPLICATIONTYPE, RULENAME, RULETRIGGER, RULEPRIORITY, CATEGORY}

  public ExceptionCategorizationConfiguration(Element element) {
    applicationTypeExceptionCategorizationData = new HashMap<>();
    parseExceptionCategorization(element);
  }

  public Map<String, List<ExceptionCategorizationData>> getExceptionCategorizationData() {
    return this.applicationTypeExceptionCategorizationData;
  }

  private void parseExceptionCategorization(Element configuration) {

    NodeList nodes = configuration.getChildNodes();
    for (int index = 0; index < nodes.getLength(); index++) {
      Node node = nodes.item(index);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element classificationRule = (Element) node;
        Node applicationTypeNode =
            classificationRule.getElementsByTagName(ClassificationTag.APPLICATIONTYPE.name().toLowerCase()).item(0);
        Node ruleNameNode =
            classificationRule.getElementsByTagName(ClassificationTag.RULENAME.name().toLowerCase()).item(0);
        Node ruleTriggerNode =
            classificationRule.getElementsByTagName(ClassificationTag.RULETRIGGER.name().toLowerCase()).item(0);
        Node rulePriorityNode =
            classificationRule.getElementsByTagName(ClassificationTag.RULEPRIORITY.name().toLowerCase()).item(0);
        Node categoryNode =
            classificationRule.getElementsByTagName(ClassificationTag.CATEGORY.name().toLowerCase()).item(0);
        if (applicationTypeNode == null || ruleNameNode == null || ruleTriggerNode == null || rulePriorityNode == null
            || categoryNode == null) {
          logger.error(" Insufficient information " + classificationRule);
          break;
        }
        String applicationType = applicationTypeNode.getTextContent();
        String ruleName = ruleNameNode.getTextContent();
        String ruleTrigger = ruleTriggerNode.getTextContent();
        String rulePriority = rulePriorityNode.getTextContent();
        String category = categoryNode.getTextContent();
        if (debugEnabled) {
          logger.debug("Application type=" + applicationType + " Rule Name=" + ruleName + " Rule Trigger" + ruleTrigger
              + " Rule priority=" + rulePriority + " Rule category=" + category);
        }
        List<ExceptionCategorizationData> exceptionCategorizationData =
            applicationTypeExceptionCategorizationData.get(applicationType);
        if (exceptionCategorizationData == null) {
          exceptionCategorizationData = new ArrayList<>();
        }
        exceptionCategorizationData.add(new ExceptionCategorizationData(ruleName, ruleTrigger, rulePriority, category));
        applicationTypeExceptionCategorizationData.put(applicationType, exceptionCategorizationData);
      }
    }
    applicationTypeExceptionCategorizationData.forEach((key, value) -> Collections.sort(value));
  }
}
