package com.linkedin.drelephant.exceptions;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * This class parsed EFCategorization.xml and create List<ExceptionCategorizationData>
 */

public class ExceptionCategorization {
  private static final Logger logger = Logger.getLogger(ExceptionCategorization.class);
  private Map<String, List<ExceptionCategorizationData>> applicationTypeExceptionCategorizationData = null;
  boolean debugEnabled = logger.isDebugEnabled();

  private enum ClassificationTag {APPLICATIONTYPE, RULENAME, RULETRIGGER, RULEPRIORITY, CATEGORY}

  public ExceptionCategorization(Element element) {
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
          logger.debug("Application type " + applicationType);
          logger.debug("Rule Name " + ruleName);
          logger.debug("Rule Trigger " + ruleTrigger);
          logger.debug("Rule priority " + rulePriority);
          logger.debug("Rule category " + category);
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
