package com.linkedin.drelephant.exceptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Exception categorization data
 */
public class ExceptionCategorizationData implements Comparable<ExceptionCategorizationData> {
  private String ruleName = null;
  private List<String> ruleTriggers = null;
  private String priority = null;
  private String category = null;
  private static final String RULE_TRIGGER_DELIMITER = ",";
  private static final String UNNECESSARY_DATA = "\n";

  ExceptionCategorizationData(String ruleName, String ruleTrigger, String priority, String category) {
    this.ruleName = ruleName;
    this.ruleTriggers =
        Arrays.asList(ruleTrigger.toLowerCase().replaceAll(UNNECESSARY_DATA, "").split(RULE_TRIGGER_DELIMITER))
            .stream()
            .map(String::trim)
            .collect(Collectors.toList());
    this.priority = priority;
    this.category = category;
  }

  public String getRuleName() {
    return ruleName;
  }

  public List<String> getRuleTrigger() {
    return ruleTriggers;
  }

  public String getPriority() {
    return priority;
  }

  public String getCategory() {
    return category;
  }

  @Override
  public int compareTo(ExceptionCategorizationData o) {
    return this.priority.compareTo(o.priority);
  }
}
