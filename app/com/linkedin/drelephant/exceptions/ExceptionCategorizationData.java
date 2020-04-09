package com.linkedin.drelephant.exceptions;

import java.util.List;


public class ExceptionCategorizationData implements Comparable<ExceptionCategorizationData> {
  private String ruleName = null;
  private String ruleTrigger = null;
  private String priority = null;
  private String category = null;

  public ExceptionCategorizationData(String ruleName, String ruleTrigger, String priority, String category) {
    this.ruleName = ruleName;
    this.ruleTrigger = ruleTrigger;
    this.priority = priority;
    this.category = category;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getRuleTrigger() {
    return ruleTrigger;
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
