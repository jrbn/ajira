package nl.vu.cs.ajira.examples.aurora.api.support;

import nl.vu.cs.ajira.examples.aurora.data.Ordering;

public class SortOperatorInfo implements OperatorInfo {
  private final String attributeName;
  private final int slack;
  private final Ordering ordering;

  public SortOperatorInfo(String attributeName, int slack, Ordering ordering) {
    super();
    this.attributeName = attributeName;
    this.slack = slack;
    this.ordering = ordering;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public int getSlack() {
    return slack;
  }

  public Ordering getOrdering() {
    return ordering;
  }

}
